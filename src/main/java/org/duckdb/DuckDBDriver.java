package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static org.duckdb.JdbcUtils.*;
import static org.duckdb.io.IOUtils.readToString;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.duckdb.io.LimitedInputStream;

public class DuckDBDriver implements java.sql.Driver {

    public static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
    public static final String DUCKDB_USER_AGENT_PROPERTY = "custom_user_agent";
    public static final String JDBC_STREAM_RESULTS = "jdbc_stream_results";
    public static final String JDBC_PIN_DB = "jdbc_pin_db";
    public static final String JDBC_IGNORE_UNSUPPORTED_OPTIONS = "jdbc_ignore_unsupported_options";

    static final String DUCKDB_URL_PREFIX = "jdbc:duckdb:";
    static final String MEMORY_DB = ":memory:";
    private static final String DUCKLAKE_URL_PREFIX = DUCKDB_URL_PREFIX + "ducklake:";

    static final ScheduledThreadPoolExecutor scheduler;

    private static final LinkedHashMap<String, ByteBuffer> pinnedDbRefs = new LinkedHashMap<>();
    private static final ReentrantLock pinnedDbRefsLock = new ReentrantLock();
    private static boolean pinnedDbRefsShutdownHookRegistered = false;
    private static boolean pinnedDbRefsShutdownHookRun = false;

    private static final Set<String> supportedOptions = new LinkedHashSet<>();
    private static final ReentrantLock supportedOptionsLock = new ReentrantLock();

    private static final String SESSION_INIT_SQL_FILE_OPTION = "session_init_sql_file";
    private static final String SESSION_INIT_SQL_FILE_SHA256_OPTION = "session_init_sql_file_sha256";
    private static final long SESSION_INIT_SQL_FILE_MAX_SIZE_BYTES = 1 << 20; // 1MB
    private static final String SESSION_INIT_SQL_FILE_URL_EXAMPLE =
        "jdbc:duckdb:/path/to/db1.db;session_init_sql_file=/path/to/init.sql;session_init_sql_file_sha256=...";
    private static final String SESSION_INIT_SQL_CONN_INIT_MARKER =
        "/\\*\\s*DUCKDB_CONNECTION_INIT_BELOW_MARKER\\s*\\*/";
    private static final LinkedHashSet<String> sessionInitSQLFileDbNames = new LinkedHashSet<>();
    private static final ReentrantLock sessionInitSQLFileLock = new ReentrantLock();

    static {
        try {
            DriverManager.registerDriver(new DuckDBDriver());
            ThreadFactory tf = r -> new Thread(r, "duckdb-query-cancel-scheduler-thread");
            scheduler = new ScheduledThreadPoolExecutor(1, tf);
            scheduler.setRemoveOnCancelPolicy(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        final Properties props;
        if (info == null) {
            props = new Properties();
        } else { // make a copy because we're removing the read only property below
            props = (Properties) info.clone();
        }

        // URL options
        ParsedProps pp = parsePropsFromUrl(url);

        // Read session init file
        SessionInitSQLFile sf = readSessionInitSQLFile(pp);

        // Options in URL take preference
        for (Map.Entry<String, String> en : pp.props.entrySet()) {
            props.put(en.getKey(), en.getValue());
        }

        // Ignore unsupported
        removeUnsupportedOptions(props);

        // Read-only option
        String readOnlyStr = removeOption(props, DUCKDB_READONLY_PROPERTY);
        boolean readOnly = isStringTruish(readOnlyStr, false);

        // Client name option
        props.put("duckdb_api", "jdbc");

        // Apache Spark passes this option when SELECT on a JDBC DataSource
        // table is performed. It is the internal Spark option and is likely
        // passed by mistake, so we need to ignore it to allow the connection
        // to be established.
        props.remove("path");

        // DuckLake connection
        if (pp.shortUrl.startsWith(DUCKLAKE_URL_PREFIX)) {
            setDefaultOptionValue(props, JDBC_PIN_DB, true);
            setDefaultOptionValue(props, JDBC_STREAM_RESULTS, true);
        }

        // Pin DB option
        String pinDbOptStr = removeOption(props, JDBC_PIN_DB);
        boolean pinDBOpt = isStringTruish(pinDbOptStr, false);

        // Create connection
        DuckDBConnection conn = DuckDBConnection.newConnection(pp.shortUrl, readOnly, sf.origFileText, props);

        // Run post-init
        try {
            pinDB(pinDBOpt, pp.shortUrl, conn);
            runSessionInitSQLFile(conn, pp.shortUrl, sf);
        } catch (SQLException e) {
            closeQuietly(conn);
            throw e;
        }

        return conn;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith("jdbc:duckdb:");
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        List<DriverPropertyInfo> list = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, info); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name, value, description FROM duckdb_settings()")) {
            while (rs.next()) {
                String name = rs.getString(1);
                String value = rs.getString(2);
                String description = rs.getString(3);
                list.add(createDriverPropInfo(name, value, description));
            }
        }
        list.add(createDriverPropInfo(DUCKDB_READONLY_PROPERTY, "", "Set connection to read-only mode"));
        list.add(createDriverPropInfo(DUCKDB_USER_AGENT_PROPERTY, "", "Custom user agent string"));
        list.add(createDriverPropInfo(JDBC_STREAM_RESULTS, "", "Enable result set streaming"));
        list.add(createDriverPropInfo(JDBC_PIN_DB, "",
                                      "Do not close the DB instance after all connections to it are closed"));
        list.add(createDriverPropInfo(JDBC_IGNORE_UNSUPPORTED_OPTIONS, "",
                                      "Silently discard unsupported connection options"));
        list.sort((o1, o2) -> o1.name.compareToIgnoreCase(o2.name));
        return list.toArray(new DriverPropertyInfo[0]);
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return true; // of course!
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("no logger");
    }

    private static ParsedProps parsePropsFromUrl(String url) throws SQLException {
        if (!url.contains(";")) {
            return new ParsedProps(url);
        }
        String[] parts = url.split(";");
        LinkedHashMap<String, String> props = new LinkedHashMap<>();
        List<String> origPropNames = new ArrayList<>();
        for (int i = 1; i < parts.length; i++) {
            String entry = parts[i].trim();
            if (entry.isEmpty()) {
                continue;
            }
            String[] kv = entry.split("=");
            if (2 != kv.length) {
                throw new SQLException("Invalid URL entry: " + entry);
            }
            String key = kv[0].trim();
            String value = kv[1].trim();
            origPropNames.add(key);
            props.put(key, value);
        }
        String shortUrl = parts[0].trim();
        return new ParsedProps(shortUrl, props, origPropNames);
    }

    private static void pinDB(boolean pinnedDbOpt, String url, DuckDBConnection conn) throws SQLException {
        if (!pinnedDbOpt) {
            return;
        }
        String dbName = dbNameFromUrl(url);
        if (":memory:".equals(dbName)) {
            return;
        }

        pinnedDbRefsLock.lock();
        try {
            // Actual native DB cache uses absolute paths to file DBs,
            // but that should not make the difference unless CWD is changed,
            // that is not expected for a JVM process, see JDK-4045688.
            if (pinnedDbRefsShutdownHookRun || pinnedDbRefs.containsKey(dbName)) {
                return;
            }
            // No need to hold connRef lock here, this connection is not
            // yet available to client at this point, so it cannot be closed.
            ByteBuffer dbRef = DuckDBNative.duckdb_jdbc_create_db_ref(conn.connRef);
            pinnedDbRefs.put(dbName, dbRef);

            if (!pinnedDbRefsShutdownHookRegistered) {
                Runtime.getRuntime().addShutdownHook(new Thread(new PinnedDbRefsShutdownHook()));
                pinnedDbRefsShutdownHookRegistered = true;
            }
        } finally {
            pinnedDbRefsLock.unlock();
        }
    }

    public static boolean releaseDB(String url) throws SQLException {
        pinnedDbRefsLock.lock();
        try {
            if (pinnedDbRefsShutdownHookRun) {
                return false;
            }
            String dbName = dbNameFromUrl(url);
            ByteBuffer dbRef = pinnedDbRefs.remove(dbName);
            if (null == dbRef) {
                return false;
            }
            DuckDBNative.duckdb_jdbc_destroy_db_ref(dbRef);
            return true;
        } finally {
            pinnedDbRefsLock.unlock();
        }
    }

    private static DriverPropertyInfo createDriverPropInfo(String name, String value, String description) {
        DriverPropertyInfo dpi = new DriverPropertyInfo(name, value);
        dpi.description = description;
        return dpi;
    }

    private static void removeUnsupportedOptions(Properties props) throws SQLException {
        String ignoreStr = removeOption(props, JDBC_IGNORE_UNSUPPORTED_OPTIONS);
        boolean ignore = isStringTruish(ignoreStr, false);
        if (!ignore) {
            return;
        }
        supportedOptionsLock.lock();
        try {
            if (supportedOptions.isEmpty()) {
                Driver driver = DriverManager.getDriver(DUCKDB_URL_PREFIX);
                Properties dpiProps = new Properties();
                dpiProps.put("threads", 1);
                DriverPropertyInfo[] dpis = driver.getPropertyInfo(DUCKDB_URL_PREFIX, dpiProps);
                for (DriverPropertyInfo dpi : dpis) {
                    supportedOptions.add(dpi.name);
                }
            }
            List<String> unsupportedNames = new ArrayList<>();
            for (Object nameObj : props.keySet()) {
                String name = String.valueOf(nameObj);
                if (!supportedOptions.contains(name)) {
                    unsupportedNames.add(name);
                }
            }
            for (String name : unsupportedNames) {
                props.remove(name);
            }
        } finally {
            supportedOptionsLock.unlock();
        }
    }

    private static SessionInitSQLFile readSessionInitSQLFile(ParsedProps pp) throws SQLException {
        if (!pp.props.containsKey(SESSION_INIT_SQL_FILE_OPTION)) {
            return new SessionInitSQLFile();
        }

        List<String> urlOptsList = new ArrayList<>(pp.props.keySet());

        if (!SESSION_INIT_SQL_FILE_OPTION.equals(urlOptsList.get(0))) {
            throw new SQLException(
                "'session_init_sql_file' can only be specified as the first parameter in connection string,"
                + " example: '" + SESSION_INIT_SQL_FILE_URL_EXAMPLE + "'");
        }
        for (int i = 1; i < pp.origPropNames.size(); i++) {
            if (SESSION_INIT_SQL_FILE_OPTION.equalsIgnoreCase(pp.origPropNames.get(i))) {
                throw new SQLException("'session_init_sql_file' option cannot be specified more than once");
            }
        }
        String filePathStr = pp.props.remove(SESSION_INIT_SQL_FILE_OPTION);

        final String expectedSha256;
        if (pp.props.containsKey(SESSION_INIT_SQL_FILE_SHA256_OPTION)) {
            if (!SESSION_INIT_SQL_FILE_SHA256_OPTION.equals(urlOptsList.get(1))) {
                throw new SQLException(
                    "'session_init_sql_file_sha256' can only be specified as the second parameter in connection string,"
                    + " example: '" + SESSION_INIT_SQL_FILE_URL_EXAMPLE + "'");
            }
            for (int i = 2; i < pp.origPropNames.size(); i++) {
                if (SESSION_INIT_SQL_FILE_SHA256_OPTION.equalsIgnoreCase(pp.origPropNames.get(i))) {
                    throw new SQLException("'session_init_sql_file_sha256' option cannot be specified more than once");
                }
            }
            expectedSha256 = pp.props.remove(SESSION_INIT_SQL_FILE_SHA256_OPTION);
        } else {
            expectedSha256 = "";
        }

        Path filePath = Paths.get(filePathStr);
        if (!Files.exists(filePath)) {
            throw new SQLException("Specified session init SQL file not found, path: " + filePath);
        }

        final String origFileText;
        final String actualSha256;
        try {
            long fileSize = Files.size(filePath);
            if (fileSize > SESSION_INIT_SQL_FILE_MAX_SIZE_BYTES) {
                throw new SQLException("Specified session init SQL file size: " + fileSize +
                                       " exceeds max allowed size: " + SESSION_INIT_SQL_FILE_MAX_SIZE_BYTES);
            }
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = new DigestInputStream(
                     new LimitedInputStream(Files.newInputStream(filePath, READ), fileSize), md)) {
                Reader reader = new InputStreamReader(is, UTF_8);
                origFileText = readToString(reader);
                actualSha256 = bytesToHex(md.digest());
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }

        if (!expectedSha256.isEmpty() && !expectedSha256.toLowerCase().equals(actualSha256)) {
            throw new SQLException("Session init SQL file SHA-256 mismatch, expected: " + expectedSha256 +
                                   ", actual: " + actualSha256);
        }

        String[] parts = origFileText.split(SESSION_INIT_SQL_CONN_INIT_MARKER);
        if (parts.length > 2) {
            throw new SQLException("Connection init marker: '" + SESSION_INIT_SQL_CONN_INIT_MARKER +
                                   "' can only be specified once");
        }
        if (1 == parts.length) {
            return new SessionInitSQLFile(origFileText, parts[0].trim());
        } else {
            return new SessionInitSQLFile(origFileText, parts[0].trim(), parts[1].trim());
        }
    }

    private static void runSessionInitSQLFile(Connection conn, String url, SessionInitSQLFile sf) throws SQLException {
        if (sf.isEmpty()) {
            return;
        }
        sessionInitSQLFileLock.lock();
        try {

            if (!sf.dbInitSQL.isEmpty()) {
                String dbName = dbNameFromUrl(url);
                if (MEMORY_DB.equals(dbName) || !sessionInitSQLFileDbNames.contains(dbName)) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(sf.dbInitSQL);
                    }
                }
                sessionInitSQLFileDbNames.add(dbName);
            }

            if (!sf.connInitSQL.isEmpty()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sf.connInitSQL);
                }
            }

        } finally {
            sessionInitSQLFileLock.unlock();
        }
    }

    private static class ParsedProps {
        final String shortUrl;
        final LinkedHashMap<String, String> props;
        final List<String> origPropNames;

        private ParsedProps(String url) {
            this(url, new LinkedHashMap<>(), new ArrayList<>());
        }

        private ParsedProps(String shortUrl, LinkedHashMap<String, String> props, List<String> origPropNames) {
            this.shortUrl = shortUrl;
            this.props = props;
            this.origPropNames = origPropNames;
        }
    }

    private static class PinnedDbRefsShutdownHook implements Runnable {
        @Override
        public void run() {
            pinnedDbRefsLock.lock();
            try {
                List<ByteBuffer> dbRefsList = new ArrayList<>(pinnedDbRefs.values());
                Collections.reverse(dbRefsList);
                for (ByteBuffer dbRef : dbRefsList) {
                    DuckDBNative.duckdb_jdbc_destroy_db_ref(dbRef);
                }
                pinnedDbRefsShutdownHookRun = true;
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                pinnedDbRefsLock.unlock();
            }
        }
    }

    private static class SessionInitSQLFile {
        final String dbInitSQL;
        final String connInitSQL;
        final String origFileText;

        private SessionInitSQLFile() {
            this(null, null, null);
        }

        private SessionInitSQLFile(String origFileText, String dbInitSQL) {
            this(origFileText, dbInitSQL, "");
        }

        private SessionInitSQLFile(String origFileText, String dbInitSQL, String connInitSQL) {
            this.origFileText = origFileText;
            this.dbInitSQL = dbInitSQL;
            this.connInitSQL = connInitSQL;
        }

        boolean isEmpty() {
            return null == dbInitSQL && null == connInitSQL && null == origFileText;
        }
    }
}

package org.duckdb;

import static org.duckdb.JdbcUtils.*;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class DuckDBDriver implements java.sql.Driver {

    public static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
    public static final String DUCKDB_USER_AGENT_PROPERTY = "custom_user_agent";
    public static final String JDBC_STREAM_RESULTS = "jdbc_stream_results";
    public static final String JDBC_AUTO_COMMIT = "jdbc_auto_commit";
    public static final String JDBC_PIN_DB = "jdbc_pin_db";

    static final String DUCKDB_URL_PREFIX = "jdbc:duckdb:";
    static final String MEMORY_DB = ":memory:";

    static final ScheduledThreadPoolExecutor scheduler;

    private static final String DUCKLAKE_OPTION = "ducklake";
    private static final String DUCKLAKE_ALIAS_OPTION = "ducklake_alias";
    private static final Pattern DUCKLAKE_ALIAS_OPTION_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    private static final String DUCKLAKE_URL_PREFIX = "ducklake:";
    private static final String DUCKLAKE_DEFAULT_DBNAME = MEMORY_DB + "ducklakemem";
    private static final LinkedHashSet<String> ducklakeInstances = new LinkedHashSet<>();
    private static final ReentrantLock ducklakeInitLock = new ReentrantLock();

    private static final LinkedHashMap<String, ByteBuffer> pinnedDbRefs = new LinkedHashMap<>();
    private static final ReentrantLock pinnedDbRefsLock = new ReentrantLock();
    private static boolean pinnedDbRefsShutdownHookRegistered = false;
    private static boolean pinnedDbRefsShutdownHookRun = false;

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

        ParsedProps pp = parsePropsFromUrl(url);
        for (Map.Entry<String, String> en : pp.props.entrySet()) {
            props.put(en.getKey(), en.getValue());
        }

        String readOnlyStr = removeOption(props, DUCKDB_READONLY_PROPERTY);
        boolean readOnly = isStringTruish(readOnlyStr, false);
        props.put("duckdb_api", "jdbc");

        // Apache Spark passes this option when SELECT on a JDBC DataSource
        // table is performed. It is the internal Spark option and is likely
        // passed by mistake, so we need to ignore it to allow the connection
        // to be established.
        props.remove("path");

        String ducklake = removeOption(props, DUCKLAKE_OPTION);
        String ducklakeAlias = removeOption(props, DUCKLAKE_ALIAS_OPTION, DUCKLAKE_OPTION);
        final String shortUrl;
        if (null != ducklake) {
            setDefaultOptionValue(props, JDBC_PIN_DB, true);
            setDefaultOptionValue(props, JDBC_STREAM_RESULTS, true);
            String dbName = dbNameFromUrl(pp.shortUrl);
            if (MEMORY_DB.equals(dbName)) {
                shortUrl = DUCKDB_URL_PREFIX + DUCKLAKE_DEFAULT_DBNAME;
            } else {
                shortUrl = pp.shortUrl;
            }
        } else {
            shortUrl = pp.shortUrl;
        }

        String pinDbOptStr = removeOption(props, JDBC_PIN_DB);
        boolean pinDBOpt = isStringTruish(pinDbOptStr, false);

        DuckDBConnection conn = DuckDBConnection.newConnection(shortUrl, readOnly, props);

        pinDB(pinDBOpt, shortUrl, conn);

        initDucklake(conn, shortUrl, ducklake, ducklakeAlias);

        return conn;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith(DUCKDB_URL_PREFIX);
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
        list.add(createDriverPropInfo(JDBC_AUTO_COMMIT, "", "Set default auto-commit mode"));
        list.add(createDriverPropInfo(JDBC_PIN_DB, "",
                                      "Do not close the DB instance after all connections to it are closed"));
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

    private static void initDucklake(Connection conn, String url, String ducklake, String ducklakeAlias)
        throws SQLException {
        if (null == ducklake) {
            return;
        }
        ducklakeInitLock.lock();
        try {
            String dbName = dbNameFromUrl(url);
            String key = dbName + "#" + ducklake;
            if (!ducklakeInstances.contains(key)) {
                String attachQuery = createAttachQuery(ducklake, ducklakeAlias);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSTALL ducklake");
                    stmt.execute("LOAD ducklake");
                    stmt.execute(attachQuery);
                }
                ducklakeInstances.add(key);
            }
        } finally {
            ducklakeInitLock.unlock();
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("USE " + ducklakeAlias);
        }
    }

    private static String createAttachQuery(String ducklake, String ducklakeAlias) throws SQLException {
        ducklake = ducklake.replaceAll("'", "''");
        if (!ducklake.startsWith(DUCKLAKE_URL_PREFIX)) {
            ducklake = DUCKLAKE_URL_PREFIX + ducklake;
        }
        if (!DUCKLAKE_ALIAS_OPTION_PATTERN.matcher(ducklakeAlias).matches()) {
            throw new SQLException("Invalid DuckLake alias specified: " + ducklakeAlias);
        }
        return "ATTACH '" + ducklake + "' AS " + ducklakeAlias;
    }

    private static ParsedProps parsePropsFromUrl(String url) throws SQLException {
        if (!url.contains(";")) {
            return new ParsedProps(url);
        }
        String[] parts = url.split(";");
        LinkedHashMap<String, String> props = new LinkedHashMap<>();
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
            props.put(key, value);
        }
        String shortUrl = parts[0].trim();
        return new ParsedProps(shortUrl, props);
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

    private static class ParsedProps {
        final String shortUrl;
        final LinkedHashMap<String, String> props;

        private ParsedProps(String url) {
            this(url, new LinkedHashMap<>());
        }

        private ParsedProps(String shortUrl, LinkedHashMap<String, String> props) {
            this.shortUrl = shortUrl;
            this.props = props;
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
}

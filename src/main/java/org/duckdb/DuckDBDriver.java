package org.duckdb;

import static org.duckdb.JdbcUtils.*;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
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

    private static final String DUCKLAKE_OPTION = "ducklake";
    private static final String DUCKLAKE_ALIAS_OPTION = "ducklake_alias";
    private static final Pattern DUCKLAKE_ALIAS_OPTION_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    private static final String DUCKLAKE_URL_PREFIX = "ducklake:";
    private static final ReentrantLock DUCKLAKE_INIT_LOCK = new ReentrantLock();

    private static final LinkedHashMap<String, ByteBuffer> pinnedDbRefs = new LinkedHashMap<>();
    private static final ReentrantLock pinnedDbRefsLock = new ReentrantLock();
    private static boolean pinnedDbRefsShutdownHookRegistered = false;
    private static boolean pinnedDbRefsShutdownHookRun = false;

    static {
        try {
            DriverManager.registerDriver(new DuckDBDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        if (info == null) {
            info = new Properties();
        } else { // make a copy because we're removing the read only property below
            info = (Properties) info.clone();
        }

        ParsedProps pp = parsePropsFromUrl(url);
        for (Map.Entry<String, String> en : pp.props.entrySet()) {
            info.put(en.getKey(), en.getValue());
        }
        url = pp.shortUrl;

        String readOnlyStr = removeOption(info, DUCKDB_READONLY_PROPERTY);
        boolean readOnly = isStringTruish(readOnlyStr, false);
        info.put("duckdb_api", "jdbc");

        // Apache Spark passes this option when SELECT on a JDBC DataSource
        // table is performed. It is the internal Spark option and is likely
        // passed by mistake, so we need to ignore it to allow the connection
        // to be established.
        info.remove("path");

        String pinDbOptStr = removeOption(info, JDBC_PIN_DB);
        boolean pinDBOpt = isStringTruish(pinDbOptStr, false);

        String ducklake = removeOption(info, DUCKLAKE_OPTION);
        String ducklakeAlias = removeOption(info, DUCKLAKE_ALIAS_OPTION);

        DuckDBConnection conn = DuckDBConnection.newConnection(url, readOnly, info);

        pinDB(pinDBOpt, url, conn);

        initDucklake(conn, ducklake, ducklakeAlias);

        return conn;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith(DUCKDB_URL_PREFIX);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo[] ret = {};
        return ret; // no properties
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

    private static void initDucklake(Connection conn, String ducklake, String ducklakeAlias) throws SQLException {
        if (null == ducklake) {
            return;
        }
        DUCKLAKE_INIT_LOCK.lock();
        try {
            String attachQuery = createAttachQuery(ducklake, ducklakeAlias);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSTALL ducklake");
                stmt.execute("LOAD ducklake");
                stmt.execute(attachQuery);
                if (null != ducklakeAlias) {
                    stmt.execute("USE " + ducklakeAlias);
                }
            }
        } finally {
            DUCKLAKE_INIT_LOCK.unlock();
        }
    }

    private static String createAttachQuery(String ducklake, String ducklakeAlias) throws SQLException {
        ducklake = ducklake.replaceAll("'", "''");
        if (!ducklake.startsWith(DUCKLAKE_URL_PREFIX)) {
            ducklake = DUCKLAKE_URL_PREFIX + ducklake;
        }
        String query = "ATTACH IF NOT EXISTS '" + ducklake + "'";
        if (null != ducklakeAlias) {
            if (!DUCKLAKE_ALIAS_OPTION_PATTERN.matcher(ducklakeAlias).matches()) {
                throw new SQLException("Invalid DuckLake alias specified: " + ducklakeAlias);
            }
            query += " AS " + ducklakeAlias;
        }
        return query;
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

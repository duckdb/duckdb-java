package org.duckdb;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class DuckDBDriver implements java.sql.Driver {

    public static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
    public static final String DUCKDB_USER_AGENT_PROPERTY = "custom_user_agent";
    public static final String JDBC_STREAM_RESULTS = "jdbc_stream_results";

    private static final String DUCKDB_URL_PREFIX = "jdbc:duckdb:";

    private static final String DUCKLAKE_OPTION = "ducklake";
    private static final String DUCKLAKE_ALIAS_OPTION = "ducklake_alias";
    private static final Pattern DUCKLAKE_ALIAS_OPTION_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    private static final String DUCKLAKE_URL_PREFIX = "ducklake:";
    private static final ReentrantLock DUCKLAKE_INIT_LOCK = new ReentrantLock();

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
        boolean read_only = false;
        if (info == null) {
            info = new Properties();
        } else { // make a copy because we're removing the read only property below
            info = (Properties) info.clone();
        }
        String prop_val = (String) info.remove(DUCKDB_READONLY_PROPERTY);
        if (prop_val != null) {
            String prop_clean = prop_val.trim().toLowerCase();
            read_only = prop_clean.equals("1") || prop_clean.equals("true") || prop_clean.equals("yes");
        }
        info.put("duckdb_api", "jdbc");

        // Apache Spark passes this option when SELECT on a JDBC DataSource
        // table is performed. It is the internal Spark option and is likely
        // passed by mistake, so we need to ignore it to allow the connection
        // to be established.
        info.remove("path");

        String ducklake = removeOption(info, DUCKLAKE_OPTION);
        String ducklakeAlias = removeOption(info, DUCKLAKE_ALIAS_OPTION);

        Connection conn = DuckDBConnection.newConnection(url, read_only, info);

        initDucklake(conn, url, ducklake, ducklakeAlias);

        return conn;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(DUCKDB_URL_PREFIX);
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

    private static void initDucklake(Connection conn, String url, String ducklake, String ducklakeAlias)
        throws SQLException {
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

    private static String removeOption(Properties props, String opt) {
        Object obj = props.remove(opt);
        if (null != obj) {
            return obj.toString();
        }
        return null;
    }
}

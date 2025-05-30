package org.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

public class DuckDBDriver implements java.sql.Driver {

    public static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
    public static final String DUCKDB_USER_AGENT_PROPERTY = "custom_user_agent";
    public static final String JDBC_STREAM_RESULTS = "jdbc_stream_results";

    static final ScheduledThreadPoolExecutor scheduler;

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

        ParsedProps pp = parsePropsFromUrl(url);
        for (Map.Entry<String, String> en : pp.props.entrySet()) {
            info.put(en.getKey(), en.getValue());
        }
        url = pp.shortUrl;

        info.put("duckdb_api", "jdbc");

        // Apache Spark passes this option when SELECT on a JDBC DataSource
        // table is performed. It is the internal Spark option and is likely
        // passed by mistake, so we need to ignore it to allow the connection
        // to be established.
        info.remove("path");

        return DuckDBConnection.newConnection(url, read_only, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith("jdbc:duckdb:");
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
}

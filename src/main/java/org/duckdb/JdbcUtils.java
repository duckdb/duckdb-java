package org.duckdb;

import static org.duckdb.DuckDBDriver.DUCKDB_URL_PREFIX;
import static org.duckdb.DuckDBDriver.MEMORY_DB;

import java.sql.SQLException;
import java.util.Properties;

final class JdbcUtils {

    private JdbcUtils() {
    }

    @SuppressWarnings("unchecked")
    static <T> T unwrap(Object obj, Class<T> iface) throws SQLException {
        if (!iface.isInstance(obj)) {
            throw new SQLException(obj.getClass().getName() + " not unwrappable from " + iface.getName());
        }
        return (T) obj;
    }

    static String removeOption(Properties props, String opt) {
        return removeOption(props, opt, null);
    }

    static String removeOption(Properties props, String opt, String defaultVal) {
        Object obj = props.remove(opt);
        if (null != obj) {
            return obj.toString().trim();
        }
        return defaultVal;
    }

    static String getOption(Properties props, String opt) {
        Object obj = props.get(opt);
        if (null != obj) {
            return obj.toString().trim();
        }
        return null;
    }

    static void setDefaultOptionValue(Properties props, String opt, Object value) {
        if (props.containsKey(opt)) {
            return;
        }
        props.put(opt, value);
    }

    static boolean isStringTruish(String val, boolean defaultVal) throws SQLException {
        if (null == val) {
            return defaultVal;
        }
        String valLower = val.toLowerCase().trim();
        if (valLower.equals("true") || valLower.equals("1") || valLower.equals("yes") || valLower.equals("on")) {
            return true;
        }
        if (valLower.equals("false") || valLower.equals("0") || valLower.equals("no") || valLower.equals("off")) {
            return false;
        }
        throw new SQLException("Invalid boolean option value: " + val);
    }

    static String dbNameFromUrl(String url) throws SQLException {
        if (null == url) {
            throw new SQLException("Invalid null URL specified");
        }
        if (!url.startsWith(DUCKDB_URL_PREFIX)) {
            throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
        }
        final String shortUrl;
        if (url.contains(";")) {
            String[] parts = url.split(";");
            shortUrl = parts[0].trim();
        } else {
            shortUrl = url;
        }
        String dbName = shortUrl.substring(DUCKDB_URL_PREFIX.length()).trim();
        if (dbName.length() == 0) {
            dbName = MEMORY_DB;
        }
        if (dbName.startsWith(MEMORY_DB.substring(1))) {
            dbName = ":" + dbName;
        }
        return dbName;
    }

    static String bytesToHex(byte[] bytes) {
        if (null == bytes) {
            return "";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            // suppress
        }
    }
}

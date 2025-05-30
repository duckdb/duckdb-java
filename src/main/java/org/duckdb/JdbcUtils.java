package org.duckdb;

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
        Object obj = props.remove(opt);
        if (null != obj) {
            return obj.toString().trim();
        }
        return null;
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
}

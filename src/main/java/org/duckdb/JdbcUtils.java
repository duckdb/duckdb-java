package org.duckdb;

import static org.duckdb.DuckDBDriver.DUCKDB_URL_PREFIX;
import static org.duckdb.DuckDBDriver.MEMORY_DB;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Properties;

public final class JdbcUtils {

    private JdbcUtils() {
    }

    @SuppressWarnings("unchecked")
    static <T> T unwrap(Object obj, Class<T> iface) throws SQLException {
        if (!iface.isInstance(obj)) {
            throw createSQLException(obj.getClass().getName() + " not unwrappable from " + iface.getName(),
                                     ErrorCode.UNWRAP_FAILED);
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
        throw createSQLException("Invalid boolean option value: " + val, ErrorCode.BOOLEAN_OPTION_INVALID);
    }

    static String dbNameFromUrl(String url) throws SQLException {
        if (null == url) {
            throw createSQLException("Invalid null URL specified", ErrorCode.URL_NULL);
        }
        if (!url.startsWith(DUCKDB_URL_PREFIX)) {
            throw createSQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'", ErrorCode.URL_PREFIX);
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

    static String collectStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Builds a {@link SQLException} carrying the given message with a categorized {@link SQLState} and
     * a unique, per-call-site {@link ErrorCode}. The message string is used verbatim so that existing
     * callers that match on {@code getMessage()} keep working.
     */
    static SQLException createSQLException(String message, ErrorCode code) {
        return createSQLException(message, code, null);
    }

    /** {@link #createSQLException(String, ErrorCode)} with a cause. */
    static SQLException createSQLException(String message, ErrorCode code, Throwable cause) {
        return new SQLException(message, code.getSQLState().getCode(), code.getCode(), cause);
    }

    /** Categorized I/O error used by the {@code org.duckdb.io} helpers. */
    public static SQLException ioError(String message, Throwable cause) {
        return createSQLException(message, ErrorCode.IO_ERROR, cause);
    }

    /** Categorized I/O stream error used by the {@code org.duckdb.io} helpers. */
    public static SQLException ioStreamError(String message, Throwable cause) {
        return createSQLException(message, ErrorCode.IO_STREAM_ERROR, cause);
    }

    /** {@link #createSQLException(String, ErrorCode)} with an explicit SQLState override and cause. */
    static SQLException createSQLException(String message, SQLState state, int code, Throwable cause) {
        return new SQLException(message, state.getCode(), code, cause);
    }

    /**
     * Builds a {@link SQLException} for a native DuckDB error surfaced as free text (e.g. from
     * {@code duckdb_result_error} or {@code duckdb_appender_error}). The SQLState is derived from the
     * error prefix via {@link #nativeState(String)}; the message is preserved verbatim.
     */
    static SQLException createSQLExceptionFromNativeError(String nativeMessage) {
        return createSQLExceptionFromNativeError(nativeMessage, null);
    }

    /** {@link #createSQLExceptionFromNativeError(String)} with a cause. */
    static SQLException createSQLExceptionFromNativeError(String nativeMessage, Throwable cause) {
        String message = nativeMessage == null ? "" : nativeMessage;
        return createSQLException(message, nativeState(message), ErrorCode.NATIVE_UNDECODED.getCode(), cause);
    }

    /**
     * Maps a DuckDB free-text error prefix to a categorized {@link SQLState}. Returns {@code HY000}
     * when the prefix is not recognised. Prefix matching is case-insensitive and non-exclusive: the
     * first matching rule wins.
     */
    static SQLState nativeState(String message) {
        if (message == null) {
            return SQLState.HY000;
        }
        String m = message.toLowerCase();
        if (contains(m, "parser error") || contains(m, "syntax error") || contains(m, "parse error")) {
            return SQLState.SYNTAX_ERROR;
        }
        if (contains(m, "binder error") || contains(m, "catalog error") || contains(m, "invalid catalog") ||
            contains(m, "table with name") || contains(m, "table \"") || contains(m, "does not exist")) {
            return SQLState.TABLE_NOT_FOUND;
        }
        if (contains(m, "conversion error") || contains(m, "cast error") || contains(m, "invalid type") ||
            contains(m, "failed to cast")) {
            return SQLState.INVALID_CHARACTER_VALUE_FOR_CAST;
        }
        if (contains(m, "out of range") || contains(m, "overflow") || contains(m, "too large")) {
            return SQLState.NUMERIC_VALUE_OUT_OF_RANGE;
        }
        if (contains(m, "constraint error") || contains(m, "duplicate key") || contains(m, "not null constraint") ||
            contains(m, "unique constraint")) {
            return SQLState.INTEGRITY_CONSTRAINT;
        }
        if (contains(m, "connection error")) {
            return SQLState.CONNECTION_REJECTED;
        }
        if (contains(m, "not supported")) {
            return SQLState.NOT_SUPPORTED;
        }
        return SQLState.HY000;
    }

    private static boolean contains(String haystack, String needle) {
        return haystack.contains(needle);
    }
}

package org.duckdb;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.sql.SQLException;

/**
 * Unique vendor error code plus its categorized {@link SQLState} for every {@link java.sql.SQLException}
 * the DuckDB JDBC driver may throw. Each constant identifies a single, fixed call site so that a caller
 * seeing {@code getErrorCode()} can pinpoint the exact origin of the failure.
 *
 * <p>The numeric codes are deterministic and stable: each distinct origin (throwing class) is assigned a
 * 100-wide block starting at {@code 1000}, allocated in order of first appearance in this enum; within a
 * block the code increments sequentially by declaration order. Codes are therefore always non-negative,
 * grouped by origin, and unique. The {@code getOrigin()} identifies the throwing class.
 *
 * <p>Native DuckDB errors (returned as free text by the C API) are wrapped by
 * {@link JdbcUtils#createSQLExceptionFromNativeError(String, Throwable)} and receive
 * {@link #NATIVE_UNDECODED}; their SQLState is derived from the error text prefix, see
 * {@link JdbcUtils#nativeState(String)}.
 *
 * <p>This enum is the documented source of truth for error codes; see also {@code SQL_ERRORS.md}.
 */
enum ErrorCode {

    // ------------------------------------------------------------------
    // GENERAL / housekeeping (running general number, state HY000 unless
    // the semantic is more specific)
    // ------------------------------------------------------------------
    RESULT_SET_IS_CLOSED("DuckDBResultSet", SQLState.HY000, "ResultSet was closed"),
    RESULT_SET_NO_ROW("DuckDBResultSet", SQLState.NO_ROW_FOUND, "No row in context"),
    RESULT_SET_COLUMN_OOB("DuckDBResultSet", SQLState.COLUMN_NOT_FOUND, "Column index out of bounds"),
    RESULT_SET_COLUMN_LABEL("DuckDBResultSet", SQLState.COLUMN_NOT_FOUND, "Could not find column with label"),
    RESULT_SET_NULL_LABEL("DuckDBResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "columnLabel is null"),
    RESULT_SET_INVALID_POS_LEN("DuckDBResultSet", SQLState.SUBSTRING_ERROR, "Invalid position or length"),
    RESULT_SET_BAD_FETCH_SIZE("DuckDBResultSet", SQLState.INVALID_ATTRIBUTE_VALUE, "Fetch size has to be >= 0"),
    RESULT_SET_TYPE_NULL("DuckDBResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "type is null"),
    RESULT_SET_CONVERSION("DuckDBResultSet", SQLState.DATA_EXCEPTION, "Type conversion failure"),
    RESULT_SET_NATIVE_WRAP("DuckDBResultSet", SQLState.HY000, "Internal wrap error"),

    GENERATED_KEYS_IS_CLOSED("DuckDBGeneratedKeysResultSet", SQLState.HY000, "ResultSet was closed"),
    GENERATED_KEYS_NO_ROW("DuckDBGeneratedKeysResultSet", SQLState.NO_ROW_FOUND, "No row in context"),
    GENERATED_KEYS_COLUMN_OOB("DuckDBGeneratedKeysResultSet", SQLState.COLUMN_NOT_FOUND, "Column index out of bounds"),
    GENERATED_KEYS_COLUMN_LABEL("DuckDBGeneratedKeysResultSet", SQLState.COLUMN_NOT_FOUND,
                                "Could not find column with label"),
    GENERATED_KEYS_CONVERSION("DuckDBGeneratedKeysResultSet", SQLState.DATA_EXCEPTION, "Value conversion failure"),
    GENERATED_KEYS_NULL_TYPE("DuckDBGeneratedKeysResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "Type argument cannot be null"),

    ARRAY_RS_ERROR("DuckDBArrayResultSet", SQLState.DATA_EXCEPTION, "Array result set conversion"),
    ARRAY_RS_NUMERIC_GETTER("DuckDBArrayResultSet", SQLState.DATA_EXCEPTION,
                            "Array first element requires numeric getter"),
    ARRAY_RS_COLUMN_COUNT("DuckDBArrayResultSet", SQLState.COLUMN_NOT_FOUND,
                          "Array-backed ResultSet must have two columns"),
    ARRAY_RS_COLUMN_LABEL("DuckDBArrayResultSet", SQLState.COLUMN_NOT_FOUND, "Could not find column with label"),

    META_COLUMN_OOB("DuckDBResultSetMetaData", SQLState.COLUMN_NOT_FOUND, "Column index out of bounds"),

    PARAM_META_INDEX_OOB("DuckDBParameterMetaData", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Parameter index out of bounds"),
    PARAM_META_UNSUPPORTED("DuckDBParameterMetaData", SQLState.NOT_SUPPORTED, "Operation not supported"),

    VECTOR_TYPE_READ("DuckDBVectorTypeInfo", SQLState.DATA_EXCEPTION, "Cannot read vector type"),
    VECTOR_TYPE_UNSUPPORTED("DuckDBVectorTypeInfo", SQLState.NOT_SUPPORTED, "Unsupported vector type"),

    LOGICAL_TYPE_CREATE("DuckDBLogicalType", SQLState.DATA_EXCEPTION, "Failed to create logical type"),
    LOGICAL_TYPE_NULL("DuckDBLogicalType", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "Logical type cannot be null"),
    LOGICAL_TYPE_CONVERSION("DuckDBLogicalType", SQLState.DATA_EXCEPTION, "Unsupported logical type"),
    LOGICAL_TYPE_CLOSED("DuckDBLogicalType", SQLState.HY000, "Logical type is already closed"),
    LOGICAL_TYPE_UDF("DuckDBLogicalType", SQLState.DATA_EXCEPTION, "Unsupported logical type for UDF registration"),
    LOGICAL_TYPE_DECIMAL_WIDTH("DuckDBLogicalType", SQLState.NUMERIC_VALUE_OUT_OF_RANGE, "DECIMAL width out of range"),
    LOGICAL_TYPE_DECIMAL_SCALE("DuckDBLogicalType", SQLState.NUMERIC_VALUE_OUT_OF_RANGE, "DECIMAL scale out of range"),

    HUGEINT_NULL("DuckDBHugeInt", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "BigInteger instance is null"),
    HUGEINT_RANGE("DuckDBHugeInt", SQLState.NUMERIC_VALUE_OUT_OF_RANGE, "BigInteger out of range for HUGEINT"),
    TIMESTAMP_UNSUPPORTED_UNIT("DuckDBTimestamp", SQLState.NOT_SUPPORTED, "Unsupported unit type"),
    UNWRAP_FAILED("JdbcUtils", SQLState.HY000, "Object not unwrappable"),
    BOOLEAN_OPTION_INVALID("JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid boolean option value"),
    URL_NULL("JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid null URL"),
    URL_PREFIX("JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "URL needs to start with jdbc:duckdb:"),
    IO_ERROR("io.IOUtils", SQLState.CONNECTION_IO, "I/O failure"),
    IO_STREAM_ERROR("io.IOUtils", SQLState.CONNECTION_IO, "I/O stream failure"),

    // ------------------------------------------------------------------
    // CONNECTION (08003 etc.)
    // ------------------------------------------------------------------
    CONNECTION_CLOSED("DuckDBConnection", SQLState.CONNECTION_CLOSED, "Connection was closed"),
    CONNECTION_LOCK_STATE("DuckDBConnection", SQLState.FUNCTION_SEQUENCE_ERROR, "Connection lock state error"),
    CONNECTION_SESSION_INIT("DuckDBConnection", SQLState.CONNECTION_UNABLE_ESTABLISH, "Session init failure"),
    CONNECTION_STARTUP("DuckDBConnection", SQLState.CONNECTION_UNABLE_ESTABLISH, "Native connection startup failure"),

    DRIVER_URL_ENTRY("DuckDBDriver", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid URL entry"),
    DRIVER_OPTION_DUP("DuckDBDriver", SQLState.INVALID_ATTRIBUTE_VALUE, "Option specified more than once"),
    DRIVER_SESSION_FILE("DuckDBDriver", SQLState.CONNECTION_REJECTED, "Session init SQL file error"),
    DRIVER_SESSION_HASH("DuckDBDriver", SQLState.CONNECTION_REJECTED, "Session init SQL file SHA-256 mismatch"),
    DRIVER_SESSION_MARKER("DuckDBDriver", SQLState.CONNECTION_REJECTED, "Connection init marker"),
    DRIVER_NATIVE_WRAP("DuckDBDriver", SQLState.CONNECTION_UNABLE_ESTABLISH, "Native startup error"),

    SINGLE_VALUE_BAD_CONNECTION("DuckDBSingleValueAppender", SQLState.CONNECTION_CLOSED, "Invalid connection"),

    PREPARED_CONNECTION_NULL("DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "connection parameter cannot be null"),
    PREPARED_SQL_NULL("DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                      "sql query parameter cannot be null"),
    PREPARED_IS_CLOSED("DuckDBPreparedStatement", SQLState.CONNECTION_CLOSED, "Statement was closed"),
    PREPARED_CONN_CLOSED("DuckDBPreparedStatement", SQLState.CONNECTION_CLOSED, "Connection was closed"),
    PREPARED_CHUNK_INTERFACE("DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR,
                             "Data Chunk interface misuse"),
    PREPARED_NO_RESULT_SET("DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR,
                           "executeQuery with no ResultSet"),
    PREPARED_PARAM_OOB("DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                       "Parameter index out of bounds"),
    PREPARED_NEG_TIMEOUT("DuckDBPreparedStatement", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid negative timeout"),
    PREPARED_NATIVE_PREPARE("DuckDBPreparedStatement", SQLState.HY000, "Native prepare error"),
    PREPARED_NATIVE_EXECUTE("DuckDBPreparedStatement", SQLState.HY000, "Native execute error"),
    PREPARED_NATIVE_WRAP("DuckDBPreparedStatement", SQLState.HY000, "Native wrap error"),
    PREPARED_CONVERSION("DuckDBPreparedStatement", SQLState.DATA_EXCEPTION, "Value conversion failure"),
    PREPARED_UNKNOWN_TYPE("DuckDBPreparedStatement", SQLState.NOT_SUPPORTED, "Unknown target SQL type"),
    PREPARED_BATCH_MISUSE("DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR, "Batched query misuse"),
    PREPARED_NO_QUERY("DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR, "Query to execute not specified"),
    PREPARED_STREAM_EXECUTE("DuckDBPreparedStatement", SQLState.HY000, "Streaming execute error"),

    // ------------------------------------------------------------------
    // SCALAR / TABLE FUNCTION BUILDING (validation errors -> HY024 / HY000,
    // registration -> 42000)
    // ------------------------------------------------------------------
    FUNCTION_CREATE("DuckDBScalarFunctionBuilder", SQLState.DATA_EXCEPTION, "Failed to create scalar function"),
    FUNCTION_NAME_EMPTY("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                        "Function name cannot be null or empty"),
    FUNCTION_PARAM_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                        "Parameter type cannot be null"),
    FUNCTION_PARAMS_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Parameter types cannot be null"),
    FUNCTION_RETURN_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Return type cannot be null"),
    FUNCTION_CALLBACK_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                           "Scalar function callback cannot be null"),
    FUNCTION_VARARGS_MISUSE("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE, "Varargs misuse"),
    FUNCTION_PARAM_COUNT("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                         "Callback parameter count mismatch"),
    FUNCTION_TYPE_MISMATCH("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                           "Callback parameter/return type mismatch"),
    FUNCTION_VARARGS_TYPE_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Varargs type cannot be null"),
    FUNCTION_CONNECTION_NULL("DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "Connection cannot be null"),
    FUNCTION_NO_NAME("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE, "Function name must be defined"),
    FUNCTION_NO_RETURN("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE, "Return type must be defined"),
    FUNCTION_NO_CALLBACK("DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                         "Scalar function callback must be defined"),
    FUNCTION_FINALIZED("DuckDBScalarFunctionBuilder", SQLState.FUNCTION_SEQUENCE_ERROR, "Builder already finalized"),
    FUNCTION_REGISTER_NATIVE("DuckDBScalarFunctionBuilder", SQLState.SYNTAX_ERROR,
                             "Failed to register scalar function"),
    FUNCTION_REGISTER_CONNECTION("DuckDBScalarFunctionBuilder", SQLState.CONNECTION_CLOSED,
                                 "Registration requires a DuckDB JDBC connection"),

    ADAPTER_JAVA_TYPE_NULL("DuckDBScalarFunctionAdapter", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                           "Java type cannot be null"),
    ADAPTER_LOGICAL_TYPE_NULL("DuckDBScalarFunctionAdapter", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                              "Logical type cannot be null"),
    ADAPTER_JAVA_TYPE_UNSUPPORTED("DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                  "Unsupported Java type mapping"),
    ADAPTER_LOGICAL_TYPE_UNSUPPORTED("DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                     "Unsupported logical type mapping"),
    ADAPTER_DUCKDB_TYPE_UNSUPPORTED("DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                    "Unsupported DuckDB type mapping"),
    ADAPTER_MAPPING("DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION, "Function mapping error"),

    TABLE_FUNCTION_CREATE("DuckDBTableFunctionBuilder", SQLState.DATA_EXCEPTION, "Failed to create table function"),
    TABLE_FUNCTION_NAME_EMPTY("DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                              "Function name cannot be null or empty"),
    TABLE_FUNCTION_PARAM_NULL("DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                              "Parameter type cannot be null"),
    TABLE_FUNCTION_PARAMS_NULL("DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Parameter types cannot be null"),
    TABLE_FUNCTION_PARAM_NAME_EMPTY("DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                                    "Parameter name cannot be empty"),
    TABLE_FUNCTION_CONNECTION_NULL("DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                                   "Connection cannot be null"),
    TABLE_FUNCTION_OBJECT_NULL("DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Table function object cannot be null"),
    TABLE_FUNCTION_NO_NAME("DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                           "Function name must be defined"),
    TABLE_FUNCTION_NO_CALLBACK("DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                               "Table function callback must be defined"),
    TABLE_FUNCTION_FINALIZED("DuckDBTableFunctionBuilder", SQLState.FUNCTION_SEQUENCE_ERROR,
                             "Builder already finalized"),
    TABLE_FUNCTION_REGISTER_NATIVE("DuckDBTableFunctionBuilder", SQLState.SYNTAX_ERROR,
                                   "Failed to register table function"),

    // ------------------------------------------------------------------
    // APPENDER
    // ------------------------------------------------------------------
    APPENDER_NATIVE("DuckDBAppender", SQLState.HY000, "Native appender error"),
    APPENDER_RAW_ERROR("DuckDBAppender", SQLState.HY000, "duckdb_appender_create_ext error"),
    APPENDER_CONVERSION("DuckDBAppender", SQLState.DATA_EXCEPTION, "Appender value/type conversion failure"),
    APPENDER_SEQUENCE("DuckDBAppender", SQLState.FUNCTION_SEQUENCE_ERROR,
                      "Appender begin/end operation sequence error"),
    APPENDER_CHUNK("DuckDBAppender", SQLState.DATA_EXCEPTION, "Data chunk initialization/reset failure"),
    APPENDER_IS_CLOSED("DuckDBAppender", SQLState.CONNECTION_CLOSED, "Appender was closed"),
    APPENDER_PARAM("DuckDBAppender", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "Appender parameter error"),
    APPENDER_HUGEINT_RANGE("DuckDBAppender", SQLState.NUMERIC_VALUE_OUT_OF_RANGE,
                           "BigInteger out of range for HUGEINT"),

    // ------------------------------------------------------------------
    // BINDINGS / chunked result / native
    // ------------------------------------------------------------------
    BINDINGS_UNKNOWN_ID("DuckDBBindings", SQLState.DATA_EXCEPTION, "Invalid unknown ID"),
    CHUNKED_RESULT_NATIVE("DuckDBChunkedResult", SQLState.HY000, "Query failed"),

    /** Fallback for a native DuckDB error whose text prefix could not be classified. */
    NATIVE_UNDECODED("duckdb.native", SQLState.HY000, "Undecoded native DuckDB error");

    /** Code-to-name lookup pre-filled in {@link #allocateCodes()}. */
    private static final Map<Integer, String> CODES = allocateCodes();

    private static Map<Integer, String> allocateCodes() {
        // Allocate a deterministic, stable numeric code per constant. Each distinct origin is assigned a
        // 100-wide block (1000, 1100, 1200, ...) in order of first appearance; within a block the code
        // increments by declaration order. Codes are therefore non-negative, grouped by origin, and stable
        // as long as the enumeration order is not changed. Uniqueness and non-negativity are enforced below.
        Map<String, Integer> blockByOrigin = new LinkedHashMap<>();
        Map<String, Integer> seqByOrigin = new LinkedHashMap<>();
        Map<Integer, String> codes = new LinkedHashMap<>();
        for (ErrorCode ec : values()) {
            Integer block = blockByOrigin.get(ec.origin);
            if (block == null) {
                block = 1000 + blockByOrigin.size() * 100;
                blockByOrigin.put(ec.origin, block);
                seqByOrigin.put(ec.origin, 0);
            }
            int seq = seqByOrigin.get(ec.origin);
            int code = block + seq;
            seqByOrigin.put(ec.origin, seq + 1);
            if (code < 0 || codes.put(code, ec.name()) != null) {
                throw new ExceptionInInitializerError("Duplicate or negative error code " + code + " for " + ec);
            }
            ec.code = code;
        }
        return codes;
    }

    private final String origin;
    private final SQLState state;
    private final String description;
    private int code;

    ErrorCode(String origin, SQLState state, String description) {
        this.origin = origin;
        this.state = state;
        this.description = description;
    }

    /** The unique, stable vendor error code. Unique across every constant. */
    int getCode() {
        return code;
    }

    /** The categorized SQLState assigned to this error origin. */
    SQLState getSQLState() {
        return state;
    }

    /** The driver class (last component) that throws this error. */
    String getOrigin() {
        return origin;
    }

    /** Short human-readable description of the failure. */
    String getDescription() {
        return description;
    }

    /** Builds a fully-populated {@link SQLException} for this code. */
    SQLException asException() {
        return new SQLException(description, state.getCode(), code);
    }

    /** @return the number of distinct error codes (useful for the uniqueness invariant test). */
    static int count() {
        return values().length;
    }

    /** @return true if {@code code} was assigned to exactly one enum constant. */
    static boolean isUnique(int code) {
        long matches = Arrays.stream(values()).filter(ec -> ec.code == code).count();
        return matches == 1;
    }
}
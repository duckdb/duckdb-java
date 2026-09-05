package org.duckdb;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * Unique vendor error code plus its categorized {@link SQLState} for every {@link java.sql.SQLException}
 * the DuckDB JDBC driver may throw. Each constant identifies a single, fixed call site so that a caller
 * seeing {@code getErrorCode()} can pinpoint the exact origin of the failure.
 *
 * <p>The numeric codes are deterministic and stable, baked as a literal on each constant and never computed
 * at class-load: each distinct origin (throwing class) is assigned a 100-wide block starting at
 * {@code 1000}, in order of first appearance in this enum; within a block the code increments sequentially
 * by declaration order. Codes are therefore always non-negative, grouped by origin, and unique. The
 * {@code getOrigin()} identifies the throwing class.
 *
 * <p>Native DuckDB errors (returned as free text by the C API) are wrapped by
 * {@link JdbcUtils#createSQLException(String, ErrorCode, Throwable)} with a {@code null} {@code ErrorCode}
 * and receive {@link #NATIVE_UNDECODED}; their SQLState is derived from the error text prefix, see
 * {@link JdbcUtils#nativeState(String)}.
 *
 * <p>This enum is the documented source of truth for error codes; see also {@code SQL_ERRORS.md}.
 */
enum ErrorCode {

    // ------------------------------------------------------------------
    // GENERAL / housekeeping (running general number, state HY000 unless
    // the semantic is more specific)
    // ------------------------------------------------------------------
    RESULT_SET_IS_CLOSED(1000, "DuckDBResultSet", SQLState.HY000, "ResultSet was closed"),
    RESULT_SET_NO_ROW(1001, "DuckDBResultSet", SQLState.NO_ROW_FOUND, "No row in context"),
    RESULT_SET_COLUMN_OOB(1002, "DuckDBResultSet", SQLState.COLUMN_NOT_FOUND, "Column index out of bounds"),
    RESULT_SET_COLUMN_LABEL(1003, "DuckDBResultSet", SQLState.COLUMN_NOT_FOUND, "Could not find column with label"),
    RESULT_SET_NULL_LABEL(1004, "DuckDBResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "columnLabel is null"),
    RESULT_SET_INVALID_POS_LEN(1005, "DuckDBResultSet", SQLState.SUBSTRING_ERROR, "Invalid position or length"),
    RESULT_SET_BAD_FETCH_SIZE(1006, "DuckDBResultSet", SQLState.INVALID_ATTRIBUTE_VALUE, "Fetch size has to be >= 0"),
    RESULT_SET_TYPE_NULL(1007, "DuckDBResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "type is null"),
    RESULT_SET_CONVERSION(1008, "DuckDBResultSet", SQLState.DATA_EXCEPTION, "Type conversion failure"),
    RESULT_SET_NATIVE_WRAP(1009, "DuckDBResultSet", SQLState.HY000, "Internal wrap error"),

    GENERATED_KEYS_IS_CLOSED(1100, "DuckDBGeneratedKeysResultSet", SQLState.HY000, "ResultSet was closed"),
    GENERATED_KEYS_NO_ROW(1101, "DuckDBGeneratedKeysResultSet", SQLState.NO_ROW_FOUND, "No row in context"),
    GENERATED_KEYS_COLUMN_OOB(1102, "DuckDBGeneratedKeysResultSet", SQLState.COLUMN_NOT_FOUND,
                              "Column index out of bounds"),
    GENERATED_KEYS_COLUMN_LABEL(1103, "DuckDBGeneratedKeysResultSet", SQLState.COLUMN_NOT_FOUND,
                                "Could not find column with label"),
    GENERATED_KEYS_CONVERSION(1104, "DuckDBGeneratedKeysResultSet", SQLState.DATA_EXCEPTION,
                              "Value conversion failure"),
    GENERATED_KEYS_NULL_TYPE(1105, "DuckDBGeneratedKeysResultSet", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "Type argument cannot be null"),

    ARRAY_RS_ERROR(1200, "DuckDBArrayResultSet", SQLState.DATA_EXCEPTION, "Array result set conversion"),
    ARRAY_RS_NUMERIC_GETTER(1201, "DuckDBArrayResultSet", SQLState.DATA_EXCEPTION,
                            "Array first element requires numeric getter"),
    ARRAY_RS_COLUMN_COUNT(1202, "DuckDBArrayResultSet", SQLState.COLUMN_NOT_FOUND,
                          "Array-backed ResultSet must have two columns"),
    ARRAY_RS_COLUMN_LABEL(1203, "DuckDBArrayResultSet", SQLState.COLUMN_NOT_FOUND, "Could not find column with label"),

    META_COLUMN_OOB(1300, "DuckDBResultSetMetaData", SQLState.COLUMN_NOT_FOUND, "Column index out of bounds"),

    PARAM_META_INDEX_OOB(1400, "DuckDBParameterMetaData", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Parameter index out of bounds"),
    PARAM_META_UNSUPPORTED(1401, "DuckDBParameterMetaData", SQLState.NOT_SUPPORTED, "Operation not supported"),

    VECTOR_TYPE_READ(1500, "DuckDBVectorTypeInfo", SQLState.DATA_EXCEPTION, "Cannot read vector type"),
    VECTOR_TYPE_UNSUPPORTED(1501, "DuckDBVectorTypeInfo", SQLState.NOT_SUPPORTED, "Unsupported vector type"),

    LOGICAL_TYPE_CREATE(1600, "DuckDBLogicalType", SQLState.DATA_EXCEPTION, "Failed to create logical type"),
    LOGICAL_TYPE_NULL(1601, "DuckDBLogicalType", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "Logical type cannot be null"),
    LOGICAL_TYPE_CONVERSION(1602, "DuckDBLogicalType", SQLState.DATA_EXCEPTION, "Unsupported logical type"),
    LOGICAL_TYPE_CLOSED(1603, "DuckDBLogicalType", SQLState.HY000, "Logical type is already closed"),
    LOGICAL_TYPE_UDF(1604, "DuckDBLogicalType", SQLState.DATA_EXCEPTION,
                     "Unsupported logical type for UDF registration"),
    LOGICAL_TYPE_DECIMAL_WIDTH(1605, "DuckDBLogicalType", SQLState.NUMERIC_VALUE_OUT_OF_RANGE,
                               "DECIMAL width out of range"),
    LOGICAL_TYPE_DECIMAL_SCALE(1606, "DuckDBLogicalType", SQLState.NUMERIC_VALUE_OUT_OF_RANGE,
                               "DECIMAL scale out of range"),

    HUGEINT_NULL(1700, "DuckDBHugeInt", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "BigInteger instance is null"),
    HUGEINT_RANGE(1701, "DuckDBHugeInt", SQLState.NUMERIC_VALUE_OUT_OF_RANGE, "BigInteger out of range for HUGEINT"),
    TIMESTAMP_UNSUPPORTED_UNIT(1800, "DuckDBTimestamp", SQLState.NOT_SUPPORTED, "Unsupported unit type"),
    UNWRAP_FAILED(1900, "JdbcUtils", SQLState.HY000, "Object not unwrappable"),
    BOOLEAN_OPTION_INVALID(1901, "JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid boolean option value"),
    URL_NULL(1902, "JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid null URL"),
    URL_PREFIX(1903, "JdbcUtils", SQLState.INVALID_ATTRIBUTE_VALUE, "URL needs to start with jdbc:duckdb:"),
    IO_ERROR(2000, "IOUtils", SQLState.CONNECTION_IO, "I/O failure"),
    IO_STREAM_ERROR(2001, "IOUtils", SQLState.CONNECTION_IO, "I/O stream failure"),

    // ------------------------------------------------------------------
    // CONNECTION (08003 etc.)
    // ------------------------------------------------------------------
    CONNECTION_CLOSED(2100, "DuckDBConnection", SQLState.CONNECTION_CLOSED, "Connection was closed"),
    CONNECTION_LOCK_STATE(2101, "DuckDBConnection", SQLState.FUNCTION_SEQUENCE_ERROR, "Connection lock state error"),
    CONNECTION_SESSION_INIT(2102, "DuckDBConnection", SQLState.CONNECTION_UNABLE_ESTABLISH, "Session init failure"),
    CONNECTION_STARTUP(2103, "DuckDBConnection", SQLState.CONNECTION_UNABLE_ESTABLISH,
                       "Native connection startup failure"),

    DRIVER_URL_ENTRY(2200, "DuckDBDriver", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid URL entry"),
    DRIVER_OPTION_DUP(2201, "DuckDBDriver", SQLState.INVALID_ATTRIBUTE_VALUE, "Option specified more than once"),
    DRIVER_SESSION_FILE(2202, "DuckDBDriver", SQLState.CONNECTION_REJECTED, "Session init SQL file error"),
    DRIVER_SESSION_HASH(2203, "DuckDBDriver", SQLState.CONNECTION_REJECTED, "Session init SQL file SHA-256 mismatch"),
    DRIVER_SESSION_MARKER(2204, "DuckDBDriver", SQLState.CONNECTION_REJECTED, "Connection init marker"),
    DRIVER_NATIVE_WRAP(2205, "DuckDBDriver", SQLState.CONNECTION_UNABLE_ESTABLISH, "Native startup error"),

    SINGLE_VALUE_BAD_CONNECTION(2300, "DuckDBSingleValueAppender", SQLState.CONNECTION_CLOSED, "Invalid connection"),

    PREPARED_CONNECTION_NULL(2400, "DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "connection parameter cannot be null"),
    PREPARED_SQL_NULL(2401, "DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                      "sql query parameter cannot be null"),
    PREPARED_IS_CLOSED(2402, "DuckDBPreparedStatement", SQLState.CONNECTION_CLOSED, "Statement was closed"),
    PREPARED_CONN_CLOSED(2403, "DuckDBPreparedStatement", SQLState.CONNECTION_CLOSED, "Connection was closed"),
    PREPARED_CHUNK_INTERFACE(2404, "DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR,
                             "Data Chunk interface misuse"),
    PREPARED_NO_RESULT_SET(2405, "DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR,
                           "executeQuery with no ResultSet"),
    PREPARED_PARAM_OOB(2406, "DuckDBPreparedStatement", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                       "Parameter index out of bounds"),
    PREPARED_NEG_TIMEOUT(2407, "DuckDBPreparedStatement", SQLState.INVALID_ATTRIBUTE_VALUE, "Invalid negative timeout"),
    PREPARED_NATIVE_PREPARE(2408, "DuckDBPreparedStatement", SQLState.HY000, "Native prepare error"),
    PREPARED_NATIVE_EXECUTE(2409, "DuckDBPreparedStatement", SQLState.HY000, "Native execute error"),
    PREPARED_NATIVE_WRAP(2410, "DuckDBPreparedStatement", SQLState.HY000, "Native wrap error"),
    PREPARED_CONVERSION(2411, "DuckDBPreparedStatement", SQLState.DATA_EXCEPTION, "Value conversion failure"),
    PREPARED_UNKNOWN_TYPE(2412, "DuckDBPreparedStatement", SQLState.NOT_SUPPORTED, "Unknown target SQL type"),
    PREPARED_BATCH_MISUSE(2413, "DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR, "Batched query misuse"),
    PREPARED_NO_QUERY(2414, "DuckDBPreparedStatement", SQLState.FUNCTION_SEQUENCE_ERROR,
                      "Query to execute not specified"),
    PREPARED_STREAM_EXECUTE(2415, "DuckDBPreparedStatement", SQLState.HY000, "Streaming execute error"),

    // ------------------------------------------------------------------
    // SCALAR / TABLE FUNCTION BUILDING (validation errors -> HY024 / HY000,
    // registration -> 42000)
    // ------------------------------------------------------------------
    FUNCTION_CREATE(2500, "DuckDBScalarFunctionBuilder", SQLState.DATA_EXCEPTION, "Failed to create scalar function"),
    FUNCTION_NAME_EMPTY(2501, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                        "Function name cannot be null or empty"),
    FUNCTION_PARAM_NULL(2502, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                        "Parameter type cannot be null"),
    FUNCTION_PARAMS_NULL(2503, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Parameter types cannot be null"),
    FUNCTION_RETURN_NULL(2504, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                         "Return type cannot be null"),
    FUNCTION_CALLBACK_NULL(2505, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                           "Scalar function callback cannot be null"),
    FUNCTION_VARARGS_MISUSE(2506, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE, "Varargs misuse"),
    FUNCTION_PARAM_COUNT(2507, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                         "Callback parameter count mismatch"),
    FUNCTION_TYPE_MISMATCH(2508, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                           "Callback parameter/return type mismatch"),
    FUNCTION_VARARGS_TYPE_NULL(2509, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Varargs type cannot be null"),
    FUNCTION_CONNECTION_NULL(2510, "DuckDBScalarFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                             "Connection cannot be null"),
    FUNCTION_NO_NAME(2511, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                     "Function name must be defined"),
    FUNCTION_NO_RETURN(2512, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                       "Return type must be defined"),
    FUNCTION_NO_CALLBACK(2513, "DuckDBScalarFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                         "Scalar function callback must be defined"),
    FUNCTION_FINALIZED(2514, "DuckDBScalarFunctionBuilder", SQLState.FUNCTION_SEQUENCE_ERROR,
                       "Builder already finalized"),
    FUNCTION_REGISTER_NATIVE(2515, "DuckDBScalarFunctionBuilder", SQLState.SYNTAX_ERROR,
                             "Failed to register scalar function"),
    FUNCTION_REGISTER_CONNECTION(2516, "DuckDBScalarFunctionBuilder", SQLState.CONNECTION_CLOSED,
                                 "Registration requires a DuckDB JDBC connection"),

    ADAPTER_JAVA_TYPE_NULL(2600, "DuckDBScalarFunctionAdapter", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                           "Java type cannot be null"),
    ADAPTER_LOGICAL_TYPE_NULL(2601, "DuckDBScalarFunctionAdapter", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                              "Logical type cannot be null"),
    ADAPTER_JAVA_TYPE_UNSUPPORTED(2602, "DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                  "Unsupported Java type mapping"),
    ADAPTER_LOGICAL_TYPE_UNSUPPORTED(2603, "DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                     "Unsupported logical type mapping"),
    ADAPTER_DUCKDB_TYPE_UNSUPPORTED(2604, "DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION,
                                    "Unsupported DuckDB type mapping"),
    ADAPTER_MAPPING(2605, "DuckDBScalarFunctionAdapter", SQLState.DATA_EXCEPTION, "Function mapping error"),

    TABLE_FUNCTION_CREATE(2700, "DuckDBTableFunctionBuilder", SQLState.DATA_EXCEPTION,
                          "Failed to create table function"),
    TABLE_FUNCTION_NAME_EMPTY(2701, "DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                              "Function name cannot be null or empty"),
    TABLE_FUNCTION_PARAM_NULL(2702, "DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                              "Parameter type cannot be null"),
    TABLE_FUNCTION_PARAMS_NULL(2703, "DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Parameter types cannot be null"),
    TABLE_FUNCTION_PARAM_NAME_EMPTY(2704, "DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                                    "Parameter name cannot be empty"),
    TABLE_FUNCTION_CONNECTION_NULL(2705, "DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                                   "Connection cannot be null"),
    TABLE_FUNCTION_OBJECT_NULL(2706, "DuckDBTableFunctionBuilder", SQLState.PARAMETER_INDEX_OUT_OF_RANGE,
                               "Table function object cannot be null"),
    TABLE_FUNCTION_NO_NAME(2707, "DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                           "Function name must be defined"),
    TABLE_FUNCTION_NO_CALLBACK(2708, "DuckDBTableFunctionBuilder", SQLState.INVALID_ATTRIBUTE_VALUE,
                               "Table function callback must be defined"),
    TABLE_FUNCTION_FINALIZED(2709, "DuckDBTableFunctionBuilder", SQLState.FUNCTION_SEQUENCE_ERROR,
                             "Builder already finalized"),
    TABLE_FUNCTION_REGISTER_NATIVE(2710, "DuckDBTableFunctionBuilder", SQLState.SYNTAX_ERROR,
                                   "Failed to register table function"),

    // ------------------------------------------------------------------
    // APPENDER
    // ------------------------------------------------------------------
    APPENDER_NATIVE(2800, "DuckDBAppender", SQLState.HY000, "Native appender error"),
    APPENDER_RAW_ERROR(2801, "DuckDBAppender", SQLState.HY000, "duckdb_appender_create_ext error"),
    APPENDER_CONVERSION(2802, "DuckDBAppender", SQLState.DATA_EXCEPTION, "Appender value/type conversion failure"),
    APPENDER_SEQUENCE(2803, "DuckDBAppender", SQLState.FUNCTION_SEQUENCE_ERROR,
                      "Appender begin/end operation sequence error"),
    APPENDER_CHUNK(2804, "DuckDBAppender", SQLState.DATA_EXCEPTION, "Data chunk initialization/reset failure"),
    APPENDER_IS_CLOSED(2805, "DuckDBAppender", SQLState.CONNECTION_CLOSED, "Appender was closed"),
    APPENDER_PARAM(2806, "DuckDBAppender", SQLState.PARAMETER_INDEX_OUT_OF_RANGE, "Appender parameter error"),
    APPENDER_HUGEINT_RANGE(2807, "DuckDBAppender", SQLState.NUMERIC_VALUE_OUT_OF_RANGE,
                           "BigInteger out of range for HUGEINT"),

    // ------------------------------------------------------------------
    // BINDINGS / chunked result / native
    // ------------------------------------------------------------------
    BINDINGS_UNKNOWN_ID(2900, "DuckDBBindings", SQLState.DATA_EXCEPTION, "Invalid unknown ID"),
    CHUNKED_RESULT_NATIVE(3000, "DuckDBChunkedResult", SQLState.HY000, "Query failed"),

    /** Fallback for a native DuckDB error whose text prefix could not be classified. */
    NATIVE_UNDECODED(3100, "duckdb.native", SQLState.HY000, "Undecoded native DuckDB error");

    private final String origin;
    private final SQLState state;
    private final String description;
    private final int code;

    ErrorCode(int code, String origin, SQLState state, String description) {
        this.code = code;
        this.origin = origin;
        this.state = state;
        this.description = description;
    }

    /** The unique, stable vendor error code. Unique across every constant. */
    final int getCode() {
        return code;
    }

    /** The categorized SQLState assigned to this error origin. */
    final SQLState getSQLState() {
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
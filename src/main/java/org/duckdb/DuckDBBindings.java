package org.duckdb;

import static org.duckdb.JdbcUtils.createSQLException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;

public class DuckDBBindings {

    static {
        try {
            Class.forName(DuckDBNative.class.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // common

    static native long duckdb_vector_size();

    // scalar function

    static native ByteBuffer duckdb_create_scalar_function();

    static native void duckdb_destroy_scalar_function(ByteBuffer scalarFunction);

    static native void duckdb_scalar_function_set_name(ByteBuffer scalarFunction, byte[] name);

    static native void duckdb_scalar_function_add_parameter(ByteBuffer scalarFunction, ByteBuffer logicalType);

    static native void duckdb_scalar_function_set_return_type(ByteBuffer scalarFunction, ByteBuffer logicalType);

    static native void duckdb_scalar_function_set_varargs(ByteBuffer scalarFunction, ByteBuffer logicalType);

    static native void duckdb_scalar_function_set_volatile(ByteBuffer scalarFunction);

    static native void duckdb_scalar_function_set_special_handling(ByteBuffer scalarFunction);

    static native int duckdb_register_scalar_function(ByteBuffer connection, ByteBuffer scalarFunction);

    static native void duckdb_scalar_function_set_extra_info(ByteBuffer scalarFunction, Object callback);

    static native void duckdb_scalar_function_set_function(ByteBuffer scalarFunction);

    static native void duckdb_scalar_function_set_error(ByteBuffer functionInfo, byte[] error);

    // table function bind

    static native void duckdb_bind_add_result_column(ByteBuffer bind_info, byte[] name, ByteBuffer logical_type);

    static native long duckdb_bind_get_parameter_count(ByteBuffer bind_info);

    static native ByteBuffer duckdb_bind_get_parameter(ByteBuffer bind_info, long index);

    static native ByteBuffer duckdb_bind_get_named_parameter(ByteBuffer bind_info, byte[] name);

    static native void duckdb_bind_set_bind_data(ByteBuffer bind_info, Object bind_data);

    static native void duckdb_bind_set_cardinality(ByteBuffer bind_info, long cardinality, boolean is_exact);

    static native void duckdb_bind_set_error(ByteBuffer bind_info, byte[] error);

    // table function init

    static native Object duckdb_init_get_bind_data(ByteBuffer init_info);

    static native void duckdb_init_set_init_data(ByteBuffer init_info, Object init_data);

    static native long duckdb_init_get_column_count(ByteBuffer init_info);

    static native long duckdb_init_get_column_index(ByteBuffer init_info, long column_index);

    static native void duckdb_init_set_max_threads(ByteBuffer init_info, long max_threads);

    static native void duckdb_init_set_error(ByteBuffer init_info, byte[] error);

    // table function

    static native ByteBuffer duckdb_create_table_function();

    static native void duckdb_destroy_table_function(ByteBuffer table_function);

    static native void duckdb_table_function_set_name(ByteBuffer table_function, byte[] name);

    static native void duckdb_table_function_add_parameter(ByteBuffer table_function, ByteBuffer logical_type);

    static native void duckdb_table_function_add_named_parameter(ByteBuffer table_function, byte[] name,
                                                                 ByteBuffer logical_type);

    static native void duckdb_table_function_set_extra_info(ByteBuffer table_function,
                                                            DuckDBTableFunctionWrapper callback);

    static native void duckdb_table_function_set_bind(ByteBuffer table_function);

    static native void duckdb_table_function_set_init(ByteBuffer table_function);

    static native void duckdb_table_function_set_local_init(ByteBuffer table_function);

    static native void duckdb_table_function_set_function(ByteBuffer table_function);

    static native void duckdb_table_function_supports_projection_pushdown(ByteBuffer table_function, boolean pushdown);

    static native int duckdb_register_table_function(ByteBuffer connection, ByteBuffer table_function);

    static native Object duckdb_function_get_bind_data(ByteBuffer table_function_info);

    static native Object duckdb_function_get_init_data(ByteBuffer table_function_info);

    static native Object duckdb_function_get_local_init_data(ByteBuffer table_function_info);

    static native void duckdb_function_set_error(ByteBuffer table_function_info, byte[] error);

    // logical type

    static native ByteBuffer duckdb_create_logical_type(int duckdb_type);

    static native ByteBuffer duckdb_create_decimal_type(int width, int scale);

    static native int duckdb_get_type_id(ByteBuffer logical_type);

    static native int duckdb_decimal_width(ByteBuffer logical_type);

    static native int duckdb_decimal_scale(ByteBuffer logical_type);

    static native int duckdb_decimal_internal_type(ByteBuffer logical_type);

    static native ByteBuffer duckdb_create_list_type(ByteBuffer logical_type);

    static native ByteBuffer duckdb_create_array_type(ByteBuffer logical_type, long array_size);

    static native ByteBuffer duckdb_create_struct_type(ByteBuffer[] member_types, byte[][] member_names);

    static native long duckdb_struct_type_child_count(ByteBuffer logical_type);

    static native byte[] duckdb_struct_type_child_name(ByteBuffer logical_type, long index);

    static native long duckdb_array_type_array_size(ByteBuffer logical_type);

    static native void duckdb_destroy_logical_type(ByteBuffer logical_type);

    static native int duckdb_enum_internal_type(ByteBuffer logical_type);

    static native long duckdb_enum_dictionary_size(ByteBuffer logical_type);

    static native byte[] duckdb_enum_dictionary_value(ByteBuffer logical_type, long index);

    // vector

    static native ByteBuffer duckdb_create_vector(ByteBuffer logical_type);

    static native void duckdb_destroy_vector(ByteBuffer vector);

    static native ByteBuffer duckdb_vector_get_column_type(ByteBuffer vector);

    static native ByteBuffer duckdb_vector_get_data(ByteBuffer vector, long size_bytes);

    static native ByteBuffer duckdb_vector_get_data_zeroed(ByteBuffer vector, long size_bytes);

    static native ByteBuffer duckdb_vector_get_validity(ByteBuffer vector, long vector_size_elems);

    static native void duckdb_vector_ensure_validity_writable(ByteBuffer vector);

    static native void duckdb_vector_assign_string_element_len(ByteBuffer vector, long index, byte[] str);

    static native void duckdb_vector_assign_string_elements(ByteBuffer vector, long index, byte[] data, long[] lengths,
                                                            int dataLength);

    static native byte[] duckdb_vector_get_string(ByteBuffer vectorData, long row);

    static native ByteBuffer duckdb_list_vector_get_child(ByteBuffer vector);

    static native long duckdb_list_vector_get_size(ByteBuffer vector);

    static native int duckdb_list_vector_set_size(ByteBuffer vector, long size);

    static native int duckdb_list_vector_reserve(ByteBuffer vector, long capacity);

    static native ByteBuffer duckdb_struct_vector_get_child(ByteBuffer vector, long index);

    static native ByteBuffer duckdb_array_vector_get_child(ByteBuffer vector);

    // validity

    static native boolean duckdb_validity_row_is_valid(ByteBuffer validity, long row);

    static native void duckdb_validity_set_row_validity(ByteBuffer validity, long row, boolean valid);

    // data chunk

    static native ByteBuffer duckdb_create_data_chunk(ByteBuffer[] logical_types);

    static native void duckdb_destroy_data_chunk(ByteBuffer chunk);

    static native void duckdb_data_chunk_reset(ByteBuffer chunk);

    static native long duckdb_data_chunk_get_column_count(ByteBuffer chunk);

    static native ByteBuffer duckdb_data_chunk_get_vector(ByteBuffer chunk, long col_idx);

    static native long duckdb_data_chunk_get_size(ByteBuffer chunk);

    static native void duckdb_data_chunk_set_size(ByteBuffer chunk, long size);

    // appender

    static native int duckdb_appender_create_ext(ByteBuffer connection, byte[] catalog, byte[] schema, byte[] table,
                                                 ByteBuffer[] out_appender);

    static native byte[] duckdb_appender_error(ByteBuffer appender);

    static native int duckdb_appender_flush(ByteBuffer appender);

    static native int duckdb_appender_close(ByteBuffer appender);

    static native int duckdb_appender_destroy(ByteBuffer appender);

    static native long duckdb_appender_column_count(ByteBuffer appender);

    static native ByteBuffer duckdb_appender_column_type(ByteBuffer appender, long col_idx);

    static native int duckdb_append_data_chunk(ByteBuffer appender, ByteBuffer chunk);

    static native int duckdb_append_default_to_chunk(ByteBuffer appender, ByteBuffer chunk, long col, long row);

    // value

    static native boolean duckdb_is_null_value(ByteBuffer value);

    static native int duckdb_get_value_type(ByteBuffer value);

    static native void duckdb_destroy_value(ByteBuffer value);

    static native boolean duckdb_get_bool(ByteBuffer value);

    static native byte duckdb_get_int8(ByteBuffer value);

    static native short duckdb_get_uint8(ByteBuffer value);

    static native short duckdb_get_int16(ByteBuffer value);

    static native int duckdb_get_uint16(ByteBuffer value);

    static native int duckdb_get_int32(ByteBuffer value);

    static native long duckdb_get_uint32(ByteBuffer value);

    static native long duckdb_get_int64(ByteBuffer value);

    static native long duckdb_get_uint64(ByteBuffer value);

    static native BigInteger duckdb_get_hugeint(ByteBuffer value);

    static native BigInteger duckdb_get_uhugeint(ByteBuffer value);

    static native BigInteger duckdb_get_bignum(ByteBuffer value);

    static native BigDecimal duckdb_get_decimal(ByteBuffer value);

    static native float duckdb_get_float(ByteBuffer value);

    static native double duckdb_get_double(ByteBuffer value);

    static native int duckdb_get_date(ByteBuffer value);

    static native long duckdb_get_time(ByteBuffer value);

    static native long duckdb_get_time_ns(ByteBuffer value);

    static native long duckdb_get_time_tz(ByteBuffer value);

    static native long duckdb_get_timestamp(ByteBuffer value);

    static native long duckdb_get_timestamp_tz(ByteBuffer value);

    static native long duckdb_get_timestamp_s(ByteBuffer value);

    static native long duckdb_get_timestamp_ms(ByteBuffer value);

    static native long duckdb_get_timestamp_ns(ByteBuffer value);

    static native byte[] duckdb_get_varchar(ByteBuffer value);

    // result

    static native void duckdb_destroy_result(ByteBuffer result);

    static native ByteBuffer duckdb_fetch_chunk(ByteBuffer result);

    static native byte[] duckdb_column_name(ByteBuffer result, long col);

    static native int duckdb_column_type(ByteBuffer result, long col);

    static native long duckdb_column_count(ByteBuffer result);

    static native byte[] duckdb_result_error(ByteBuffer result);

    // C API v2

    // transitional: the C API v2 connection of a connection reference
    static native ByteBuffer duckdb_v2_connection_from_ref(ByteBuffer conn_ref) throws SQLException;

    static native void duckdb_v2_connection_interrupt(ByteBuffer conn) throws SQLException;

    static native QueryProgress duckdb_v2_connection_progress_get(ByteBuffer conn) throws SQLException;

    static native ByteBuffer duckdb_v2_parse_sql(ByteBuffer conn, byte[] sql) throws SQLException;

    // returns null when there are no more statements
    static native ByteBuffer duckdb_v2_statement_iterator_next(ByteBuffer iterator) throws SQLException;

    static native void duckdb_v2_statement_iterator_destroy(ByteBuffer iterator) throws SQLException;

    static native void duckdb_v2_sql_statement_destroy(ByteBuffer statement) throws SQLException;

    static native int duckdb_v2_sql_statement_get_type(ByteBuffer statement) throws SQLException;

    static native long duckdb_v2_sql_statement_get_parameter_count(ByteBuffer statement) throws SQLException;

    static native ByteBuffer duckdb_v2_prepared_statement_create(ByteBuffer conn, ByteBuffer statement,
                                                                 boolean require_cacheable) throws SQLException;

    static native void duckdb_v2_prepared_statement_destroy(ByteBuffer prepared) throws SQLException;

    // the values are borrowed for the call
    static native ByteBuffer duckdb_v2_prepared_statement_execute(ByteBuffer prepared, ByteBuffer[] values)
        throws SQLException;

    static native ByteBuffer duckdb_v2_statement_execute(ByteBuffer conn, ByteBuffer statement, ByteBuffer[] values)
        throws SQLException;

    // returns the output schema and the parameters schema
    static native ByteBuffer[] duckdb_v2_statement_bind(ByteBuffer conn, ByteBuffer statement) throws SQLException;

    static native void duckdb_v2_schema_destroy(ByteBuffer schema) throws SQLException;

    static native long duckdb_v2_schema_get_count(ByteBuffer schema) throws SQLException;

    static native byte[] duckdb_v2_schema_get_field_name(ByteBuffer schema, long index) throws SQLException;

    // the returned type is borrowed from the schema and must not be destroyed
    static native ByteBuffer duckdb_v2_schema_get_field_type(ByteBuffer schema, long index) throws SQLException;

    static native void duckdb_v2_result_destroy(ByteBuffer result) throws SQLException;

    static native ByteBuffer duckdb_v2_result_get_schema(ByteBuffer result) throws SQLException;

    static native int duckdb_v2_result_get_result_type(ByteBuffer result) throws SQLException;

    static native int duckdb_v2_result_get_statement_type(ByteBuffer result) throws SQLException;

    static native void duckdb_v2_result_wait(ByteBuffer result) throws SQLException;

    static native long duckdb_v2_result_drain(ByteBuffer result) throws SQLException;

    // the step status is written to status_out[0], the chunk is null unless the status is RESULT_STEP_CHUNK
    static native ByteBuffer duckdb_v2_result_step(ByteBuffer result, int[] status_out) throws SQLException;

    // returns null at the end of the result
    static native ByteBuffer duckdb_v2_result_fetch_chunk(ByteBuffer result) throws SQLException;

    // factory and values

    // the returned factory is borrowed from the connection
    static native ByteBuffer duckdb_v2_connection_get_factory(ByteBuffer conn) throws SQLException;

    static native ByteBuffer duckdb_v2_factory_create_type_from_text(ByteBuffer factory, byte[] text)
        throws SQLException;

    static native void duckdb_v2_value_destroy(ByteBuffer value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_null(ByteBuffer factory, ByteBuffer type) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_bool(ByteBuffer factory, boolean value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_tinyint(ByteBuffer factory, byte value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_smallint(ByteBuffer factory, short value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_int(ByteBuffer factory, int value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_bigint(ByteBuffer factory, long value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_float(ByteBuffer factory, float value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_double(ByteBuffer factory, double value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_date(ByteBuffer factory, int days) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_time(ByteBuffer factory, long micros) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_timestamp(ByteBuffer factory, long micros) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_timestamp_ns(ByteBuffer factory, long nanos) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_timestamp_tz(ByteBuffer factory, long micros) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_hugeint(ByteBuffer factory, long lower, long upper)
        throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_uuid(ByteBuffer factory, long lower, long upper)
        throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_decimal(ByteBuffer factory, long lower, long upper, int width,
                                                            int scale) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_varchar(ByteBuffer factory, byte[] value) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_blob(ByteBuffer factory, byte[] value) throws SQLException;

    // the child values are borrowed for the call
    static native ByteBuffer duckdb_v2_value_create_list(ByteBuffer factory, ByteBuffer childType,
                                                         ByteBuffer[] children) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_struct(ByteBuffer factory, byte[][] names, ByteBuffer[] children)
        throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_map(ByteBuffer factory, ByteBuffer keyType, ByteBuffer valueType,
                                                        ByteBuffer[] keys, ByteBuffer[] values) throws SQLException;

    static native ByteBuffer duckdb_v2_value_create_type(ByteBuffer factory, ByteBuffer type) throws SQLException;

    static native byte[] duckdb_v2_value_to_string(ByteBuffer value) throws SQLException;

    // vectors

    // forces the vector (not its children) into the flat representation, in place
    static native void duckdb_v2_vector_flatten(ByteBuffer vector) throws SQLException;

    // the returned value is owned by the caller
    static native ByteBuffer duckdb_v2_vector_get_value(ByteBuffer vector, long row) throws SQLException;

    // logical type parameters: the child types of nested types come back as TYPE values

    static native long duckdb_v2_logical_type_get_param_count(ByteBuffer type) throws SQLException;

    // the returned value is owned by the caller
    static native ByteBuffer duckdb_v2_logical_type_get_param_value(ByteBuffer type, long index) throws SQLException;

    // returns null for a positional parameter
    static native byte[] duckdb_v2_logical_type_get_param_name(ByteBuffer type, long index) throws SQLException;

    // unwraps the type of a TYPE value, the returned type is owned by the caller
    static native ByteBuffer duckdb_v2_value_get_type(ByteBuffer value) throws SQLException;

    // configuration

    // the returned config is borrowed from the connection
    static native ByteBuffer duckdb_v2_connection_get_config(ByteBuffer conn) throws SQLException;

    // the returned value is owned by the caller
    static native ByteBuffer duckdb_v2_config_get_option_value(ByteBuffer config, byte[] name) throws SQLException;

    // transitional: wraps the result into a chunked result of the bindings API that takes the ownership of the result
    static native ByteBuffer duckdb_v2_chunked_result_create(ByteBuffer result) throws SQLException;

    // variants

    // the returned value is owned by the caller
    static native ByteBuffer duckdb_v2_value_variant_to_value(ByteBuffer value) throws SQLException;

    // the returned type is owned by the caller
    static native ByteBuffer duckdb_v2_value_get_logical_type(ByteBuffer value) throws SQLException;

    static native void duckdb_v2_vector_set_value(ByteBuffer vector, long row, ByteBuffer value) throws SQLException;

    static native void duckdb_v2_vector_set_size(ByteBuffer vector, long size) throws SQLException;

    // DUCKDB_V2_RESULT_STEP_STATUS
    static final int RESULT_STEP_WAITING = 0;
    static final int RESULT_STEP_CHUNK = 1;
    static final int RESULT_STEP_FINISHED = 2;
    static final int RESULT_STEP_CANCELLED = 3;

    // DUCKDB_V2_RESULT_TYPE
    static final int RESULT_TYPE_QUERY_RESULT = 0;
    static final int RESULT_TYPE_CHANGED_ROWS = 1;
    static final int RESULT_TYPE_NOTHING = 2;

    enum CAPIType {
        DUCKDB_TYPE_INVALID(0, 0),
        // bool
        DUCKDB_TYPE_BOOLEAN(1, 1),
        // int8_t
        DUCKDB_TYPE_TINYINT(2, 1),
        // int16_t
        DUCKDB_TYPE_SMALLINT(3, 2),
        // int32_t
        DUCKDB_TYPE_INTEGER(4, 4),
        // int64_t
        DUCKDB_TYPE_BIGINT(5, 8),
        // uint8_t
        DUCKDB_TYPE_UTINYINT(6, 1),
        // uint16_t
        DUCKDB_TYPE_USMALLINT(7, 2),
        // uint32_t
        DUCKDB_TYPE_UINTEGER(8, 4),
        // uint64_t
        DUCKDB_TYPE_UBIGINT(9, 8),
        // float
        DUCKDB_TYPE_FLOAT(10, 4),
        // double
        DUCKDB_TYPE_DOUBLE(11, 8),
        // duckdb_timestamp (microseconds)
        DUCKDB_TYPE_TIMESTAMP(12, 8),
        // duckdb_date
        DUCKDB_TYPE_DATE(13, 4),
        // duckdb_time
        DUCKDB_TYPE_TIME(14, 8),
        // duckdb_interval
        DUCKDB_TYPE_INTERVAL(15),
        // duckdb_hugeint
        DUCKDB_TYPE_HUGEINT(16, 16),
        // duckdb_uhugeint
        DUCKDB_TYPE_UHUGEINT(32, 16),
        // const char*
        DUCKDB_TYPE_VARCHAR(17, 16),
        // duckdb_blob
        DUCKDB_TYPE_BLOB(18, 16),
        // duckdb_decimal
        DUCKDB_TYPE_DECIMAL(19, 0),
        // duckdb_timestamp_s (seconds)
        DUCKDB_TYPE_TIMESTAMP_S(20, 8),
        // duckdb_timestamp_ms (milliseconds)
        DUCKDB_TYPE_TIMESTAMP_MS(21, 8),
        // duckdb_timestamp_ns (nanoseconds)
        DUCKDB_TYPE_TIMESTAMP_NS(22, 8),
        // enum type, only useful as logical type
        DUCKDB_TYPE_ENUM(23, 0),
        // list type, only useful as logical type
        DUCKDB_TYPE_LIST(24, 16),
        // struct type, only useful as logical type
        DUCKDB_TYPE_STRUCT(25, 0),
        // map type, only useful as logical type
        DUCKDB_TYPE_MAP(26, 16),
        // duckdb_array, only useful as logical type
        DUCKDB_TYPE_ARRAY(33, 0),
        // duckdb_hugeint
        DUCKDB_TYPE_UUID(27, 16),
        // union type, only useful as logical type
        DUCKDB_TYPE_UNION(28),
        // duckdb_bit
        DUCKDB_TYPE_BIT(29),
        // duckdb_time_tz
        DUCKDB_TYPE_TIME_TZ(30, 8),
        // duckdb_timestamp (microseconds)
        DUCKDB_TYPE_TIMESTAMP_TZ(31, 8),
        // enum type, only useful as logical type
        DUCKDB_TYPE_ANY(34),
        // duckdb_varint
        DUCKDB_TYPE_VARINT(35),
        // enum type, only useful as logical type
        DUCKDB_TYPE_SQLNULL(36),
        // enum type, only useful as logical type
        DUCKDB_TYPE_STRING_LITERAL(37),
        // enum type, only useful as logical type
        DUCKDB_TYPE_INTEGER_LITERAL(38),
        // duckdb_time_ns (nanoseconds)
        // DUCKDB_TYPE_TIME_NS = 39,
        // WKB blob
        DUCKDB_TYPE_GEOMETRY(40, 16);

        final int typeId;
        final long widthBytes;
        final CAPIType[] typeArray;

        private static final CAPIType[] BY_TYPE_ID;

        static {
            int max = 0;
            CAPIType[] values = CAPIType.values();
            for (CAPIType ct : values) {
                if (ct.typeId > max) {
                    max = ct.typeId;
                }
            }
            BY_TYPE_ID = new CAPIType[max + 1];
            for (CAPIType ct : values) {
                BY_TYPE_ID[ct.typeId] = ct;
            }
        }

        CAPIType(int typeId) {
            this(typeId, 0);
        }

        CAPIType(int typeId, long widthBytes) {
            this.typeId = typeId;
            this.widthBytes = widthBytes;
            this.typeArray = new CAPIType[] {this};
        }

        static CAPIType capiTypeFromTypeId(int typeId) throws SQLException {
            if (typeId >= 0 && typeId < BY_TYPE_ID.length && BY_TYPE_ID[typeId] != null) {
                return BY_TYPE_ID[typeId];
            }
            throw createSQLException("Invalid unknown ID not found: " + typeId, ErrorCode.BINDINGS_UNKNOWN_ID);
        }
    }
}

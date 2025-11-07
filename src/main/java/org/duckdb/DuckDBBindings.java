package org.duckdb;

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

    // logical type

    static native ByteBuffer duckdb_create_logical_type(int duckdb_type);

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

    static native ByteBuffer duckdb_vector_get_validity(ByteBuffer vector, long vector_size_elems);

    static native void duckdb_vector_ensure_validity_writable(ByteBuffer vector);

    static native void duckdb_vector_assign_string_element_len(ByteBuffer vector, long index, byte[] str);

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
        DUCKDB_TYPE_INTEGER_LITERAL(38);

        final int typeId;
        final long widthBytes;
        final CAPIType[] typeArray;

        CAPIType(int typeId) {
            this(typeId, 0);
        }

        CAPIType(int typeId, long widthBytes) {
            this.typeId = typeId;
            this.widthBytes = widthBytes;
            this.typeArray = new CAPIType[] {this};
        }

        static CAPIType capiTypeFromTypeId(int typeId) throws SQLException {
            for (CAPIType ct : CAPIType.values()) {
                if (ct.typeId == typeId) {
                    return ct;
                }
            }
            throw new SQLException("Invalid unknown ID not found: " + typeId);
        }
    }
}

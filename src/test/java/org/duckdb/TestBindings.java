package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

public class TestBindings {

    static final int STRING_T_SIZE_BYTES = 16;

    public static void test_bindings_vector_size() throws Exception {
        long size = duckdb_vector_size();
        assertTrue(size > 0);
    }

    public static void test_bindings_logical_type() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER.typeId);
        assertNotNull(lt);
        assertEquals(DUCKDB_TYPE_INTEGER.typeId, duckdb_get_type_id(lt));

        ByteBuffer listType = duckdb_create_list_type(lt);
        assertTrue(duckdb_get_type_id(listType) != DUCKDB_TYPE_INVALID.typeId);
        duckdb_destroy_logical_type(listType);

        ByteBuffer arrayType = duckdb_create_array_type(lt, 42);
        assertTrue(duckdb_get_type_id(arrayType) != DUCKDB_TYPE_INVALID.typeId);
        duckdb_destroy_logical_type(arrayType);

        duckdb_destroy_logical_type(lt);

        assertEquals(duckdb_get_type_id(duckdb_create_logical_type(-1)), DUCKDB_TYPE_INVALID.typeId);
        assertEquals(duckdb_get_type_id(duckdb_create_logical_type(DUCKDB_TYPE_INVALID.typeId)),
                     DUCKDB_TYPE_INVALID.typeId);

        assertThrows(() -> { duckdb_destroy_logical_type(null); }, SQLException.class);
    }

    public static void test_bindings_vector_create() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER.typeId);
        ByteBuffer vec = duckdb_create_vector(lt);
        assertNotNull(vec);

        ByteBuffer data = duckdb_vector_get_data(vec, duckdb_vector_size() * 4);
        assertNotNull(data);
        assertEquals(data.capacity(), (int) duckdb_vector_size() * 4);

        ByteBuffer vecLt = duckdb_vector_get_column_type(vec);
        assertNotNull(vecLt);
        assertEquals(duckdb_get_type_id(vecLt), DUCKDB_TYPE_INTEGER.typeId);
        duckdb_destroy_logical_type(vecLt);

        assertThrows(() -> { duckdb_create_vector(null); }, SQLException.class);
        assertThrows(() -> { duckdb_vector_get_data(null, 0); }, SQLException.class);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(lt);
    }

    private static void checkVectorInsertString(ByteBuffer vec) throws Exception {
        String str = "foo";
        int idx = 7;

        byte[] bytes = str.getBytes(UTF_8);
        duckdb_vector_assign_string_element_len(vec, idx, bytes);
        ByteBuffer data = duckdb_vector_get_data(vec, duckdb_vector_size() * 16);
        assertEquals(data.remaining(), (int) duckdb_vector_size() * 16);

        int lengthPos = STRING_T_SIZE_BYTES * idx;
        int length = data.getInt(lengthPos);
        assertEquals(length, str.length());

        int dataPos = lengthPos + 4;
        byte[] buf = new byte[length];
        data.position(dataPos);
        data.get(buf);
        assertEquals(new String(buf, UTF_8), str);
    }

    public static void test_bindings_vector_strings() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer vec = duckdb_create_vector(lt);

        checkVectorInsertString(vec);

        String str = "bar";
        int idx = 9;
        duckdb_vector_assign_string_element_len(vec, idx, str.getBytes(UTF_8));
        ByteBuffer data = duckdb_vector_get_data(vec, duckdb_vector_size() * 16);

        int lengthPos = STRING_T_SIZE_BYTES * idx;
        int length = data.getInt(lengthPos);
        assertEquals(length, str.length());

        int dataPos = lengthPos + 4;
        byte[] buf = new byte[length];
        data.position(dataPos);
        data.get(buf);
        assertEquals(new String(buf, UTF_8), str);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(lt);
    }

    public static void test_bindings_vector_validity() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer vec = duckdb_create_vector(lt);

        ByteBuffer emptyValidity = duckdb_vector_get_validity(vec, duckdb_vector_size());
        assertNull(emptyValidity);

        duckdb_vector_ensure_validity_writable(vec);
        ByteBuffer validity = duckdb_vector_get_validity(vec, duckdb_vector_size());
        assertNotNull(validity);
        assertEquals(validity.capacity(), (int) duckdb_vector_size() / 8);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(lt);
    }

    public static void test_bindings_list_vector() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer listType = duckdb_create_list_type(lt);
        assertTrue(duckdb_get_type_id(listType) != DUCKDB_TYPE_INVALID.typeId);
        ByteBuffer vec = duckdb_create_vector(listType);

        ByteBuffer childVec = duckdb_list_vector_get_child(vec);
        assertNotNull(childVec);
        ByteBuffer data = duckdb_vector_get_data(childVec, duckdb_vector_size() * 16);
        assertNotNull(data);
        assertEquals(data.capacity(), (int) duckdb_vector_size() * 16);
        checkVectorInsertString(childVec);

        assertEquals(duckdb_list_vector_get_size(vec), 0L);
        assertEquals(duckdb_list_vector_reserve(vec, 42), 0);
        assertEquals(duckdb_list_vector_set_size(vec, 42), 0);
        assertEquals(duckdb_list_vector_get_size(vec), 42L);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(listType);
        duckdb_destroy_logical_type(lt);
    }

    public static void test_bindings_array_vector() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer arrayType = duckdb_create_array_type(lt, 42);
        assertTrue(duckdb_get_type_id(arrayType) != DUCKDB_TYPE_INVALID.typeId);
        assertEquals(duckdb_array_type_array_size(arrayType), 42L);
        ByteBuffer vec = duckdb_create_vector(arrayType);

        ByteBuffer childVec = duckdb_array_vector_get_child(vec);
        assertNotNull(childVec);
        ByteBuffer data = duckdb_vector_get_data(childVec, duckdb_vector_size() * 16);
        assertNotNull(data);
        assertEquals(data.capacity(), (int) duckdb_vector_size() * 16);
        checkVectorInsertString(childVec);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(arrayType);
        duckdb_destroy_logical_type(lt);
    }

    public static void test_bindings_struct_vector() throws Exception {
        ByteBuffer intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER.typeId);
        ByteBuffer varcharType = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer structType = duckdb_create_struct_type(new ByteBuffer[] {intType, varcharType},
                                                          new byte[][] {"foo".getBytes(UTF_8), "bar".getBytes(UTF_8)});
        assertTrue(duckdb_get_type_id(structType) != DUCKDB_TYPE_INVALID.typeId);
        assertEquals(duckdb_struct_type_child_count(structType), 2L);

        assertEquals(duckdb_struct_type_child_name(structType, 0), "foo".getBytes(UTF_8));
        assertEquals(duckdb_struct_type_child_name(structType, 1), "bar".getBytes(UTF_8));
        assertThrows(() -> { duckdb_struct_type_child_name(structType, -1); }, SQLException.class);
        assertThrows(() -> { duckdb_struct_type_child_name(structType, 2); }, SQLException.class);

        ByteBuffer vec = duckdb_create_vector(structType);

        ByteBuffer childVec = duckdb_struct_vector_get_child(vec, 1);
        assertNotNull(childVec);
        ByteBuffer data = duckdb_vector_get_data(childVec, duckdb_vector_size() * 16);
        assertNotNull(data);
        assertEquals(data.capacity(), (int) duckdb_vector_size() * 16);
        checkVectorInsertString(childVec);

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(structType);
        duckdb_destroy_logical_type(varcharType);
        duckdb_destroy_logical_type(intType);
    }

    public static void test_bindings_validity() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer vec = duckdb_create_vector(lt);
        duckdb_vector_ensure_validity_writable(vec);
        ByteBuffer validity = duckdb_vector_get_validity(vec, duckdb_vector_size());

        long row = 7;
        assertTrue(duckdb_validity_row_is_valid(validity, row));
        duckdb_validity_set_row_validity(validity, row, false);
        assertFalse(duckdb_validity_row_is_valid(validity, row));
        duckdb_validity_set_row_validity(validity, row, true);
        assertTrue(duckdb_validity_row_is_valid(validity, row));

        duckdb_destroy_vector(vec);
        duckdb_destroy_logical_type(lt);
    }

    public static void test_bindings_data_chunk() throws Exception {
        ByteBuffer intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER.typeId);
        ByteBuffer varcharType = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);

        ByteBuffer chunk = duckdb_create_data_chunk(new ByteBuffer[] {intType, varcharType});
        assertNotNull(chunk);

        assertEquals(duckdb_data_chunk_get_column_count(chunk), 2L);

        assertEquals(duckdb_data_chunk_get_size(chunk), 0L);
        duckdb_data_chunk_set_size(chunk, 42L);
        assertEquals(duckdb_data_chunk_get_size(chunk), 42L);

        ByteBuffer vec = duckdb_data_chunk_get_vector(chunk, 1);
        assertNotNull(vec);
        ByteBuffer data = duckdb_vector_get_data(vec, duckdb_vector_size() * 16);
        assertNotNull(data);
        assertEquals(data.capacity(), (int) duckdb_vector_size() * 16);
        checkVectorInsertString(vec);

        duckdb_vector_ensure_validity_writable(vec);
        assertNotNull(duckdb_vector_get_validity(vec, duckdb_vector_size()));
        duckdb_data_chunk_reset(chunk);
        assertNull(duckdb_vector_get_validity(vec, duckdb_vector_size()));

        duckdb_destroy_data_chunk(chunk);
        duckdb_destroy_logical_type(varcharType);
        duckdb_destroy_logical_type(intType);
    }

    public static void test_bindings_appender() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR)");

            ByteBuffer[] out = new ByteBuffer[1];
            int state =
                duckdb_appender_create_ext(conn.connRef, "memory".getBytes(UTF_8), null, "tab1".getBytes(UTF_8), out);

            assertEquals(state, 0);
            ByteBuffer appender = out[0];
            assertNotNull(appender);

            assertEquals(duckdb_appender_column_count(appender), 2L);
            ByteBuffer col1Type = duckdb_appender_column_type(appender, 0);
            assertEquals(duckdb_get_type_id(col1Type), DUCKDB_TYPE_INTEGER.typeId);
            ByteBuffer col2Type = duckdb_appender_column_type(appender, 1);
            assertEquals(duckdb_get_type_id(col2Type), DUCKDB_TYPE_VARCHAR.typeId);

            ByteBuffer chunk = duckdb_create_data_chunk(new ByteBuffer[] {col1Type, col2Type});

            ByteBuffer col1Vec = duckdb_data_chunk_get_vector(chunk, 0);
            duckdb_vector_ensure_validity_writable(col1Vec);
            ByteBuffer col1Data = duckdb_vector_get_data(col1Vec, duckdb_vector_size() * 4);
            col1Data.putInt(42);
            col1Data.putInt(43);
            duckdb_append_default_to_chunk(appender, chunk, 0, 2);

            ByteBuffer col2Vec = duckdb_data_chunk_get_vector(chunk, 1);
            duckdb_vector_ensure_validity_writable(col2Vec);
            duckdb_vector_assign_string_element_len(col2Vec, 0, "foo".getBytes(UTF_8));
            duckdb_vector_assign_string_element_len(col2Vec, 1, "bar".getBytes(UTF_8));
            duckdb_append_default_to_chunk(appender, chunk, 1, 2);

            duckdb_data_chunk_set_size(chunk, 3);
            assertEquals(duckdb_append_data_chunk(appender, chunk), 0);
            assertEquals(duckdb_appender_flush(appender), 0);
            assertNull(duckdb_appender_error(appender));

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(2), "foo");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 43);
                assertEquals(rs.getString(2), "bar");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 0);
                assertEquals(rs.getString(2), null);
                assertFalse(rs.next());
            }

            assertEquals(duckdb_appender_close(appender), 0);
            assertEquals(duckdb_appender_destroy(appender), 0);
            duckdb_destroy_data_chunk(chunk);
            duckdb_destroy_logical_type(col1Type);
            duckdb_destroy_logical_type(col2Type);
        }
    }

    public static void test_bindings_decimal_type() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE tab1(dec2 DECIMAL(3,1), dec4 DECIMAL(7,3), dec8 DECIMAL(15,5), dec16 DECIMAL(30,10))");

            ByteBuffer[] out = new ByteBuffer[1];
            int state =
                duckdb_appender_create_ext(conn.connRef, "memory".getBytes(UTF_8), null, "tab1".getBytes(UTF_8), out);
            assertEquals(state, 0);
            ByteBuffer appender = out[0];
            assertNotNull(appender);

            ByteBuffer dec2Type = duckdb_appender_column_type(appender, 0);
            assertEquals(duckdb_decimal_width(dec2Type), 3);
            assertEquals(duckdb_decimal_scale(dec2Type), 1);
            assertEquals(duckdb_decimal_internal_type(dec2Type), DUCKDB_TYPE_SMALLINT.typeId);

            ByteBuffer dec4Type = duckdb_appender_column_type(appender, 1);
            assertEquals(duckdb_decimal_width(dec4Type), 7);
            assertEquals(duckdb_decimal_scale(dec4Type), 3);
            assertEquals(duckdb_decimal_internal_type(dec4Type), DUCKDB_TYPE_INTEGER.typeId);

            ByteBuffer dec8Type = duckdb_appender_column_type(appender, 2);
            assertEquals(duckdb_decimal_width(dec8Type), 15);
            assertEquals(duckdb_decimal_scale(dec8Type), 5);
            assertEquals(duckdb_decimal_internal_type(dec8Type), DUCKDB_TYPE_BIGINT.typeId);

            ByteBuffer dec16Type = duckdb_appender_column_type(appender, 3);
            assertEquals(duckdb_decimal_width(dec16Type), 30);
            assertEquals(duckdb_decimal_scale(dec16Type), 10);
            assertEquals(duckdb_decimal_internal_type(dec16Type), DUCKDB_TYPE_HUGEINT.typeId);

            duckdb_destroy_logical_type(dec16Type);
            duckdb_destroy_logical_type(dec8Type);
            duckdb_destroy_logical_type(dec4Type);
            duckdb_destroy_logical_type(dec2Type);
            assertEquals(duckdb_appender_close(appender), 0);
            assertEquals(duckdb_appender_destroy(appender), 0);
        }
    }

    public static void test_bindings_enum_type() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
            stmt.execute("CREATE TABLE tab1(col1 mood)");

            ByteBuffer[] out = new ByteBuffer[1];
            int state =
                duckdb_appender_create_ext(conn.connRef, "memory".getBytes(UTF_8), null, "tab1".getBytes(UTF_8), out);
            assertEquals(state, 0);
            ByteBuffer appender = out[0];
            assertNotNull(appender);

            ByteBuffer enumType = duckdb_appender_column_type(appender, 0);
            assertEquals(duckdb_enum_internal_type(enumType), DUCKDB_TYPE_UTINYINT.typeId);
            assertEquals(duckdb_enum_dictionary_size(enumType), 3L);
            assertEquals(duckdb_enum_dictionary_value(enumType, 0), "sad".getBytes(UTF_8));
            assertEquals(duckdb_enum_dictionary_value(enumType, 1), "ok".getBytes(UTF_8));
            assertEquals(duckdb_enum_dictionary_value(enumType, 2), "happy".getBytes(UTF_8));

            assertThrows(() -> { duckdb_enum_dictionary_value(enumType, 3); }, SQLException.class);
            assertThrows(() -> { duckdb_enum_dictionary_value(enumType, -1); }, SQLException.class);
        }
    }
}

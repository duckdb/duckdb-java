package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import org.duckdb.udf.UdfLogicalType;

public class TestBindings {

    static final int STRING_T_SIZE_BYTES = 16;

    private static void assertAccessors(DuckDBColumnType type, EnumSet<UdfTypeCatalog.Accessor> expected)
        throws Exception {
        for (UdfTypeCatalog.Accessor accessor : UdfTypeCatalog.Accessor.values()) {
            assertEquals(UdfTypeCatalog.supportsAccessor(type, accessor), expected.contains(accessor));
        }
        assertEquals(UdfTypeCatalog.accessorMatrixView().get(type), expected);
    }

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
        ByteBuffer[] childTypes = new ByteBuffer[] {intType, varcharType};
        byte[][] childNames = new byte[][] {"foo".getBytes(UTF_8), "bar".getBytes(UTF_8)};
        ByteBuffer structType = duckdb_create_struct_type(childTypes, childNames);
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

    public static void test_bindings_udf_type_catalog_mappings() throws Exception {
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.INTEGER));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.VARCHAR));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.BOOLEAN));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TINYINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.SMALLINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.BIGINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.UTINYINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.USMALLINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.UINTEGER));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.UBIGINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.HUGEINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.UHUGEINT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.FLOAT));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.DOUBLE));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.DECIMAL));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.BLOB));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.DATE));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIME));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIME_NS));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIMESTAMP));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIMESTAMP_S));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIMESTAMP_MS));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIMESTAMP_NS));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIME_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isScalarUdfImplemented(DuckDBColumnType.UUID));

        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.BOOLEAN));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TINYINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.SMALLINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.INTEGER));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.BIGINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.UTINYINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.USMALLINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.UINTEGER));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.UBIGINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.HUGEINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.UHUGEINT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.FLOAT));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.DOUBLE));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.VARCHAR));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.DECIMAL));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.BLOB));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.DATE));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIME));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIME_NS));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIMESTAMP));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIMESTAMP_S));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIMESTAMP_MS));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIMESTAMP_NS));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIME_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isTableBindSchemaType(DuckDBColumnType.UUID));

        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.BOOLEAN));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TINYINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.SMALLINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.INTEGER));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.BIGINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.UTINYINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.USMALLINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.UINTEGER));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.UBIGINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.HUGEINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.UHUGEINT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.FLOAT));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.DOUBLE));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.VARCHAR));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.DECIMAL));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.BLOB));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.DATE));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIME));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIME_NS));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIMESTAMP));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIMESTAMP_S));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIMESTAMP_MS));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIMESTAMP_NS));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIME_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(UdfTypeCatalog.isTableFunctionParameterType(DuckDBColumnType.UUID));

        assertTrue(UdfTypeCatalog.isVarLenType(DuckDBColumnType.VARCHAR));
        assertTrue(UdfTypeCatalog.isVarLenType(DuckDBColumnType.BLOB));
        assertFalse(UdfTypeCatalog.isVarLenType(DuckDBColumnType.INTEGER));
        assertTrue(UdfTypeCatalog.requiresVectorRef(DuckDBColumnType.VARCHAR));
        assertTrue(UdfTypeCatalog.requiresVectorRef(DuckDBColumnType.BLOB));
        assertTrue(UdfTypeCatalog.requiresVectorRef(DuckDBColumnType.DECIMAL));
        assertFalse(UdfTypeCatalog.requiresVectorRef(DuckDBColumnType.TIMESTAMP));

        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.INTEGER, UdfTypeCatalog.Accessor.GET_INT));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.INTEGER, UdfTypeCatalog.Accessor.SET_INT));
        assertFalse(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.INTEGER, UdfTypeCatalog.Accessor.GET_LONG));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.VARCHAR, UdfTypeCatalog.Accessor.GET_STRING));
        assertFalse(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.VARCHAR, UdfTypeCatalog.Accessor.SET_DOUBLE));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.BLOB, UdfTypeCatalog.Accessor.GET_BYTES));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.DECIMAL, UdfTypeCatalog.Accessor.GET_DECIMAL));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.DATE, UdfTypeCatalog.Accessor.GET_INT));
        assertTrue(UdfTypeCatalog.supportsAccessor(DuckDBColumnType.TIMESTAMP, UdfTypeCatalog.Accessor.GET_LONG));

        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.BOOLEAN), DUCKDB_TYPE_BOOLEAN.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TINYINT), DUCKDB_TYPE_TINYINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.SMALLINT), DUCKDB_TYPE_SMALLINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.INTEGER), DUCKDB_TYPE_INTEGER.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.BIGINT), DUCKDB_TYPE_BIGINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.UTINYINT), DUCKDB_TYPE_UTINYINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.USMALLINT), DUCKDB_TYPE_USMALLINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.UINTEGER), DUCKDB_TYPE_UINTEGER.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.UBIGINT), DUCKDB_TYPE_UBIGINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.HUGEINT), DUCKDB_TYPE_HUGEINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.UHUGEINT), DUCKDB_TYPE_UHUGEINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.FLOAT), DUCKDB_TYPE_FLOAT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.DOUBLE), DUCKDB_TYPE_DOUBLE.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.VARCHAR), DUCKDB_TYPE_VARCHAR.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.BLOB), DUCKDB_TYPE_BLOB.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.DECIMAL), DUCKDB_TYPE_DECIMAL.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.DATE), DUCKDB_TYPE_DATE.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIME), DUCKDB_TYPE_TIME.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIME_NS), DUCKDB_TYPE_TIME_NS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIMESTAMP), DUCKDB_TYPE_TIMESTAMP.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIMESTAMP_S), DUCKDB_TYPE_TIMESTAMP_S.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIMESTAMP_MS), DUCKDB_TYPE_TIMESTAMP_MS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIMESTAMP_NS), DUCKDB_TYPE_TIMESTAMP_NS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIME_WITH_TIME_ZONE), DUCKDB_TYPE_TIME_TZ.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE),
                     DUCKDB_TYPE_TIMESTAMP_TZ.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeId(DuckDBColumnType.UUID), DUCKDB_TYPE_UUID.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForTableFunctionParameter(DuckDBColumnType.INTEGER),
                     DUCKDB_TYPE_INTEGER.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForTableFunctionParameter(DuckDBColumnType.DOUBLE),
                     DUCKDB_TYPE_DOUBLE.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForTableFunctionParameter(DuckDBColumnType.VARCHAR),
                     DUCKDB_TYPE_VARCHAR.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForTableFunctionParameter(DuckDBColumnType.DECIMAL),
                     DUCKDB_TYPE_DECIMAL.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForTableFunctionParameter(DuckDBColumnType.TIMESTAMP_NS),
                     DUCKDB_TYPE_TIMESTAMP_NS.typeId);
        assertEquals(UdfTypeCatalog.fromCapiTypeId(DUCKDB_TYPE_INTEGER.typeId), DuckDBColumnType.INTEGER);
        assertEquals(UdfTypeCatalog.fromCapiTypeId(DUCKDB_TYPE_VARCHAR.typeId), DuckDBColumnType.VARCHAR);
        assertEquals(UdfTypeCatalog.fromCapiTypeId(DUCKDB_TYPE_BLOB.typeId), DuckDBColumnType.BLOB);
        assertEquals(UdfTypeCatalog.fromCapiTypeId(DUCKDB_TYPE_TIME_NS.typeId), DuckDBColumnType.TIME_NS);
        assertEquals(UdfTypeCatalog.fromCapiTypeId(DUCKDB_TYPE_TIME_TZ.typeId), DuckDBColumnType.TIME_WITH_TIME_ZONE);

        assertThrows(() -> { UdfTypeCatalog.fromCapiTypeId(-1); }, SQLFeatureNotSupportedException.class);
    }

    public static void test_bindings_udf_java_type_mapper_biginteger_maps_to_hugeint() throws Exception {
        UdfLogicalType mapped = UdfJavaTypeMapper.toLogicalType(BigInteger.class);
        assertEquals(mapped.getType(), DuckDBColumnType.HUGEINT);
    }

    public static void test_bindings_udf_type_catalog_accessor_matrix_all_supported_types() throws Exception {
        assertAccessors(DuckDBColumnType.BOOLEAN,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_BOOLEAN, UdfTypeCatalog.Accessor.SET_BOOLEAN));
        assertAccessors(DuckDBColumnType.TINYINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.SMALLINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.INTEGER,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.BIGINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.UTINYINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.USMALLINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.UINTEGER,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.UBIGINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.HUGEINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_BYTES, UdfTypeCatalog.Accessor.SET_BYTES));
        assertAccessors(DuckDBColumnType.UHUGEINT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_BYTES, UdfTypeCatalog.Accessor.SET_BYTES));
        assertAccessors(DuckDBColumnType.FLOAT,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_FLOAT, UdfTypeCatalog.Accessor.SET_FLOAT));
        assertAccessors(DuckDBColumnType.DOUBLE,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_DOUBLE, UdfTypeCatalog.Accessor.SET_DOUBLE));
        assertAccessors(DuckDBColumnType.VARCHAR,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_STRING, UdfTypeCatalog.Accessor.SET_STRING));
        assertAccessors(DuckDBColumnType.BLOB,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_BYTES, UdfTypeCatalog.Accessor.SET_BYTES));
        assertAccessors(DuckDBColumnType.DECIMAL,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_DECIMAL, UdfTypeCatalog.Accessor.SET_DECIMAL));
        assertAccessors(DuckDBColumnType.DATE,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_INT, UdfTypeCatalog.Accessor.SET_INT));
        assertAccessors(DuckDBColumnType.TIME,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIME_NS,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIMESTAMP,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIMESTAMP_S,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIMESTAMP_MS,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIMESTAMP_NS,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIME_WITH_TIME_ZONE,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_LONG, UdfTypeCatalog.Accessor.SET_LONG));
        assertAccessors(DuckDBColumnType.UUID,
                        EnumSet.of(UdfTypeCatalog.Accessor.GET_BYTES, UdfTypeCatalog.Accessor.SET_BYTES));
    }

    public static void test_bindings_udf_scalar_registration_type_ids() throws Exception {
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.BOOLEAN),
                     DUCKDB_TYPE_BOOLEAN.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TINYINT),
                     DUCKDB_TYPE_TINYINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.SMALLINT),
                     DUCKDB_TYPE_SMALLINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.INTEGER),
                     DUCKDB_TYPE_INTEGER.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.BIGINT),
                     DUCKDB_TYPE_BIGINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.UTINYINT),
                     DUCKDB_TYPE_UTINYINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.USMALLINT),
                     DUCKDB_TYPE_USMALLINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.UINTEGER),
                     DUCKDB_TYPE_UINTEGER.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.UBIGINT),
                     DUCKDB_TYPE_UBIGINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.HUGEINT),
                     DUCKDB_TYPE_HUGEINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.UHUGEINT),
                     DUCKDB_TYPE_UHUGEINT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.FLOAT),
                     DUCKDB_TYPE_FLOAT.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.DOUBLE),
                     DUCKDB_TYPE_DOUBLE.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.VARCHAR),
                     DUCKDB_TYPE_VARCHAR.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.DECIMAL),
                     DUCKDB_TYPE_DECIMAL.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.BLOB), DUCKDB_TYPE_BLOB.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.DATE), DUCKDB_TYPE_DATE.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIME), DUCKDB_TYPE_TIME.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIME_NS),
                     DUCKDB_TYPE_TIME_NS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIMESTAMP),
                     DUCKDB_TYPE_TIMESTAMP.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIMESTAMP_S),
                     DUCKDB_TYPE_TIMESTAMP_S.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIMESTAMP_MS),
                     DUCKDB_TYPE_TIMESTAMP_MS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIMESTAMP_NS),
                     DUCKDB_TYPE_TIMESTAMP_NS.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIME_WITH_TIME_ZONE),
                     DUCKDB_TYPE_TIME_TZ.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE),
                     DUCKDB_TYPE_TIMESTAMP_TZ.typeId);
        assertEquals(UdfTypeCatalog.toCapiTypeIdForScalarRegistration(DuckDBColumnType.UUID), DUCKDB_TYPE_UUID.typeId);
    }

    public static void test_bindings_udf_capi_type_roundtrip_supported_types() throws Exception {
        int[] supportedTypeIds = new int[] {
            DUCKDB_TYPE_BOOLEAN.typeId,      DUCKDB_TYPE_TINYINT.typeId,      DUCKDB_TYPE_SMALLINT.typeId,
            DUCKDB_TYPE_INTEGER.typeId,      DUCKDB_TYPE_BIGINT.typeId,       DUCKDB_TYPE_FLOAT.typeId,
            DUCKDB_TYPE_UTINYINT.typeId,     DUCKDB_TYPE_USMALLINT.typeId,    DUCKDB_TYPE_UINTEGER.typeId,
            DUCKDB_TYPE_UBIGINT.typeId,      DUCKDB_TYPE_HUGEINT.typeId,      DUCKDB_TYPE_UHUGEINT.typeId,
            DUCKDB_TYPE_DOUBLE.typeId,       DUCKDB_TYPE_VARCHAR.typeId,      DUCKDB_TYPE_BLOB.typeId,
            DUCKDB_TYPE_DATE.typeId,         DUCKDB_TYPE_TIME.typeId,         DUCKDB_TYPE_TIME_NS.typeId,
            DUCKDB_TYPE_TIME_TZ.typeId,      DUCKDB_TYPE_TIMESTAMP.typeId,    DUCKDB_TYPE_TIMESTAMP_S.typeId,
            DUCKDB_TYPE_TIMESTAMP_MS.typeId, DUCKDB_TYPE_TIMESTAMP_NS.typeId, DUCKDB_TYPE_TIMESTAMP_TZ.typeId,
            DUCKDB_TYPE_UUID.typeId};
        for (int typeId : supportedTypeIds) {
            ByteBuffer lt = duckdb_create_logical_type(typeId);
            assertNotNull(lt);
            assertEquals(duckdb_get_type_id(lt), typeId);
            assertEquals(UdfTypeCatalog.toCapiTypeId(UdfTypeCatalog.fromCapiTypeId(typeId)), typeId);
            duckdb_destroy_logical_type(lt);
        }
    }

    public static void test_bindings_udf_scalar_writer_fixed_width_accessors() throws Exception {
        int rowCount = 8;
        ByteBuffer validity = ByteBuffer.allocateDirect((rowCount + 7) / 8);
        validity.put(0, (byte) 0xFF);

        UdfScalarWriter boolVector = new UdfScalarWriter(DUCKDB_TYPE_BOOLEAN.typeId,
                                                         ByteBuffer.allocateDirect(rowCount), null, validity, rowCount);
        boolVector.setBoolean(0, true);
        boolVector.setBoolean(1, false);
        assertTrue(boolVector.getBoolean(0));
        assertFalse(boolVector.getBoolean(1));
        assertThrows(() -> { boolVector.getInt(0); }, UnsupportedOperationException.class);

        UdfScalarWriter tinyVector = new UdfScalarWriter(DUCKDB_TYPE_TINYINT.typeId,
                                                         ByteBuffer.allocateDirect(rowCount), null, validity, rowCount);
        tinyVector.setInt(0, 127);
        tinyVector.setInt(1, -12);
        assertEquals(tinyVector.getInt(0), 127);
        assertEquals(tinyVector.getInt(1), -12);
        assertThrows(() -> { tinyVector.setInt(2, 128); }, IllegalArgumentException.class);

        UdfScalarWriter smallVector = new UdfScalarWriter(
            DUCKDB_TYPE_SMALLINT.typeId, ByteBuffer.allocateDirect(rowCount * Short.BYTES), null, validity, rowCount);
        smallVector.setInt(0, 12345);
        smallVector.setInt(1, -12345);
        assertEquals(smallVector.getInt(0), 12345);
        assertEquals(smallVector.getInt(1), -12345);
        assertThrows(() -> { smallVector.setInt(2, 40000); }, IllegalArgumentException.class);

        UdfScalarWriter unsignedTinyVector = new UdfScalarWriter(
            DUCKDB_TYPE_UTINYINT.typeId, ByteBuffer.allocateDirect(rowCount), null, validity, rowCount);
        unsignedTinyVector.setInt(0, 255);
        assertEquals(unsignedTinyVector.getInt(0), 255);
        assertThrows(() -> { unsignedTinyVector.setInt(1, -1); }, IllegalArgumentException.class);
        assertThrows(() -> { unsignedTinyVector.setInt(1, 256); }, IllegalArgumentException.class);

        UdfScalarWriter unsignedSmallVector = new UdfScalarWriter(
            DUCKDB_TYPE_USMALLINT.typeId, ByteBuffer.allocateDirect(rowCount * Short.BYTES), null, validity, rowCount);
        unsignedSmallVector.setInt(0, 65535);
        assertEquals(unsignedSmallVector.getInt(0), 65535);
        assertThrows(() -> { unsignedSmallVector.setInt(1, -1); }, IllegalArgumentException.class);
        assertThrows(() -> { unsignedSmallVector.setInt(1, 70000); }, IllegalArgumentException.class);

        UdfScalarWriter intVector = new UdfScalarWriter(
            DUCKDB_TYPE_INTEGER.typeId, ByteBuffer.allocateDirect(rowCount * Integer.BYTES), null, validity, rowCount);
        intVector.setInt(0, 99);
        assertEquals(intVector.getInt(0), 99);
        intVector.setNull(0);
        assertTrue(intVector.isNull(0));
        intVector.setInt(0, 77);
        assertFalse(intVector.isNull(0));
        assertThrows(() -> { intVector.getLong(0); }, UnsupportedOperationException.class);

        UdfScalarWriter longVector = new UdfScalarWriter(
            DUCKDB_TYPE_BIGINT.typeId, ByteBuffer.allocateDirect(rowCount * Long.BYTES), null, validity, rowCount);
        longVector.setLong(0, 9_000_000_000L);
        assertEquals(longVector.getLong(0), 9_000_000_000L);
        longVector.setObject(1, new BigDecimal("9223372036854775807"));
        assertEquals(longVector.getLong(1), Long.MAX_VALUE);
        assertThrows(() -> { longVector.setObject(2, new BigDecimal("1.5")); }, IllegalArgumentException.class);
        assertThrows(
            () -> { longVector.setObject(3, new BigInteger("9223372036854775808")); }, IllegalArgumentException.class);
        assertThrows(() -> { longVector.setInt(0, 1); }, UnsupportedOperationException.class);

        UdfScalarWriter unsignedIntVector = new UdfScalarWriter(
            DUCKDB_TYPE_UINTEGER.typeId, ByteBuffer.allocateDirect(rowCount * Integer.BYTES), null, validity, rowCount);
        unsignedIntVector.setLong(0, 4_000_000_000L);
        assertEquals(unsignedIntVector.getLong(0), 4_000_000_000L);
        assertThrows(() -> { unsignedIntVector.setLong(1, -1); }, IllegalArgumentException.class);

        UdfScalarWriter unsignedBigVector = new UdfScalarWriter(
            DUCKDB_TYPE_UBIGINT.typeId, ByteBuffer.allocateDirect(rowCount * Long.BYTES), null, validity, rowCount);
        unsignedBigVector.setLong(0, -1L);
        assertEquals(unsignedBigVector.getLong(0), -1L);
        unsignedBigVector.setObject(1, new BigInteger("18446744073709551615"));
        assertEquals(unsignedBigVector.getLong(1), -1L);
        assertThrows(() -> { unsignedBigVector.setObject(2, new BigDecimal("1.5")); }, IllegalArgumentException.class);
        assertThrows(() -> {
            unsignedBigVector.setObject(3, new BigInteger("18446744073709551616"));
        }, IllegalArgumentException.class);

        UdfScalarWriter hugeVector = new UdfScalarWriter(
            DUCKDB_TYPE_HUGEINT.typeId, ByteBuffer.allocateDirect(rowCount * 16), null, validity, rowCount);
        byte[] hugeValue = new byte[16];
        hugeValue[0] = 42;
        hugeValue[15] = 7;
        hugeVector.setBytes(0, hugeValue);
        assertEquals(hugeVector.getBytes(0), hugeValue);
        BigInteger hugeBi = new BigInteger("170141183460469231731687303715884105727");
        hugeVector.setObject(1, hugeBi);
        assertEquals(hugeVector.getBigInteger(1), hugeBi);
        BigInteger hugeNegOne = BigInteger.valueOf(-1);
        hugeVector.setObject(2, hugeNegOne);
        assertEquals(hugeVector.getBigInteger(2), hugeNegOne);
        BigInteger hugeMin = new BigInteger("-170141183460469231731687303715884105728");
        hugeVector.setObject(3, hugeMin);
        assertEquals(hugeVector.getBigInteger(3), hugeMin);
        BigInteger hugeMinPlusOne = hugeMin.add(BigInteger.ONE);
        hugeVector.setObject(4, hugeMinPlusOne);
        assertEquals(hugeVector.getBigInteger(4), hugeMinPlusOne);
        assertThrows(() -> {
            hugeVector.setObject(5, new BigInteger("170141183460469231731687303715884105728"));
        }, IllegalArgumentException.class);

        UdfScalarWriter uhugeVector = new UdfScalarWriter(
            DUCKDB_TYPE_UHUGEINT.typeId, ByteBuffer.allocateDirect(rowCount * 16), null, validity, rowCount);
        byte[] uhugeValue = new byte[16];
        uhugeValue[3] = 9;
        uhugeValue[8] = 11;
        uhugeVector.setBytes(0, uhugeValue);
        assertEquals(uhugeVector.getBytes(0), uhugeValue);
        BigInteger uhugeBi = new BigInteger("340282366920938463463374607431768211455");
        uhugeVector.setObject(1, uhugeBi);
        assertEquals(uhugeVector.getBigInteger(1), uhugeBi);
        assertThrows(() -> { uhugeVector.setObject(2, BigInteger.valueOf(-1)); }, IllegalArgumentException.class);

        UdfScalarWriter uuidVector = new UdfScalarWriter(
            DUCKDB_TYPE_UUID.typeId, ByteBuffer.allocateDirect(rowCount * 16), null, validity, rowCount);
        byte[] uuidBytes = new byte[16];
        for (int i = 0; i < uuidBytes.length; i++) {
            uuidBytes[i] = (byte) (i + 1);
        }
        uuidVector.setBytes(0, uuidBytes);
        assertEquals(uuidVector.getBytes(0), uuidBytes);
        assertThrows(() -> { uuidVector.setBytes(1, new byte[8]); }, IllegalArgumentException.class);

        UdfScalarWriter timetzVector = new UdfScalarWriter(
            DUCKDB_TYPE_TIME_TZ.typeId, ByteBuffer.allocateDirect(rowCount * Long.BYTES), null, validity, rowCount);
        timetzVector.setLong(0, 123456789L);
        assertEquals(timetzVector.getLong(0), 123456789L);
        timetzVector.setObject(1, OffsetTime.of(1, 2, 3, 0, ZoneOffset.ofHoursMinutesSeconds(15, 59, 59)));
        assertThrows(() -> {
            timetzVector.setObject(2, OffsetTime.of(1, 2, 3, 0, ZoneOffset.ofHours(16)));
        }, IllegalArgumentException.class);

        UdfScalarWriter timestamptzVector =
            new UdfScalarWriter(DUCKDB_TYPE_TIMESTAMP_TZ.typeId, ByteBuffer.allocateDirect(rowCount * Long.BYTES), null,
                                validity, rowCount);
        timestamptzVector.setLong(0, 987654321L);
        assertEquals(timestamptzVector.getLong(0), 987654321L);

        UdfScalarWriter floatVector = new UdfScalarWriter(
            DUCKDB_TYPE_FLOAT.typeId, ByteBuffer.allocateDirect(rowCount * Float.BYTES), null, validity, rowCount);
        floatVector.setFloat(0, 1.25f);
        assertEquals(floatVector.getFloat(0), 1.25f, 0.0001f);

        UdfScalarWriter doubleVector = new UdfScalarWriter(
            DUCKDB_TYPE_DOUBLE.typeId, ByteBuffer.allocateDirect(rowCount * Double.BYTES), null, validity, rowCount);
        doubleVector.setDouble(0, 42.5d);
        assertEquals(doubleVector.getDouble(0), 42.5d, 0.0000001d);

        assertThrows(() -> {
            new UdfScalarWriter(DUCKDB_TYPE_INTEGER.typeId, null, null, validity, rowCount);
        }, IllegalArgumentException.class);
        assertThrows(() -> {
            new UdfScalarWriter(-1, ByteBuffer.allocateDirect(Integer.BYTES), null, validity, 1);
        }, IllegalArgumentException.class);
    }

    public static void test_bindings_udf_scalar_writer_varchar_accessors() throws Exception {
        ByteBuffer lt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR.typeId);
        ByteBuffer vec = duckdb_create_vector(lt);
        try {
            duckdb_vector_ensure_validity_writable(vec);
            int rowCount = (int) duckdb_vector_size();
            ByteBuffer validity = duckdb_vector_get_validity(vec, rowCount);
            UdfScalarWriter stringVector =
                new UdfScalarWriter(DUCKDB_TYPE_VARCHAR.typeId, null, vec, validity, rowCount);
            stringVector.setString(0, "alpha");
            assertEquals(stringVector.getString(0), "alpha");
            stringVector.setString(1, null);
            assertTrue(stringVector.isNull(1));
            assertNull(stringVector.getString(1));
            assertThrows(() -> { stringVector.getDouble(0); }, UnsupportedOperationException.class);
        } finally {
            duckdb_destroy_vector(vec);
            duckdb_destroy_logical_type(lt);
        }

        ByteBuffer blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB.typeId);
        ByteBuffer blobVec = duckdb_create_vector(blobType);
        try {
            duckdb_vector_ensure_validity_writable(blobVec);
            int rowCount = (int) duckdb_vector_size();
            ByteBuffer validity = duckdb_vector_get_validity(blobVec, rowCount);
            UdfScalarWriter blobVector =
                new UdfScalarWriter(DUCKDB_TYPE_BLOB.typeId, null, blobVec, validity, rowCount);
            byte[] payload = new byte[] {1, 2, 3, 4};
            blobVector.setBytes(0, payload);
            assertEquals(blobVector.getBytes(0), payload);
            blobVector.setBytes(1, null);
            assertTrue(blobVector.isNull(1));
            assertNull(blobVector.getBytes(1));
            assertThrows(() -> { blobVector.getString(0); }, UnsupportedOperationException.class);
        } finally {
            duckdb_destroy_vector(blobVec);
            duckdb_destroy_logical_type(blobType);
        }
    }

    public static void test_bindings_udf_scalar_writer_native_vectors_supported_types() throws Exception {
        int rowCount = (int) duckdb_vector_size();
        int[] udfTypeIds = new int[] {
            DUCKDB_TYPE_BOOLEAN.typeId,      DUCKDB_TYPE_TINYINT.typeId,      DUCKDB_TYPE_SMALLINT.typeId,
            DUCKDB_TYPE_INTEGER.typeId,      DUCKDB_TYPE_BIGINT.typeId,       DUCKDB_TYPE_UTINYINT.typeId,
            DUCKDB_TYPE_USMALLINT.typeId,    DUCKDB_TYPE_UINTEGER.typeId,     DUCKDB_TYPE_UBIGINT.typeId,
            DUCKDB_TYPE_HUGEINT.typeId,      DUCKDB_TYPE_UHUGEINT.typeId,     DUCKDB_TYPE_FLOAT.typeId,
            DUCKDB_TYPE_DOUBLE.typeId,       DUCKDB_TYPE_VARCHAR.typeId,      DUCKDB_TYPE_BLOB.typeId,
            DUCKDB_TYPE_DATE.typeId,         DUCKDB_TYPE_TIME.typeId,         DUCKDB_TYPE_TIME_NS.typeId,
            DUCKDB_TYPE_TIME_TZ.typeId,      DUCKDB_TYPE_TIMESTAMP.typeId,    DUCKDB_TYPE_TIMESTAMP_S.typeId,
            DUCKDB_TYPE_TIMESTAMP_MS.typeId, DUCKDB_TYPE_TIMESTAMP_NS.typeId, DUCKDB_TYPE_TIMESTAMP_TZ.typeId,
            DUCKDB_TYPE_UUID.typeId};

        for (int typeId : udfTypeIds) {
            ByteBuffer lt = duckdb_create_logical_type(typeId);
            ByteBuffer vec = duckdb_create_vector(lt);
            try {
                duckdb_vector_ensure_validity_writable(vec);
                ByteBuffer validity = duckdb_vector_get_validity(vec, rowCount);
                UdfScalarWriter column;
                if (UdfTypeCatalog.requiresVectorRef(UdfTypeCatalog.fromCapiTypeId(typeId))) {
                    column = new UdfScalarWriter(typeId, null, vec, validity, rowCount);
                } else {
                    long widthBytes = CAPIType.capiTypeFromTypeId(typeId).widthBytes;
                    ByteBuffer data = duckdb_vector_get_data(vec, rowCount * widthBytes);
                    column = new UdfScalarWriter(typeId, data, null, validity, rowCount);
                }

                if (typeId == DUCKDB_TYPE_BOOLEAN.typeId) {
                    column.setBoolean(0, true);
                    assertTrue(column.getBoolean(0));
                } else if (typeId == DUCKDB_TYPE_TINYINT.typeId) {
                    column.setInt(0, -7);
                    assertEquals(column.getInt(0), -7);
                } else if (typeId == DUCKDB_TYPE_SMALLINT.typeId) {
                    column.setInt(0, 32123);
                    assertEquals(column.getInt(0), 32123);
                } else if (typeId == DUCKDB_TYPE_INTEGER.typeId) {
                    column.setInt(0, 123456789);
                    assertEquals(column.getInt(0), 123456789);
                } else if (typeId == DUCKDB_TYPE_BIGINT.typeId) {
                    column.setLong(0, 9_876_543_210L);
                    assertEquals(column.getLong(0), 9_876_543_210L);
                } else if (typeId == DUCKDB_TYPE_UTINYINT.typeId) {
                    column.setInt(0, 250);
                    assertEquals(column.getInt(0), 250);
                } else if (typeId == DUCKDB_TYPE_USMALLINT.typeId) {
                    column.setInt(0, 65000);
                    assertEquals(column.getInt(0), 65000);
                } else if (typeId == DUCKDB_TYPE_UINTEGER.typeId) {
                    column.setLong(0, 4_000_000_000L);
                    assertEquals(column.getLong(0), 4_000_000_000L);
                } else if (typeId == DUCKDB_TYPE_UBIGINT.typeId) {
                    column.setLong(0, -1L);
                    assertEquals(column.getLong(0), -1L);
                } else if (typeId == DUCKDB_TYPE_HUGEINT.typeId || typeId == DUCKDB_TYPE_UHUGEINT.typeId ||
                           typeId == DUCKDB_TYPE_UUID.typeId) {
                    byte[] bytes = new byte[16];
                    bytes[0] = 1;
                    bytes[15] = 42;
                    column.setBytes(0, bytes);
                    assertEquals(column.getBytes(0), bytes);
                } else if (typeId == DUCKDB_TYPE_FLOAT.typeId) {
                    column.setFloat(0, 3.25f);
                    assertEquals(column.getFloat(0), 3.25f, 0.0001f);
                } else if (typeId == DUCKDB_TYPE_DOUBLE.typeId) {
                    column.setDouble(0, 8.125d);
                    assertEquals(column.getDouble(0), 8.125d, 0.0000001d);
                } else if (typeId == DUCKDB_TYPE_VARCHAR.typeId) {
                    column.setString(0, "alpha");
                    assertEquals(column.getString(0), "alpha");
                } else if (typeId == DUCKDB_TYPE_BLOB.typeId) {
                    byte[] blob = "blob-extended".getBytes(StandardCharsets.UTF_8);
                    column.setBytes(0, blob);
                    assertEquals(column.getBytes(0), blob);
                } else if (typeId == DUCKDB_TYPE_DECIMAL.typeId) {
                    column.setBigDecimal(0, new BigDecimal("123.75"));
                    assertEquals(column.getBigDecimal(0), new BigDecimal("123.75"));
                } else if (typeId == DUCKDB_TYPE_DATE.typeId) {
                    column.setInt(0, 19723);
                    assertEquals(column.getInt(0), 19723);
                } else if (typeId == DUCKDB_TYPE_TIME.typeId || typeId == DUCKDB_TYPE_TIME_NS.typeId ||
                           typeId == DUCKDB_TYPE_TIME_TZ.typeId || typeId == DUCKDB_TYPE_TIMESTAMP.typeId ||
                           typeId == DUCKDB_TYPE_TIMESTAMP_S.typeId || typeId == DUCKDB_TYPE_TIMESTAMP_MS.typeId ||
                           typeId == DUCKDB_TYPE_TIMESTAMP_NS.typeId || typeId == DUCKDB_TYPE_TIMESTAMP_TZ.typeId) {
                    column.setLong(0, 123456789L);
                    assertEquals(column.getLong(0), 123456789L);
                } else {
                    fail("Unhandled UDF type id: " + typeId);
                }

                column.setNull(1);
                assertTrue(column.isNull(1));
                if (typeId == DUCKDB_TYPE_VARCHAR.typeId) {
                    assertNull(column.getString(1));
                } else if (typeId == DUCKDB_TYPE_BLOB.typeId || typeId == DUCKDB_TYPE_HUGEINT.typeId ||
                           typeId == DUCKDB_TYPE_UHUGEINT.typeId || typeId == DUCKDB_TYPE_UUID.typeId) {
                    assertNull(column.getBytes(1));
                }
            } finally {
                duckdb_destroy_vector(vec);
                duckdb_destroy_logical_type(lt);
            }
        }
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

    public static void test_bindings_scalar_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            duckdb_register_scalar_function_java(conn.connRef, "bindings_java_scalar".getBytes(UTF_8),
                                                 (ctx, args, out, rowCount)
                                                     -> {
                                                     for (int row = 0; row < rowCount; row++) {
                                                         out.setInt(row, 42);
                                                     }
                                                 },
                                                 new org.duckdb.udf.UdfLogicalType[0],
                                                 org.duckdb.udf.UdfLogicalType.of(DuckDBColumnType.INTEGER), false,
                                                 false, true, false);

            try (ResultSet rs = stmt.executeQuery("SELECT bindings_java_scalar()")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertFalse(rs.next());
            }

            duckdb_register_scalar_function_java(
                conn.connRef, "bindings_java_scalar_add1".getBytes(UTF_8),
                (ctx, args, out, rowCount)
                    -> {
                    for (int row = 0; row < rowCount; row++) {
                        out.setInt(row, args[0].getInt(row) + 1);
                    }
                },
                new org.duckdb.udf.UdfLogicalType[] {org.duckdb.udf.UdfLogicalType.of(DuckDBColumnType.INTEGER)},
                org.duckdb.udf.UdfLogicalType.of(DuckDBColumnType.INTEGER), false, false, true, false);

            try (ResultSet rs = stmt.executeQuery("SELECT bindings_java_scalar_add1(41)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_bindings_table_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            duckdb_register_table_function_java(
                conn.connRef, "bindings_range_java".getBytes(UTF_8),
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public org.duckdb.udf.TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        return new org.duckdb.udf.TableBindResult(
                            new String[] {"i"},
                            new org.duckdb.udf.UdfLogicalType[] {
                                org.duckdb.udf.UdfLogicalType.of(DuckDBColumnType.BIGINT)},
                            ((Number) parameters[0]).longValue());
                    }

                    @Override
                    public org.duckdb.udf.TableState init(org.duckdb.udf.InitContext ctx,
                                                          org.duckdb.udf.TableBindResult bind) {
                        long end = ((Number) bind.getBindState()).longValue();
                        return new org.duckdb.udf.TableState(new long[] {0L, end});
                    }

                    @Override
                    public int produce(org.duckdb.udf.TableState state, org.duckdb.UdfOutputAppender out) {
                        long[] st = (long[]) state.getState();
                        int produced = 0;
                        for (; produced < 64 && st[0] < st[1]; produced++, st[0]++) {
                            out.beginRow().append(st[0]).endRow();
                        }
                        return out.getSize();
                    }
                },
                new org.duckdb.udf.UdfLogicalType[] {org.duckdb.udf.UdfLogicalType.of(DuckDBColumnType.BIGINT)}, true,
                4, true);

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM bindings_range_java(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), (long) i);
                }
                assertFalse(rs.next());
            }
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

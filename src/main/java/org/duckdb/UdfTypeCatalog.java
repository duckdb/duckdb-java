package org.duckdb;

import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.duckdb.udf.UdfLogicalType;

public final class UdfTypeCatalog {
    public enum Accessor {
        GET_INT,
        GET_LONG,
        GET_FLOAT,
        GET_DOUBLE,
        GET_DECIMAL,
        GET_BOOLEAN,
        GET_STRING,
        GET_BYTES,
        SET_INT,
        SET_LONG,
        SET_FLOAT,
        SET_DOUBLE,
        SET_DECIMAL,
        SET_BOOLEAN,
        SET_STRING,
        SET_BYTES
    }

    private static final EnumSet<DuckDBColumnType> CORE_TYPES = EnumSet.of(
        DuckDBColumnType.BOOLEAN, DuckDBColumnType.TINYINT, DuckDBColumnType.SMALLINT, DuckDBColumnType.INTEGER,
        DuckDBColumnType.BIGINT, DuckDBColumnType.FLOAT, DuckDBColumnType.DOUBLE, DuckDBColumnType.VARCHAR);

    private static final EnumSet<DuckDBColumnType> EXTENDED_TYPES =
        EnumSet.of(DuckDBColumnType.DECIMAL, DuckDBColumnType.BLOB, DuckDBColumnType.DATE, DuckDBColumnType.TIME,
                   DuckDBColumnType.TIME_NS, DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIMESTAMP_S,
                   DuckDBColumnType.TIMESTAMP_MS, DuckDBColumnType.TIMESTAMP_NS);

    private static final EnumSet<DuckDBColumnType> ADVANCED_TYPES = EnumSet.of(
        DuckDBColumnType.UTINYINT, DuckDBColumnType.USMALLINT, DuckDBColumnType.UINTEGER, DuckDBColumnType.UBIGINT,
        DuckDBColumnType.HUGEINT, DuckDBColumnType.UHUGEINT, DuckDBColumnType.TIME_WITH_TIME_ZONE,
        DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE, DuckDBColumnType.UUID);

    private static final EnumSet<DuckDBColumnType> SUPPORTED_UDF_TYPES = EnumSet.copyOf(CORE_TYPES);

    static {
        SUPPORTED_UDF_TYPES.addAll(EXTENDED_TYPES);
        SUPPORTED_UDF_TYPES.addAll(ADVANCED_TYPES);
    }

    private static final EnumSet<DuckDBColumnType> SCALAR_IMPLEMENTED_TYPES = EnumSet.copyOf(SUPPORTED_UDF_TYPES);

    private static final EnumSet<DuckDBColumnType> TABLE_BIND_SCHEMA_TYPES = EnumSet.copyOf(SUPPORTED_UDF_TYPES);

    private static final EnumSet<DuckDBColumnType> TABLE_PARAMETER_TYPES = EnumSet.copyOf(SUPPORTED_UDF_TYPES);

    private static final EnumSet<DuckDBColumnType> VARLEN_TYPES =
        EnumSet.of(DuckDBColumnType.VARCHAR, DuckDBColumnType.BLOB);
    private static final EnumSet<DuckDBColumnType> VECTOR_REF_TYPES =
        EnumSet.of(DuckDBColumnType.VARCHAR, DuckDBColumnType.BLOB, DuckDBColumnType.DECIMAL);

    private static final EnumMap<DuckDBColumnType, Integer> CAPI_TYPE_IDS = new EnumMap<>(DuckDBColumnType.class);
    private static final EnumMap<DuckDBColumnType, EnumSet<Accessor>> ACCESS_MATRIX =
        new EnumMap<>(DuckDBColumnType.class);
    private static final Map<Integer, DuckDBColumnType> COLUMN_TYPES_BY_CAPI_ID = new HashMap<>();
    private static final Map<DuckDBColumnType, Set<Accessor>> ACCESS_MATRIX_VIEW;

    static {
        CAPI_TYPE_IDS.put(DuckDBColumnType.BOOLEAN, DuckDBBindings.CAPIType.DUCKDB_TYPE_BOOLEAN.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TINYINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_TINYINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.SMALLINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_SMALLINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.INTEGER, DuckDBBindings.CAPIType.DUCKDB_TYPE_INTEGER.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.BIGINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_BIGINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.UTINYINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_UTINYINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.USMALLINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_USMALLINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.UINTEGER, DuckDBBindings.CAPIType.DUCKDB_TYPE_UINTEGER.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.UBIGINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_UBIGINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.HUGEINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_HUGEINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.UHUGEINT, DuckDBBindings.CAPIType.DUCKDB_TYPE_UHUGEINT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.FLOAT, DuckDBBindings.CAPIType.DUCKDB_TYPE_FLOAT.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.DOUBLE, DuckDBBindings.CAPIType.DUCKDB_TYPE_DOUBLE.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.VARCHAR, DuckDBBindings.CAPIType.DUCKDB_TYPE_VARCHAR.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.BLOB, DuckDBBindings.CAPIType.DUCKDB_TYPE_BLOB.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.DECIMAL, DuckDBBindings.CAPIType.DUCKDB_TYPE_DECIMAL.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.DATE, DuckDBBindings.CAPIType.DUCKDB_TYPE_DATE.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIME, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIME_NS, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME_NS.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIMESTAMP, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIMESTAMP_S, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_S.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIMESTAMP_MS, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_MS.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIMESTAMP_NS, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_NS.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIME_WITH_TIME_ZONE, DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME_TZ.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
                          DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_TZ.typeId);
        CAPI_TYPE_IDS.put(DuckDBColumnType.UUID, DuckDBBindings.CAPIType.DUCKDB_TYPE_UUID.typeId);

        ACCESS_MATRIX.put(DuckDBColumnType.BOOLEAN, EnumSet.of(Accessor.GET_BOOLEAN, Accessor.SET_BOOLEAN));
        ACCESS_MATRIX.put(DuckDBColumnType.TINYINT, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.SMALLINT, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.INTEGER, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.BIGINT, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.UTINYINT, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.USMALLINT, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.UINTEGER, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.UBIGINT, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.HUGEINT, EnumSet.of(Accessor.GET_BYTES, Accessor.SET_BYTES));
        ACCESS_MATRIX.put(DuckDBColumnType.UHUGEINT, EnumSet.of(Accessor.GET_BYTES, Accessor.SET_BYTES));
        ACCESS_MATRIX.put(DuckDBColumnType.FLOAT, EnumSet.of(Accessor.GET_FLOAT, Accessor.SET_FLOAT));
        ACCESS_MATRIX.put(DuckDBColumnType.DOUBLE, EnumSet.of(Accessor.GET_DOUBLE, Accessor.SET_DOUBLE));
        ACCESS_MATRIX.put(DuckDBColumnType.VARCHAR, EnumSet.of(Accessor.GET_STRING, Accessor.SET_STRING));
        ACCESS_MATRIX.put(DuckDBColumnType.BLOB, EnumSet.of(Accessor.GET_BYTES, Accessor.SET_BYTES));
        ACCESS_MATRIX.put(DuckDBColumnType.DECIMAL, EnumSet.of(Accessor.GET_DECIMAL, Accessor.SET_DECIMAL));
        ACCESS_MATRIX.put(DuckDBColumnType.DATE, EnumSet.of(Accessor.GET_INT, Accessor.SET_INT));
        ACCESS_MATRIX.put(DuckDBColumnType.TIME, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIME_NS, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIMESTAMP, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIMESTAMP_S, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIMESTAMP_MS, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIMESTAMP_NS, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIME_WITH_TIME_ZONE, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE, EnumSet.of(Accessor.GET_LONG, Accessor.SET_LONG));
        ACCESS_MATRIX.put(DuckDBColumnType.UUID, EnumSet.of(Accessor.GET_BYTES, Accessor.SET_BYTES));

        for (Map.Entry<DuckDBColumnType, Integer> entry : CAPI_TYPE_IDS.entrySet()) {
            COLUMN_TYPES_BY_CAPI_ID.put(entry.getValue(), entry.getKey());
        }

        EnumMap<DuckDBColumnType, Set<Accessor>> accessorMatrixView = new EnumMap<>(DuckDBColumnType.class);
        for (Map.Entry<DuckDBColumnType, EnumSet<Accessor>> entry : ACCESS_MATRIX.entrySet()) {
            accessorMatrixView.put(entry.getKey(), Collections.unmodifiableSet(EnumSet.copyOf(entry.getValue())));
        }
        ACCESS_MATRIX_VIEW = Collections.unmodifiableMap(accessorMatrixView);
    }

    private UdfTypeCatalog() {
    }

    public static int toCapiTypeId(DuckDBColumnType type) throws SQLFeatureNotSupportedException {
        Integer capiType = CAPI_TYPE_IDS.get(type);
        if (capiType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported UDF type: " + type);
        }
        return capiType;
    }

    public static DuckDBColumnType fromCapiTypeId(int capiTypeId) throws SQLFeatureNotSupportedException {
        DuckDBColumnType type = COLUMN_TYPES_BY_CAPI_ID.get(capiTypeId);
        if (type == null) {
            throw new SQLFeatureNotSupportedException("Unsupported C API type id for UDF: " + capiTypeId);
        }
        return type;
    }

    public static int toCapiTypeIdForScalarRegistration(DuckDBColumnType type) throws SQLFeatureNotSupportedException {
        if (!SCALAR_IMPLEMENTED_TYPES.contains(type)) {
            throw new SQLFeatureNotSupportedException(
                "Supported scalar UDF types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, "
                + "DECIMAL, BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, "
                + "UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, "
                + "TIMESTAMP_WITH_TIME_ZONE, UUID");
        }
        return toCapiTypeId(type);
    }

    public static void validateScalarLogicalType(UdfLogicalType logicalType) throws SQLFeatureNotSupportedException {
        if (logicalType == null) {
            throw new SQLFeatureNotSupportedException("Scalar UDF logical type cannot be null");
        }
        DuckDBColumnType type = logicalType.getType();
        if (type == null) {
            throw new SQLFeatureNotSupportedException("Scalar UDF logical type has null DuckDBColumnType");
        }
        if (!SCALAR_IMPLEMENTED_TYPES.contains(type)) {
            throw new SQLFeatureNotSupportedException(
                "Supported scalar UDF types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, "
                + "DECIMAL, BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, "
                + "UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, "
                + "TIMESTAMP_WITH_TIME_ZONE, UUID");
        }
    }

    public static boolean isScalarUdfImplemented(DuckDBColumnType type) {
        return SCALAR_IMPLEMENTED_TYPES.contains(type);
    }

    public static boolean isTableBindSchemaType(DuckDBColumnType type) {
        return TABLE_BIND_SCHEMA_TYPES.contains(type);
    }

    public static boolean isVarLenType(DuckDBColumnType type) {
        return VARLEN_TYPES.contains(type);
    }

    public static boolean requiresVectorRef(DuckDBColumnType type) {
        return VECTOR_REF_TYPES.contains(type);
    }

    public static boolean isTableFunctionParameterType(DuckDBColumnType type) {
        return TABLE_PARAMETER_TYPES.contains(type);
    }

    public static int toCapiTypeIdForTableFunctionParameter(DuckDBColumnType type)
        throws SQLFeatureNotSupportedException {
        if (!TABLE_PARAMETER_TYPES.contains(type)) {
            throw new SQLFeatureNotSupportedException(
                "Supported table function parameter types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, "
                + "DOUBLE, VARCHAR, DECIMAL, BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, "
                + "TIMESTAMP_NS, UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, "
                + "TIMESTAMP_WITH_TIME_ZONE, UUID");
        }
        return toCapiTypeId(type);
    }

    public static void validateTableFunctionParameterLogicalType(UdfLogicalType logicalType)
        throws SQLFeatureNotSupportedException {
        if (logicalType == null) {
            throw new SQLFeatureNotSupportedException("Table function parameter logical type cannot be null");
        }
        DuckDBColumnType type = logicalType.getType();
        if (type == null) {
            throw new SQLFeatureNotSupportedException(
                "Table function parameter logical type has null DuckDBColumnType");
        }

        if (TABLE_PARAMETER_TYPES.contains(type) || type == DuckDBColumnType.ENUM) {
            return;
        }

        switch (type) {
        case LIST:
        case ARRAY:
            validateTableFunctionParameterLogicalType(logicalType.getChildType());
            return;
        case MAP:
            validateTableFunctionParameterLogicalType(logicalType.getKeyType());
            validateTableFunctionParameterLogicalType(logicalType.getValueType());
            return;
        case STRUCT:
        case UNION:
            UdfLogicalType[] fieldTypes = logicalType.getFieldTypes();
            if (fieldTypes == null || fieldTypes.length == 0) {
                throw new SQLFeatureNotSupportedException("Table function " + type + " parameter requires fields");
            }
            for (UdfLogicalType fieldType : fieldTypes) {
                validateTableFunctionParameterLogicalType(fieldType);
            }
            return;
        default:
            throw new SQLFeatureNotSupportedException("Unsupported table function parameter type in logical schema: " +
                                                      type);
        }
    }

    public static boolean supportsAccessor(DuckDBColumnType type, Accessor accessor) {
        EnumSet<Accessor> accessors = ACCESS_MATRIX.get(type);
        return accessors != null && accessors.contains(accessor);
    }

    public static Map<DuckDBColumnType, Set<Accessor>> accessorMatrixView() {
        return ACCESS_MATRIX_VIEW;
    }
}

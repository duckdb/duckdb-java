package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.duckdb.udf.UdfLogicalType;

public final class UdfJavaTypeMapper {
    private static final Map<Class<?>, UdfLogicalType> TYPE_MAPPINGS = new HashMap<>();

    static {
        TYPE_MAPPINGS.put(Boolean.class, UdfLogicalType.of(DuckDBColumnType.BOOLEAN));
        TYPE_MAPPINGS.put(Byte.class, UdfLogicalType.of(DuckDBColumnType.TINYINT));
        TYPE_MAPPINGS.put(Short.class, UdfLogicalType.of(DuckDBColumnType.SMALLINT));
        TYPE_MAPPINGS.put(Integer.class, UdfLogicalType.of(DuckDBColumnType.INTEGER));
        TYPE_MAPPINGS.put(Long.class, UdfLogicalType.of(DuckDBColumnType.BIGINT));
        TYPE_MAPPINGS.put(Float.class, UdfLogicalType.of(DuckDBColumnType.FLOAT));
        TYPE_MAPPINGS.put(Double.class, UdfLogicalType.of(DuckDBColumnType.DOUBLE));
        TYPE_MAPPINGS.put(String.class, UdfLogicalType.of(DuckDBColumnType.VARCHAR));
        TYPE_MAPPINGS.put(byte[].class, UdfLogicalType.of(DuckDBColumnType.BLOB));
        TYPE_MAPPINGS.put(BigInteger.class, UdfLogicalType.of(DuckDBColumnType.HUGEINT));
        TYPE_MAPPINGS.put(UUID.class, UdfLogicalType.of(DuckDBColumnType.UUID));
        TYPE_MAPPINGS.put(LocalDate.class, UdfLogicalType.of(DuckDBColumnType.DATE));
        TYPE_MAPPINGS.put(Date.class, UdfLogicalType.of(DuckDBColumnType.DATE));
        TYPE_MAPPINGS.put(LocalTime.class, UdfLogicalType.of(DuckDBColumnType.TIME));
        TYPE_MAPPINGS.put(OffsetTime.class, UdfLogicalType.of(DuckDBColumnType.TIME_WITH_TIME_ZONE));
        TYPE_MAPPINGS.put(LocalDateTime.class, UdfLogicalType.of(DuckDBColumnType.TIMESTAMP));
        TYPE_MAPPINGS.put(OffsetDateTime.class, UdfLogicalType.of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
    }

    private UdfJavaTypeMapper() {
    }

    public static UdfLogicalType toLogicalType(Class<?> javaType) {
        if (javaType == null) {
            throw new IllegalArgumentException("javaType must not be null");
        }
        Class<?> normalizedType = javaType.isPrimitive() ? wrapPrimitive(javaType) : javaType;
        if (normalizedType == BigDecimal.class) {
            throw new IllegalArgumentException(
                "BigDecimal requires explicit logical type; use UdfLogicalType.decimal(width, scale)");
        }
        UdfLogicalType logicalType = TYPE_MAPPINGS.get(normalizedType);
        if (logicalType == null) {
            throw new IllegalArgumentException("Unsupported Java class for scalar UDF mapping: " +
                                               normalizedType.getName());
        }
        return logicalType;
    }

    private static Class<?> wrapPrimitive(Class<?> primitiveType) {
        if (primitiveType == boolean.class) {
            return Boolean.class;
        }
        if (primitiveType == byte.class) {
            return Byte.class;
        }
        if (primitiveType == short.class) {
            return Short.class;
        }
        if (primitiveType == int.class) {
            return Integer.class;
        }
        if (primitiveType == long.class) {
            return Long.class;
        }
        if (primitiveType == float.class) {
            return Float.class;
        }
        if (primitiveType == double.class) {
            return Double.class;
        }
        throw new IllegalArgumentException("Unsupported primitive Java type for scalar UDF mapping: " +
                                           primitiveType.getName());
    }
}

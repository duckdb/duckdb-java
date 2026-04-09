package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import org.duckdb.DuckDBFunctions.FunctionException;

final class DuckDBScalarFunctionAdapter {
    private static final Map<DuckDBColumnType, TypeCodec> CODECS_BY_DUCKDB_TYPE = new LinkedHashMap<>();
    private static final Map<Class<?>, DuckDBColumnType> DUCKDB_TYPE_BY_JAVA_CLASS = new LinkedHashMap<>();
    private static final Map<Class<?>, Class<?>> BOXED_CLASSES = new LinkedHashMap<>();
    private static final TypeCodec DATE_SQL_CODEC =
        new TypeCodec(java.sql.Date.class, DuckDBReadableVector::getDate,
                      (vector, row, value) -> vector.setDate(row, (java.sql.Date) value));
    private static final TypeCodec TIMESTAMP_SQL_CODEC =
        new TypeCodec(java.sql.Timestamp.class, DuckDBReadableVector::getTimestamp,
                      (vector, row, value) -> vector.setTimestamp(row, (java.sql.Timestamp) value));
    private static final TypeCodec TIMESTAMP_UTIL_DATE_CODEC = new TypeCodec(
        Date.class,
        (vector, row)
            -> Date.from(vector.getLocalDateTime(row).toInstant(ZoneOffset.UTC)),
        (vector, row,
         value) -> vector.setTimestamp(row, LocalDateTime.ofInstant(((Date) value).toInstant(), ZoneOffset.UTC)));

    static {
        register(DuckDBColumnType.BOOLEAN, Boolean.class, DuckDBReadableVector::getBoolean,
                 DuckDBWritableVector::setBoolean);
        register(DuckDBColumnType.TINYINT, Byte.class, DuckDBReadableVector::getByte, DuckDBWritableVector::setByte);
        register(DuckDBColumnType.UTINYINT, Short.class, DuckDBReadableVector::getUint8,
                 (out, row, value) -> out.setUint8(row, value));
        register(DuckDBColumnType.SMALLINT, Short.class, DuckDBReadableVector::getShort,
                 DuckDBWritableVector::setShort);
        register(DuckDBColumnType.USMALLINT, Integer.class, DuckDBReadableVector::getUint16,
                 (out, row, value) -> out.setUint16(row, value));
        register(DuckDBColumnType.INTEGER, Integer.class, DuckDBReadableVector::getInt, DuckDBWritableVector::setInt);
        register(DuckDBColumnType.UINTEGER, Long.class, DuckDBReadableVector::getUint32,
                 (out, row, value) -> out.setUint32(row, value));
        register(DuckDBColumnType.BIGINT, Long.class, DuckDBReadableVector::getLong, DuckDBWritableVector::setLong);
        register(DuckDBColumnType.HUGEINT, BigInteger.class, DuckDBReadableVector::getHugeInt,
                 DuckDBWritableVector::setHugeInt);
        register(DuckDBColumnType.UHUGEINT, BigInteger.class, DuckDBReadableVector::getUHugeInt,
                 DuckDBWritableVector::setUHugeInt);
        register(DuckDBColumnType.UBIGINT, BigInteger.class, DuckDBReadableVector::getUint64,
                 DuckDBWritableVector::setUint64);
        register(DuckDBColumnType.FLOAT, Float.class, DuckDBReadableVector::getFloat, DuckDBWritableVector::setFloat);
        register(DuckDBColumnType.DOUBLE, Double.class, DuckDBReadableVector::getDouble,
                 DuckDBWritableVector::setDouble);
        register(DuckDBColumnType.DECIMAL, BigDecimal.class, DuckDBReadableVector::getBigDecimal,
                 DuckDBWritableVector::setBigDecimal);
        register(DuckDBColumnType.VARCHAR, String.class, DuckDBReadableVector::getString,
                 DuckDBWritableVector::setString);
        register(DuckDBColumnType.DATE, LocalDate.class, DuckDBReadableVector::getLocalDate,
                 DuckDBWritableVector::setDate);
        register(DuckDBColumnType.TIMESTAMP_S, LocalDateTime.class, DuckDBReadableVector::getLocalDateTime,
                 DuckDBWritableVector::setTimestamp);
        register(DuckDBColumnType.TIMESTAMP_MS, LocalDateTime.class, DuckDBReadableVector::getLocalDateTime,
                 DuckDBWritableVector::setTimestamp);
        register(DuckDBColumnType.TIMESTAMP, LocalDateTime.class, DuckDBReadableVector::getLocalDateTime,
                 DuckDBWritableVector::setTimestamp);
        register(DuckDBColumnType.TIMESTAMP_NS, LocalDateTime.class, DuckDBReadableVector::getLocalDateTime,
                 DuckDBWritableVector::setTimestamp);
        register(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.class,
                 DuckDBReadableVector::getOffsetDateTime, DuckDBWritableVector::setOffsetDateTime);

        DUCKDB_TYPE_BY_JAVA_CLASS.put(boolean.class, DuckDBColumnType.BOOLEAN);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Boolean.class, DuckDBColumnType.BOOLEAN);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(byte.class, DuckDBColumnType.TINYINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Byte.class, DuckDBColumnType.TINYINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(short.class, DuckDBColumnType.SMALLINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Short.class, DuckDBColumnType.SMALLINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(int.class, DuckDBColumnType.INTEGER);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Integer.class, DuckDBColumnType.INTEGER);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(long.class, DuckDBColumnType.BIGINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Long.class, DuckDBColumnType.BIGINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(float.class, DuckDBColumnType.FLOAT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Float.class, DuckDBColumnType.FLOAT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(double.class, DuckDBColumnType.DOUBLE);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Double.class, DuckDBColumnType.DOUBLE);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(String.class, DuckDBColumnType.VARCHAR);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(BigDecimal.class, DuckDBColumnType.DECIMAL);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(BigInteger.class, DuckDBColumnType.HUGEINT);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(LocalDate.class, DuckDBColumnType.DATE);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(java.sql.Date.class, DuckDBColumnType.DATE);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(LocalDateTime.class, DuckDBColumnType.TIMESTAMP);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(java.sql.Timestamp.class, DuckDBColumnType.TIMESTAMP);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(Date.class, DuckDBColumnType.TIMESTAMP);
        DUCKDB_TYPE_BY_JAVA_CLASS.put(OffsetDateTime.class, DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);

        BOXED_CLASSES.put(boolean.class, Boolean.class);
        BOXED_CLASSES.put(byte.class, Byte.class);
        BOXED_CLASSES.put(short.class, Short.class);
        BOXED_CLASSES.put(int.class, Integer.class);
        BOXED_CLASSES.put(long.class, Long.class);
        BOXED_CLASSES.put(float.class, Float.class);
        BOXED_CLASSES.put(double.class, Double.class);
    }

    static DuckDBScalarFunction unary(Function<?, ?> function, DuckDBColumnType parameterType,
                                      Class<?> parameterJavaType, DuckDBColumnType returnType, Class<?> returnJavaType)
        throws SQLException {
        @SuppressWarnings("unchecked") Function<Object, Object> typedFunction = (Function<Object, Object>) function;
        TypeCodec inCodec = codecFor(parameterType, parameterJavaType);
        TypeCodec outCodec = codecFor(returnType, returnJavaType);
        return (input, output) -> {
            DuckDBReadableVector in = input.vector(0);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    Object argument = in.isNull(row) ? null : inCodec.read(in, row);
                    Object result = typedFunction.apply(argument);
                    outCodec.write(output, row, result);
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute unary scalar function at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction binary(BiFunction<?, ?, ?> function, DuckDBColumnType leftType, Class<?> leftJavaType,
                                       DuckDBColumnType rightType, Class<?> rightJavaType, DuckDBColumnType returnType,
                                       Class<?> returnJavaType) throws SQLException {
        @SuppressWarnings("unchecked")
        BiFunction<Object, Object, Object> typedFunction = (BiFunction<Object, Object, Object>) function;
        TypeCodec leftCodec = codecFor(leftType, leftJavaType);
        TypeCodec rightCodec = codecFor(rightType, rightJavaType);
        TypeCodec outCodec = codecFor(returnType, returnJavaType);
        return (input, output) -> {
            DuckDBReadableVector left = input.vector(0);
            DuckDBReadableVector right = input.vector(1);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    Object leftValue = left.isNull(row) ? null : leftCodec.read(left, row);
                    Object rightValue = right.isNull(row) ? null : rightCodec.read(right, row);
                    Object result = typedFunction.apply(leftValue, rightValue);
                    outCodec.write(output, row, result);
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute binary scalar function at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction intUnary(IntUnaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector in = input.vector(0);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (in.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setInt(row, function.applyAsInt(in.getInt(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withIntFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction intBinary(IntBinaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector left = input.vector(0);
            DuckDBReadableVector right = input.vector(1);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (left.isNull(row) || right.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setInt(row, function.applyAsInt(left.getInt(row), right.getInt(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withIntFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction doubleUnary(DoubleUnaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector in = input.vector(0);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (in.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setDouble(row, function.applyAsDouble(in.getDouble(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withDoubleFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction doubleBinary(DoubleBinaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector left = input.vector(0);
            DuckDBReadableVector right = input.vector(1);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (left.isNull(row) || right.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setDouble(row, function.applyAsDouble(left.getDouble(row), right.getDouble(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withDoubleFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction longUnary(LongUnaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector in = input.vector(0);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (in.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setLong(row, function.applyAsLong(in.getLong(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withLongFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction longBinary(LongBinaryOperator function) {
        return (input, output) -> {
            DuckDBReadableVector left = input.vector(0);
            DuckDBReadableVector right = input.vector(1);
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    if (left.isNull(row) || right.isNull(row)) {
                        output.setNull(row);
                    } else {
                        output.setLong(row, function.applyAsLong(left.getLong(row), right.getLong(row)));
                    }
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute withLongFunction at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction nullary(Supplier<?> function, DuckDBColumnType returnType, Class<?> returnJavaType)
        throws SQLException {
        @SuppressWarnings("unchecked") Supplier<Object> typedFunction = (Supplier<Object>) function;
        TypeCodec outCodec = codecFor(returnType, returnJavaType);
        return (input, output) -> {
            long rowCount = input.rowCount();
            for (long row = 0; row < rowCount; row++) {
                try {
                    Object result = typedFunction.get();
                    outCodec.write(output, row, result);
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute supplier scalar function at row " + row, exception);
                }
            }
        };
    }

    static DuckDBScalarFunction variadic(Function<Object[], ?> function, DuckDBColumnType[] fixedTypes,
                                         Class<?>[] fixedJavaTypes, DuckDBColumnType varArgType,
                                         Class<?> varArgJavaType, DuckDBColumnType returnType, Class<?> returnJavaType)
        throws SQLException {
        TypeCodec outCodec = codecFor(returnType, returnJavaType);
        TypeCodec varArgCodec = codecFor(varArgType, varArgJavaType);
        TypeCodec[] fixedCodecs = new TypeCodec[fixedTypes.length];
        for (int i = 0; i < fixedTypes.length; i++) {
            fixedCodecs[i] = codecFor(fixedTypes[i], fixedJavaTypes[i]);
        }
        return (input, output) -> {
            long rowCount = input.rowCount();
            int vectorCount = Math.toIntExact(input.columnCount());
            DuckDBReadableVector[] vectors = new DuckDBReadableVector[vectorCount];
            TypeCodec[] codecs = new TypeCodec[vectorCount];
            for (int column = 0; column < vectorCount; column++) {
                vectors[column] = input.vector(column);
                codecs[column] = column < fixedCodecs.length ? fixedCodecs[column] : varArgCodec;
            }
            Object[] args = new Object[vectorCount];
            for (long row = 0; row < rowCount; row++) {
                try {
                    for (int column = 0; column < vectorCount; column++) {
                        DuckDBReadableVector vector = vectors[column];
                        if (vector.isNull(row)) {
                            args[column] = null;
                        } else {
                            args[column] = codecs[column].read(vector, row);
                        }
                    }
                    Object result = function.apply(args);
                    outCodec.write(output, row, result);
                } catch (FunctionException exception) {
                    throw new FunctionException("Failed to execute variadic scalar function at row " + row, exception);
                }
            }
        };
    }

    static DuckDBColumnType mapJavaClassToDuckDBType(Class<?> javaType) throws SQLException {
        if (javaType == null) {
            throw new SQLException("Java type cannot be null");
        }
        Class<?> normalizedClass = normalizeJavaClass(javaType);
        DuckDBColumnType mappedType = DUCKDB_TYPE_BY_JAVA_CLASS.get(normalizedClass);
        if (mappedType != null) {
            return mappedType;
        }
        throw new SQLException("Unsupported Java type for scalar function mapping: " + javaType.getName());
    }

    static DuckDBColumnType mapLogicalTypeToDuckDBType(DuckDBLogicalType logicalType) throws SQLException {
        if (logicalType == null) {
            throw new SQLException("Logical type cannot be null");
        }
        DuckDBBindings.CAPIType type =
            DuckDBBindings.CAPIType.capiTypeFromTypeId(duckdb_get_type_id(logicalType.logicalTypeRef()));
        switch (type) {
        case DUCKDB_TYPE_BOOLEAN:
            return DuckDBColumnType.BOOLEAN;
        case DUCKDB_TYPE_TINYINT:
            return DuckDBColumnType.TINYINT;
        case DUCKDB_TYPE_UTINYINT:
            return DuckDBColumnType.UTINYINT;
        case DUCKDB_TYPE_SMALLINT:
            return DuckDBColumnType.SMALLINT;
        case DUCKDB_TYPE_USMALLINT:
            return DuckDBColumnType.USMALLINT;
        case DUCKDB_TYPE_INTEGER:
            return DuckDBColumnType.INTEGER;
        case DUCKDB_TYPE_UINTEGER:
            return DuckDBColumnType.UINTEGER;
        case DUCKDB_TYPE_BIGINT:
            return DuckDBColumnType.BIGINT;
        case DUCKDB_TYPE_HUGEINT:
            return DuckDBColumnType.HUGEINT;
        case DUCKDB_TYPE_UBIGINT:
            return DuckDBColumnType.UBIGINT;
        case DUCKDB_TYPE_UHUGEINT:
            return DuckDBColumnType.UHUGEINT;
        case DUCKDB_TYPE_FLOAT:
            return DuckDBColumnType.FLOAT;
        case DUCKDB_TYPE_DOUBLE:
            return DuckDBColumnType.DOUBLE;
        case DUCKDB_TYPE_DECIMAL:
            return DuckDBColumnType.DECIMAL;
        case DUCKDB_TYPE_VARCHAR:
            return DuckDBColumnType.VARCHAR;
        case DUCKDB_TYPE_DATE:
            return DuckDBColumnType.DATE;
        case DUCKDB_TYPE_TIMESTAMP_S:
            return DuckDBColumnType.TIMESTAMP_S;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return DuckDBColumnType.TIMESTAMP_MS;
        case DUCKDB_TYPE_TIMESTAMP:
            return DuckDBColumnType.TIMESTAMP;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return DuckDBColumnType.TIMESTAMP_NS;
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            return DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE;
        default:
            throw new SQLException("Unsupported logical type for Function/BiFunction mapping: " + type);
        }
    }

    private static TypeCodec codecFor(DuckDBColumnType type) throws SQLException {
        TypeCodec codec = CODECS_BY_DUCKDB_TYPE.get(type);
        if (codec != null) {
            return codec;
        }
        throw new SQLException("Unsupported DuckDB type for Function/BiFunction mapping: " + type);
    }

    private static TypeCodec codecFor(DuckDBColumnType type, Class<?> declaredJavaType) throws SQLException {
        if (declaredJavaType == null) {
            return codecFor(type);
        }
        Class<?> normalizedClass = normalizeJavaClass(declaredJavaType);
        switch (type) {
        case DATE:
            if (normalizedClass == LocalDate.class) {
                return codecFor(type);
            }
            if (normalizedClass == java.sql.Date.class) {
                return DATE_SQL_CODEC;
            }
            break;
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
            if (normalizedClass == LocalDateTime.class) {
                return codecFor(type);
            }
            if (normalizedClass == java.sql.Timestamp.class) {
                return TIMESTAMP_SQL_CODEC;
            }
            if (normalizedClass == Date.class) {
                return TIMESTAMP_UTIL_DATE_CODEC;
            }
            break;
        default: {
            TypeCodec codec = codecFor(type);
            if (codec.matches(normalizedClass)) {
                return codec;
            }
            break;
        }
        }
        throw new SQLException("Unsupported Java type " + normalizedClass.getName() + " for DuckDB type " + type +
                               " in functional scalar function mapping");
    }

    private static <T> void register(DuckDBColumnType type, Class<T> javaType, Reader<T> reader, Writer<T> writer) {
        CODECS_BY_DUCKDB_TYPE.put(type, new TypeCodec(javaType, reader, writer));
    }

    private static Class<?> normalizeJavaClass(Class<?> javaType) {
        Class<?> boxedClass = BOXED_CLASSES.get(javaType);
        return boxedClass != null ? boxedClass : javaType;
    }

    private DuckDBScalarFunctionAdapter() {
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DuckDBReadableVector vector, long row);
    }

    @FunctionalInterface
    private interface Writer<T> {
        void write(DuckDBWritableVector vector, long row, T value);
    }

    private static final class TypeCodec {
        private final Class<?> javaType;
        private final Reader<?> reader;
        private final Writer<?> writer;

        private TypeCodec(Class<?> javaType, Reader<?> reader, Writer<?> writer) {
            this.javaType = javaType;
            this.reader = reader;
            this.writer = writer;
        }

        boolean matches(Class<?> declaredJavaType) {
            return javaType == declaredJavaType;
        }

        Object read(DuckDBReadableVector vector, long row) {
            return reader.read(vector, row);
        }

        void write(DuckDBWritableVector vector, long row, Object value) {
            if (value == null) {
                vector.setNull(row);
                return;
            }
            if (!javaType.isInstance(value)) {
                throw new ClassCastException("Expected value of type " + javaType.getName() + ", got " +
                                             value.getClass().getName());
            }
            @SuppressWarnings("unchecked") Writer<Object> typedWriter = (Writer<Object>) writer;
            typedWriter.write(vector, row, value);
        }
    }
}

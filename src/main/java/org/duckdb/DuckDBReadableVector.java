package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.stream.LongStream;
import org.duckdb.DuckDBFunctions.FunctionException;

public final class DuckDBReadableVector {
    private static final BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private final ByteBuffer validity;

    DuckDBReadableVector(ByteBuffer vectorRef, long rowCount) {
        if (vectorRef == null) {
            throw new FunctionException("Invalid vector reference");
        }
        this.vectorRef = vectorRef;
        this.rowCount = rowCount;
        try {
            this.typeInfo = DuckDBVectorTypeInfo.fromVector(vectorRef);
        } catch (java.sql.SQLException exception) {
            throw new FunctionException("Failed to resolve vector type info", exception);
        }
        this.data =
            duckdb_vector_get_data(vectorRef, Math.multiplyExact(rowCount, typeInfo.widthBytes)).order(NATIVE_ORDER);
        this.validity = duckdb_vector_get_validity(vectorRef, rowCount);
        if (this.validity != null) {
            this.validity.order(NATIVE_ORDER);
        }
    }

    public DuckDBColumnType getType() {
        return typeInfo.columnType;
    }

    public long rowCount() {
        return rowCount;
    }

    public LongStream stream() {
        return LongStream.range(0, rowCount);
    }

    public boolean isNull(long row) {
        checkRowIndex(row);
        if (validity == null) {
            return false;
        }
        int entryPos = Math.toIntExact(Math.multiplyExact(row / Long.SIZE, (long) Long.BYTES));
        long mask = validity.getLong(entryPos);
        return (mask & (1L << (row % Long.SIZE))) == 0;
    }

    public boolean getBoolean(long row) {
        requireType(DuckDBColumnType.BOOLEAN);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.BOOLEAN, row);
        }
        return data.get(checkedRowIndex(row)) != 0;
    }

    public boolean getBoolean(long row, boolean defaultValue) {
        requireType(DuckDBColumnType.BOOLEAN);
        return isNull(row) ? defaultValue : data.get(checkedRowIndex(row)) != 0;
    }

    public byte getByte(long row) {
        requireType(DuckDBColumnType.TINYINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.TINYINT, row);
        }
        return data.get(checkedRowIndex(row));
    }

    public byte getByte(long row, byte defaultValue) {
        requireType(DuckDBColumnType.TINYINT);
        return isNull(row) ? defaultValue : data.get(checkedRowIndex(row));
    }

    public short getShort(long row) {
        requireType(DuckDBColumnType.SMALLINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.SMALLINT, row);
        }
        return data.getShort(checkedByteOffset(row, Short.BYTES));
    }

    public short getShort(long row, short defaultValue) {
        requireType(DuckDBColumnType.SMALLINT);
        return isNull(row) ? defaultValue : data.getShort(checkedByteOffset(row, Short.BYTES));
    }

    public short getUint8(long row) {
        requireType(DuckDBColumnType.UTINYINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.UTINYINT, row);
        }
        return (short) Byte.toUnsignedInt(data.get(checkedRowIndex(row)));
    }

    public short getUint8(long row, short defaultValue) {
        requireType(DuckDBColumnType.UTINYINT);
        return isNull(row) ? defaultValue : (short) Byte.toUnsignedInt(data.get(checkedRowIndex(row)));
    }

    public int getUint16(long row) {
        requireType(DuckDBColumnType.USMALLINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.USMALLINT, row);
        }
        return Short.toUnsignedInt(data.getShort(checkedByteOffset(row, Short.BYTES)));
    }

    public int getUint16(long row, int defaultValue) {
        requireType(DuckDBColumnType.USMALLINT);
        return isNull(row) ? defaultValue : Short.toUnsignedInt(data.getShort(checkedByteOffset(row, Short.BYTES)));
    }

    public int getInt(long row) {
        requireType(DuckDBColumnType.INTEGER);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.INTEGER, row);
        }
        return data.getInt(checkedByteOffset(row, Integer.BYTES));
    }

    public int getInt(long row, int defaultValue) {
        requireType(DuckDBColumnType.INTEGER);
        return isNull(row) ? defaultValue : data.getInt(checkedByteOffset(row, Integer.BYTES));
    }

    public long getUint32(long row) {
        requireType(DuckDBColumnType.UINTEGER);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.UINTEGER, row);
        }
        return Integer.toUnsignedLong(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    public long getUint32(long row, long defaultValue) {
        requireType(DuckDBColumnType.UINTEGER);
        return isNull(row) ? defaultValue : Integer.toUnsignedLong(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    public long getLong(long row) {
        requireType(DuckDBColumnType.BIGINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.BIGINT, row);
        }
        return data.getLong(checkedByteOffset(row, Long.BYTES));
    }

    public long getLong(long row, long defaultValue) {
        requireType(DuckDBColumnType.BIGINT);
        return isNull(row) ? defaultValue : data.getLong(checkedByteOffset(row, Long.BYTES));
    }

    public BigInteger getHugeInt(long row) {
        requireType(DuckDBColumnType.HUGEINT);
        if (isNull(row)) {
            return null;
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        long lower = data.getLong(offset);
        long upper = data.getLong(offset + Long.BYTES);
        return DuckDBHugeInt.toBigInteger(lower, upper);
    }

    public BigInteger getUHugeInt(long row) {
        requireType(DuckDBColumnType.UHUGEINT);
        if (isNull(row)) {
            return null;
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        long lower = data.getLong(offset);
        long upper = data.getLong(offset + Long.BYTES);
        return DuckDBHugeInt.toUnsignedBigInteger(lower, upper);
    }

    public BigInteger getUint64(long row) {
        requireType(DuckDBColumnType.UBIGINT);
        if (isNull(row)) {
            return null;
        }
        long value = data.getLong(checkedByteOffset(row, Long.BYTES));
        return unsignedLongToBigInteger(value);
    }

    public float getFloat(long row) {
        requireType(DuckDBColumnType.FLOAT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.FLOAT, row);
        }
        return data.getFloat(checkedByteOffset(row, Float.BYTES));
    }

    public float getFloat(long row, float defaultValue) {
        requireType(DuckDBColumnType.FLOAT);
        return isNull(row) ? defaultValue : data.getFloat(checkedByteOffset(row, Float.BYTES));
    }

    public double getDouble(long row) {
        requireType(DuckDBColumnType.DOUBLE);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.DOUBLE, row);
        }
        return data.getDouble(checkedByteOffset(row, Double.BYTES));
    }

    public double getDouble(long row, double defaultValue) {
        requireType(DuckDBColumnType.DOUBLE);
        return isNull(row) ? defaultValue : data.getDouble(checkedByteOffset(row, Double.BYTES));
    }

    public LocalDate getLocalDate(long row) {
        requireType(DuckDBColumnType.DATE);
        if (isNull(row)) {
            return null;
        }
        return LocalDate.ofEpochDay(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    public Date getDate(long row) {
        LocalDate value = getLocalDate(row);
        return value == null ? null : Date.valueOf(value);
    }

    public LocalDateTime getLocalDateTime(long row) {
        requireTimestampType();
        if (isNull(row)) {
            return null;
        }
        long epochValue = data.getLong(checkedByteOffset(row, Long.BYTES));
        try {
            switch (typeInfo.capiType) {
            case DUCKDB_TYPE_TIMESTAMP_S:
                return DuckDBTimestamp.localDateTimeFromTimestamp(epochValue, ChronoUnit.SECONDS, null);
            case DUCKDB_TYPE_TIMESTAMP_MS:
                return DuckDBTimestamp.localDateTimeFromTimestamp(epochValue, ChronoUnit.MILLIS, null);
            case DUCKDB_TYPE_TIMESTAMP:
                return DuckDBTimestamp.localDateTimeFromTimestamp(epochValue, ChronoUnit.MICROS, null);
            case DUCKDB_TYPE_TIMESTAMP_NS:
                return DuckDBTimestamp.localDateTimeFromTimestamp(epochValue, ChronoUnit.NANOS, null);
            case DUCKDB_TYPE_TIMESTAMP_TZ:
                return DuckDBTimestamp.localDateTimeFromTimestampWithTimezone(epochValue, ChronoUnit.MICROS, null);
            default:
                throw new FunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
            }
        } catch (java.sql.SQLException exception) {
            throw new FunctionException("Failed to decode timestamp at row " + row, exception);
        }
    }

    public Timestamp getTimestamp(long row) {
        LocalDateTime value = getLocalDateTime(row);
        return value == null ? null : Timestamp.valueOf(value);
    }

    public OffsetDateTime getOffsetDateTime(long row) {
        requireType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);
        if (isNull(row)) {
            return null;
        }
        long micros = data.getLong(checkedByteOffset(row, Long.BYTES));
        Instant instant = instantFromEpoch(micros, ChronoUnit.MICROS);
        return instant.atZone(ZoneId.systemDefault()).toOffsetDateTime();
    }

    public BigDecimal getBigDecimal(long row) {
        requireType(DuckDBColumnType.DECIMAL);
        if (isNull(row)) {
            return null;
        }
        switch (typeInfo.storageType) {
        case DUCKDB_TYPE_SMALLINT:
            return BigDecimal.valueOf(data.getShort(checkedByteOffset(row, Short.BYTES)), typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_INTEGER:
            return BigDecimal.valueOf(data.getInt(checkedByteOffset(row, Integer.BYTES)), typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_BIGINT:
            return BigDecimal.valueOf(data.getLong(checkedByteOffset(row, Long.BYTES)), typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_HUGEINT: {
            int offset = checkedByteOffset(row, typeInfo.widthBytes);
            long lower = data.getLong(offset);
            long upper = data.getLong(offset + Long.BYTES);
            return new BigDecimal(upper)
                .multiply(ULONG_MULTIPLIER)
                .add(new BigDecimal(Long.toUnsignedString(lower)))
                .scaleByPowerOfTen(typeInfo.decimalMeta.scale * -1);
        }
        default:
            throw new FunctionException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
    }

    public String getString(long row) {
        requireType(DuckDBColumnType.VARCHAR);
        if (isNull(row)) {
            return null;
        }
        byte[] bytes = duckdb_vector_get_string(data, row);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, UTF_8);
    }

    ByteBuffer vectorRef() {
        return vectorRef;
    }

    private void requireType(DuckDBColumnType expected) {
        if (typeInfo.columnType != expected) {
            throw new FunctionException("Expected vector type " + expected + ", found " + typeInfo.columnType);
        }
    }

    private void requireTimestampType() {
        switch (typeInfo.columnType) {
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return;
        default:
            throw new FunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
        }
    }

    private void checkRowIndex(long row) {
        if (row < 0 || row >= rowCount) {
            throw new IndexOutOfBoundsException("Row index out of bounds: " + row);
        }
    }

    private int checkedRowIndex(long row) {
        checkRowIndex(row);
        return Math.toIntExact(row);
    }

    private int checkedByteOffset(long row, int elementWidth) {
        checkRowIndex(row);
        return Math.toIntExact(Math.multiplyExact(row, (long) elementWidth));
    }

    private static Instant instantFromEpoch(long value, ChronoUnit unit) {
        switch (unit) {
        case SECONDS:
            return Instant.ofEpochSecond(value);
        case MILLIS:
            return Instant.ofEpochMilli(value);
        case MICROS: {
            long epochSecond = Math.floorDiv(value, 1_000_000L);
            long nanoAdjustment = Math.floorMod(value, 1_000_000L) * 1000L;
            return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
        }
        case NANOS: {
            long epochSecond = Math.floorDiv(value, 1_000_000_000L);
            long nanoAdjustment = Math.floorMod(value, 1_000_000_000L);
            return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
        }
        default:
            throw new FunctionException("Unsupported unit type: " + unit);
        }
    }

    private static BigInteger unsignedLongToBigInteger(long value) {
        if (value >= 0) {
            return BigInteger.valueOf(value);
        }
        return BigInteger.valueOf(value & Long.MAX_VALUE).setBit(Long.SIZE - 1);
    }

    private static FunctionException primitiveNullValue(DuckDBColumnType type, long row) {
        return new FunctionException("Primitive value for " + type + " at row " + row + " is NULL");
    }
}

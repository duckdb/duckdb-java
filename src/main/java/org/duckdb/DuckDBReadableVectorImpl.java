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

final class DuckDBReadableVectorImpl extends DuckDBReadableVector {
    private static final BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private final ByteBuffer validity;

    DuckDBReadableVectorImpl(ByteBuffer vectorRef, long rowCount) {
        if (vectorRef == null) {
            throw new DuckDBFunctionException("Invalid vector reference");
        }
        this.vectorRef = vectorRef;
        this.rowCount = rowCount;
        try {
            this.typeInfo = DuckDBVectorTypeInfo.fromVector(vectorRef);
        } catch (java.sql.SQLException exception) {
            throw new DuckDBFunctionException("Failed to resolve vector type info", exception);
        }
        this.data =
            duckdb_vector_get_data(vectorRef, Math.multiplyExact(rowCount, typeInfo.widthBytes)).order(NATIVE_ORDER);
        ByteBuffer validityBuffer = duckdb_vector_get_validity(vectorRef, rowCount);
        this.validity = validityBuffer == null ? null : validityBuffer.order(NATIVE_ORDER);
    }

    @Override
    public DuckDBColumnType getType() {
        return typeInfo.columnType;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public LongStream rowIndexStream() {
        return LongStream.range(0, rowCount);
    }

    @Override
    public boolean isNull(long row) {
        checkRowIndex(row);
        if (validity == null) {
            return false;
        }
        int entryPos = Math.toIntExact(Math.multiplyExact(row / Long.SIZE, (long) Long.BYTES));
        long mask = validity.getLong(entryPos);
        return (mask & (1L << (row % Long.SIZE))) == 0;
    }

    @Override
    public boolean getBoolean(long row) {
        requireType(DuckDBColumnType.BOOLEAN);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.BOOLEAN, row);
        }
        return data.get(checkedRowIndex(row)) != 0;
    }

    @Override
    public boolean getBoolean(long row, boolean defaultVal) {
        requireType(DuckDBColumnType.BOOLEAN);
        return isNull(row) ? defaultVal : data.get(checkedRowIndex(row)) != 0;
    }

    @Override
    public byte getByte(long row) {
        requireType(DuckDBColumnType.TINYINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.TINYINT, row);
        }
        return data.get(checkedRowIndex(row));
    }

    @Override
    public byte getByte(long row, byte defaultVal) {
        requireType(DuckDBColumnType.TINYINT);
        return isNull(row) ? defaultVal : data.get(checkedRowIndex(row));
    }

    @Override
    public short getShort(long row) {
        requireType(DuckDBColumnType.SMALLINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.SMALLINT, row);
        }
        return data.getShort(checkedByteOffset(row, Short.BYTES));
    }

    @Override
    public short getShort(long row, short defaultVal) {
        requireType(DuckDBColumnType.SMALLINT);
        return isNull(row) ? defaultVal : data.getShort(checkedByteOffset(row, Short.BYTES));
    }

    @Override
    public short getUint8(long row) {
        requireType(DuckDBColumnType.UTINYINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.UTINYINT, row);
        }
        return (short) Byte.toUnsignedInt(data.get(checkedRowIndex(row)));
    }

    @Override
    public short getUint8(long row, short defaultVal) {
        requireType(DuckDBColumnType.UTINYINT);
        return isNull(row) ? defaultVal : (short) Byte.toUnsignedInt(data.get(checkedRowIndex(row)));
    }

    @Override
    public int getUint16(long row) {
        requireType(DuckDBColumnType.USMALLINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.USMALLINT, row);
        }
        return Short.toUnsignedInt(data.getShort(checkedByteOffset(row, Short.BYTES)));
    }

    @Override
    public int getUint16(long row, int defaultVal) {
        requireType(DuckDBColumnType.USMALLINT);
        return isNull(row) ? defaultVal : Short.toUnsignedInt(data.getShort(checkedByteOffset(row, Short.BYTES)));
    }

    @Override
    public int getInt(long row) {
        requireType(DuckDBColumnType.INTEGER);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.INTEGER, row);
        }
        return data.getInt(checkedByteOffset(row, Integer.BYTES));
    }

    @Override
    public int getInt(long row, int defaultVal) {
        requireType(DuckDBColumnType.INTEGER);
        return isNull(row) ? defaultVal : data.getInt(checkedByteOffset(row, Integer.BYTES));
    }

    @Override
    public long getUint32(long row) {
        requireType(DuckDBColumnType.UINTEGER);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.UINTEGER, row);
        }
        return Integer.toUnsignedLong(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    @Override
    public long getUint32(long row, long defaultVal) {
        requireType(DuckDBColumnType.UINTEGER);
        return isNull(row) ? defaultVal : Integer.toUnsignedLong(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    @Override
    public long getLong(long row) {
        requireType(DuckDBColumnType.BIGINT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.BIGINT, row);
        }
        return data.getLong(checkedByteOffset(row, Long.BYTES));
    }

    @Override
    public long getLong(long row, long defaultVal) {
        requireType(DuckDBColumnType.BIGINT);
        return isNull(row) ? defaultVal : data.getLong(checkedByteOffset(row, Long.BYTES));
    }

    @Override
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

    @Override
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

    @Override
    public BigInteger getUint64(long row) {
        requireType(DuckDBColumnType.UBIGINT);
        if (isNull(row)) {
            return null;
        }
        long value = data.getLong(checkedByteOffset(row, Long.BYTES));
        return unsignedLongToBigInteger(value);
    }

    @Override
    public float getFloat(long row) {
        requireType(DuckDBColumnType.FLOAT);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.FLOAT, row);
        }
        return data.getFloat(checkedByteOffset(row, Float.BYTES));
    }

    @Override
    public float getFloat(long row, float defaultVal) {
        requireType(DuckDBColumnType.FLOAT);
        return isNull(row) ? defaultVal : data.getFloat(checkedByteOffset(row, Float.BYTES));
    }

    @Override
    public double getDouble(long row) {
        requireType(DuckDBColumnType.DOUBLE);
        if (isNull(row)) {
            throw primitiveNullValue(DuckDBColumnType.DOUBLE, row);
        }
        return data.getDouble(checkedByteOffset(row, Double.BYTES));
    }

    @Override
    public double getDouble(long row, double defaultVal) {
        requireType(DuckDBColumnType.DOUBLE);
        return isNull(row) ? defaultVal : data.getDouble(checkedByteOffset(row, Double.BYTES));
    }

    @Override
    public LocalDate getLocalDate(long row) {
        requireType(DuckDBColumnType.DATE);
        if (isNull(row)) {
            return null;
        }
        return LocalDate.ofEpochDay(data.getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    @Override
    public Date getDate(long row) {
        LocalDate value = getLocalDate(row);
        return value == null ? null : Date.valueOf(value);
    }

    @Override
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
                throw new DuckDBFunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
            }
        } catch (java.sql.SQLException exception) {
            throw new DuckDBFunctionException("Failed to decode timestamp at row " + row, exception);
        }
    }

    @Override
    public Timestamp getTimestamp(long row) {
        LocalDateTime value = getLocalDateTime(row);
        return value == null ? null : Timestamp.valueOf(value);
    }

    @Override
    public OffsetDateTime getOffsetDateTime(long row) {
        requireType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);
        if (isNull(row)) {
            return null;
        }
        long micros = data.getLong(checkedByteOffset(row, Long.BYTES));
        Instant instant = instantFromEpoch(micros, ChronoUnit.MICROS);
        return instant.atZone(ZoneId.systemDefault()).toOffsetDateTime();
    }

    @Override
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
            throw new DuckDBFunctionException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
    }

    @Override
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
            throw new DuckDBFunctionException("Expected vector type " + expected + ", found " + typeInfo.columnType);
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
            throw new DuckDBFunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
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
            throw new DuckDBFunctionException("Unsupported unit type: " + unit);
        }
    }

    private static BigInteger unsignedLongToBigInteger(long value) {
        if (value >= 0) {
            return BigInteger.valueOf(value);
        }
        return BigInteger.valueOf(value & Long.MAX_VALUE).setBit(Long.SIZE - 1);
    }

    private static DuckDBFunctionException primitiveNullValue(DuckDBColumnType type, long row) {
        return new DuckDBFunctionException("Primitive value for " + type + " at row " + row + " is NULL");
    }
}

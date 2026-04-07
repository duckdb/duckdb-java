package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.stream.LongStream;

final class DuckDBReadableVectorImpl implements DuckDBReadableVector {
    private static final BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private final ByteBuffer validity;

    DuckDBReadableVectorImpl(ByteBuffer vectorRef, long rowCount) throws SQLException {
        if (vectorRef == null) {
            throw new SQLException("Invalid vector reference");
        }
        this.vectorRef = vectorRef;
        this.rowCount = rowCount;
        this.typeInfo = DuckDBVectorTypeInfo.fromVector(vectorRef);
        this.data = duckdb_vector_get_data(vectorRef, Math.multiplyExact(rowCount, typeInfo.widthBytes));
        this.validity = duckdb_vector_get_validity(vectorRef, rowCount);
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
        long mask = validity.order(NATIVE_ORDER).getLong(entryPos);
        return (mask & (1L << (row % Long.SIZE))) == 0;
    }

    @Override
    public boolean getBoolean(long row) throws SQLException {
        requireType(DuckDBColumnType.BOOLEAN);
        return data.get(checkedRowIndex(row)) != 0;
    }

    @Override
    public byte getByte(long row) throws SQLException {
        requireType(DuckDBColumnType.TINYINT);
        return data.get(checkedRowIndex(row));
    }

    @Override
    public short getShort(long row) throws SQLException {
        requireType(DuckDBColumnType.SMALLINT);
        return data.order(NATIVE_ORDER).getShort(checkedByteOffset(row, Short.BYTES));
    }

    @Override
    public short getUint8(long row) throws SQLException {
        requireType(DuckDBColumnType.UTINYINT);
        return (short) Byte.toUnsignedInt(data.get(checkedRowIndex(row)));
    }

    @Override
    public int getUint16(long row) throws SQLException {
        requireType(DuckDBColumnType.USMALLINT);
        return Short.toUnsignedInt(data.order(NATIVE_ORDER).getShort(checkedByteOffset(row, Short.BYTES)));
    }

    @Override
    public int getInt(long row) throws SQLException {
        requireType(DuckDBColumnType.INTEGER);
        return data.order(NATIVE_ORDER).getInt(checkedByteOffset(row, Integer.BYTES));
    }

    @Override
    public long getUint32(long row) throws SQLException {
        requireType(DuckDBColumnType.UINTEGER);
        return Integer.toUnsignedLong(data.order(NATIVE_ORDER).getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    @Override
    public long getLong(long row) throws SQLException {
        requireType(DuckDBColumnType.BIGINT);
        return data.order(NATIVE_ORDER).getLong(checkedByteOffset(row, Long.BYTES));
    }

    @Override
    public BigInteger getUint64(long row) throws SQLException {
        requireType(DuckDBColumnType.UBIGINT);
        long value = data.order(NATIVE_ORDER).getLong(checkedByteOffset(row, Long.BYTES));
        return unsignedLongToBigInteger(value);
    }

    @Override
    public float getFloat(long row) throws SQLException {
        requireType(DuckDBColumnType.FLOAT);
        return data.order(NATIVE_ORDER).getFloat(checkedByteOffset(row, Float.BYTES));
    }

    @Override
    public double getDouble(long row) throws SQLException {
        requireType(DuckDBColumnType.DOUBLE);
        return data.order(NATIVE_ORDER).getDouble(checkedByteOffset(row, Double.BYTES));
    }

    @Override
    public LocalDate getLocalDate(long row) throws SQLException {
        requireType(DuckDBColumnType.DATE);
        return LocalDate.ofEpochDay(data.order(NATIVE_ORDER).getInt(checkedByteOffset(row, Integer.BYTES)));
    }

    @Override
    public Date getDate(long row) throws SQLException {
        return Date.valueOf(getLocalDate(row));
    }

    @Override
    public LocalDateTime getLocalDateTime(long row) throws SQLException {
        requireTimestampType();
        long epochValue = data.order(NATIVE_ORDER).getLong(checkedByteOffset(row, Long.BYTES));
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
            throw new SQLException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
        }
    }

    @Override
    public Timestamp getTimestamp(long row) throws SQLException {
        return Timestamp.valueOf(getLocalDateTime(row));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(long row) throws SQLException {
        requireType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);
        long micros = data.order(NATIVE_ORDER).getLong(checkedByteOffset(row, Long.BYTES));
        Instant instant = instantFromEpoch(micros, ChronoUnit.MICROS);
        return instant.atZone(ZoneId.systemDefault()).toOffsetDateTime();
    }

    @Override
    public BigDecimal getBigDecimal(long row) throws SQLException {
        requireType(DuckDBColumnType.DECIMAL);
        switch (typeInfo.storageType) {
        case DUCKDB_TYPE_SMALLINT:
            return BigDecimal.valueOf(data.order(NATIVE_ORDER).getShort(checkedByteOffset(row, Short.BYTES)),
                                      typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_INTEGER:
            return BigDecimal.valueOf(data.order(NATIVE_ORDER).getInt(checkedByteOffset(row, Integer.BYTES)),
                                      typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_BIGINT:
            return BigDecimal.valueOf(data.order(NATIVE_ORDER).getLong(checkedByteOffset(row, Long.BYTES)),
                                      typeInfo.decimalMeta.scale);
        case DUCKDB_TYPE_HUGEINT: {
            ByteBuffer slice = data.duplicate().order(NATIVE_ORDER);
            slice.position(checkedByteOffset(row, typeInfo.widthBytes));
            long lower = slice.getLong();
            long upper = slice.getLong();
            return new BigDecimal(upper)
                .multiply(ULONG_MULTIPLIER)
                .add(new BigDecimal(Long.toUnsignedString(lower)))
                .scaleByPowerOfTen(typeInfo.decimalMeta.scale * -1);
        }
        default:
            throw new SQLException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
    }

    @Override
    public String getString(long row) throws SQLException {
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

    private void requireType(DuckDBColumnType expected) throws SQLException {
        if (typeInfo.columnType != expected) {
            throw new SQLException("Expected vector type " + expected + ", found " + typeInfo.columnType);
        }
    }

    private void requireTimestampType() throws SQLException {
        switch (typeInfo.columnType) {
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return;
        default:
            throw new SQLException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
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

    private static Instant instantFromEpoch(long value, ChronoUnit unit) throws SQLException {
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
            throw new SQLException("Unsupported unit type: " + unit);
        }
    }

    private static BigInteger unsignedLongToBigInteger(long value) {
        if (value >= 0) {
            return BigInteger.valueOf(value);
        }
        return BigInteger.valueOf(value & Long.MAX_VALUE).setBit(Long.SIZE - 1);
    }
}

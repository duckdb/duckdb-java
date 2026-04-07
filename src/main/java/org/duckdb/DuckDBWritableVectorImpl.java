package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

final class DuckDBWritableVectorImpl implements DuckDBWritableVector {
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private ByteBuffer validity;
    private long appendIndex;

    DuckDBWritableVectorImpl(ByteBuffer vectorRef, long rowCount) throws SQLException {
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
    public void addNull() throws SQLException {
        setNull(nextAppendRow());
    }

    @Override
    public void setNull(long row) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        ensureValidity();
        setRowValidity(row, false);
        advanceAppendIndex(row);
    }

    @Override
    public void addBoolean(boolean value) throws SQLException {
        setBoolean(nextAppendRow(), value);
    }

    @Override
    public void setBoolean(long row, boolean value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BOOLEAN);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.put(checkedRowIndex(row), value ? (byte) 1 : (byte) 0);
        markValid(row);
    }

    @Override
    public void addByte(byte value) throws SQLException {
        setByte(nextAppendRow(), value);
    }

    @Override
    public void setByte(long row, byte value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.TINYINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.put(checkedRowIndex(row), value);
        markValid(row);
    }

    @Override
    public void addShort(short value) throws SQLException {
        setShort(nextAppendRow(), value);
    }

    @Override
    public void setShort(long row, short value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.SMALLINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.order(NATIVE_ORDER).putShort(checkedByteOffset(row, Short.BYTES), value);
        markValid(row);
    }

    @Override
    public void addUint8(int value) throws SQLException {
        setUint8(nextAppendRow(), value);
    }

    @Override
    public void setUint8(long row, int value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UTINYINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UTINYINT", value, 0xFFL);
        if (rangeError != null) {
            throw new SQLException(rangeError);
        }
        data.put(checkedRowIndex(row), (byte) value);
        markValid(row);
    }

    @Override
    public void addUint16(int value) throws SQLException {
        setUint16(nextAppendRow(), value);
    }

    @Override
    public void setUint16(long row, int value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.USMALLINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("USMALLINT", value, 0xFFFFL);
        if (rangeError != null) {
            throw new SQLException(rangeError);
        }
        data.order(NATIVE_ORDER).putShort(checkedByteOffset(row, Short.BYTES), (short) value);
        markValid(row);
    }

    @Override
    public void addInt(int value) throws SQLException {
        setInt(nextAppendRow(), value);
    }

    @Override
    public void setInt(long row, int value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.INTEGER);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.order(NATIVE_ORDER).putInt(checkedByteOffset(row, Integer.BYTES), value);
        markValid(row);
    }

    @Override
    public void addUint32(long value) throws SQLException {
        setUint32(nextAppendRow(), value);
    }

    @Override
    public void setUint32(long row, long value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UINTEGER);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UINTEGER", value, 0xFFFFFFFFL);
        if (rangeError != null) {
            throw new SQLException(rangeError);
        }
        data.order(NATIVE_ORDER).putInt(checkedByteOffset(row, Integer.BYTES), (int) value);
        markValid(row);
    }

    @Override
    public void addLong(long value) throws SQLException {
        setLong(nextAppendRow(), value);
    }

    @Override
    public void setLong(long row, long value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BIGINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), value);
        markValid(row);
    }

    @Override
    public void addUint64(BigInteger value) throws SQLException {
        setUint64(nextAppendRow(), value);
    }

    @Override
    public void setUint64(long row, BigInteger value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UBIGINT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value.signum() < 0 || value.compareTo(UINT64_MAX) > 0) {
            throw new SQLException("Value out of range for UBIGINT: " + value);
        }
        data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), value.longValue());
        markValid(row);
    }

    @Override
    public void addFloat(float value) throws SQLException {
        setFloat(nextAppendRow(), value);
    }

    @Override
    public void setFloat(long row, float value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.FLOAT);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.order(NATIVE_ORDER).putFloat(checkedByteOffset(row, Float.BYTES), value);
        markValid(row);
    }

    @Override
    public void addDouble(double value) throws SQLException {
        setDouble(nextAppendRow(), value);
    }

    @Override
    public void setDouble(long row, double value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DOUBLE);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        data.order(NATIVE_ORDER).putDouble(checkedByteOffset(row, Double.BYTES), value);
        markValid(row);
    }

    @Override
    public void addDate(LocalDate value) throws SQLException {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, LocalDate value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DATE);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new SQLException("Value out of range for DATE: " + value);
        }
        data.order(NATIVE_ORDER).putInt(checkedByteOffset(row, Integer.BYTES), (int) days);
        markValid(row);
    }

    @Override
    public void addDate(java.sql.Date value) throws SQLException {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, java.sql.Date value) throws SQLException {
        setDate(row, value == null ? null : value.toLocalDate());
    }

    @Override
    public void addDate(java.util.Date value) throws SQLException {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, java.util.Date value) throws SQLException {
        if (value == null) {
            setNull(row);
            return;
        }
        if (value instanceof java.sql.Date) {
            setDate(row, (java.sql.Date) value);
            return;
        }
        LocalDate localDate = Instant.ofEpochMilli(value.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
        setDate(row, localDate);
    }

    @Override
    public void addTimestamp(LocalDateTime value) throws SQLException {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, LocalDateTime value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), encodeLocalDateTime(value));
        markValid(row);
    }

    @Override
    public void addTimestamp(Timestamp value) throws SQLException {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, Timestamp value) throws SQLException {
        if (value == null) {
            setNull(row);
            return;
        }
        if (typeInfo.columnType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
            String rowError = rowIndexErrorMessage(row);
            if (rowError != null) {
                throw new IndexOutOfBoundsException(rowError);
            }
            data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), encodeInstant(value.toInstant()));
            markValid(row);
            return;
        }
        setTimestamp(row, value.toLocalDateTime());
    }

    @Override
    public void addTimestamp(java.util.Date value) throws SQLException {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, java.util.Date value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value instanceof Timestamp) {
            setTimestamp(row, (Timestamp) value);
            return;
        }
        data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), encodeJavaUtilDate(value));
        markValid(row);
    }

    @Override
    public void addTimestamp(LocalDate value) throws SQLException {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, LocalDate value) throws SQLException {
        if (value == null) {
            setNull(row);
            return;
        }
        if (typeInfo.columnType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
            String rowError = rowIndexErrorMessage(row);
            if (rowError != null) {
                throw new IndexOutOfBoundsException(rowError);
            }
            Instant instant = value.atStartOfDay(ZoneId.systemDefault()).toInstant();
            data.order(NATIVE_ORDER).putLong(checkedByteOffset(row, Long.BYTES), encodeInstant(instant));
            markValid(row);
            return;
        }
        setTimestamp(row, value.atStartOfDay());
    }

    @Override
    public void addOffsetDateTime(OffsetDateTime value) throws SQLException {
        setOffsetDateTime(nextAppendRow(), value);
    }

    @Override
    public void setOffsetDateTime(long row, OffsetDateTime value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(true);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.order(NATIVE_ORDER)
            .putLong(
                checkedByteOffset(row, Long.BYTES),
                DuckDBTimestamp.localDateTime2Micros(value.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()));
        markValid(row);
    }

    @Override
    public void addBigDecimal(BigDecimal value) throws SQLException {
        setBigDecimal(nextAppendRow(), value);
    }

    @Override
    public void setBigDecimal(long row, BigDecimal value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DECIMAL);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        BigDecimal scaled;
        try {
            scaled = value.setScale(typeInfo.decimalMeta.scale);
        } catch (ArithmeticException e) {
            throw decimalOutOfRange(value, e);
        }
        if (scaled.precision() > typeInfo.decimalMeta.width) {
            throw decimalOutOfRange(value);
        }
        switch (typeInfo.storageType) {
        case DUCKDB_TYPE_SMALLINT:
            try {
                data.order(NATIVE_ORDER)
                    .putShort(checkedByteOffset(row, Short.BYTES), scaled.unscaledValue().shortValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_INTEGER:
            try {
                data.order(NATIVE_ORDER)
                    .putInt(checkedByteOffset(row, Integer.BYTES), scaled.unscaledValue().intValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_BIGINT:
            try {
                data.order(NATIVE_ORDER)
                    .putLong(checkedByteOffset(row, Long.BYTES), scaled.unscaledValue().longValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_HUGEINT: {
            BigInteger unscaled = scaled.unscaledValue();
            ByteBuffer slice = data.duplicate().order(NATIVE_ORDER);
            slice.position(checkedByteOffset(row, typeInfo.widthBytes));
            slice.putLong(unscaled.longValue());
            slice.putLong(unscaled.shiftRight(Long.SIZE).longValue());
            break;
        }
        default:
            throw new SQLException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
        markValid(row);
    }

    @Override
    public void addString(String value) throws SQLException {
        setString(nextAppendRow(), value);
    }

    @Override
    public void setString(long row, String value) throws SQLException {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.VARCHAR);
        if (typeError != null) {
            throw new SQLException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        duckdb_vector_assign_string_element_len(vectorRef, row, value.getBytes(UTF_8));
        markValid(row);
    }

    ByteBuffer vectorRef() {
        return vectorRef;
    }

    private void ensureValidity() throws SQLException {
        if (validity != null) {
            return;
        }
        duckdb_vector_ensure_validity_writable(vectorRef);
        validity = duckdb_vector_get_validity(vectorRef, rowCount);
        if (validity == null) {
            throw new SQLException("Cannot initialize vector validity");
        }
    }

    private void markValid(long row) {
        if (validity == null) {
            advanceAppendIndex(row);
            return;
        }
        setRowValidity(row, true);
        advanceAppendIndex(row);
    }

    private void setRowValidity(long row, boolean valid) {
        LongBuffer entries = validity.asLongBuffer();
        int entryIndex = Math.toIntExact(row / Long.SIZE);
        long bitIndex = row % Long.SIZE;
        long mask = 1L << bitIndex;
        long entry = entries.get(entryIndex);
        if (valid) {
            entry |= mask;
        } else {
            entry &= ~mask;
        }
        entries.put(entryIndex, entry);
    }

    private String typeMismatchMessage(DuckDBColumnType expected) {
        if (typeInfo.columnType != expected) {
            return "Expected vector type " + expected + ", found " + typeInfo.columnType;
        }
        return null;
    }

    private String rowIndexErrorMessage(long row) {
        if (row < 0 || row >= rowCount) {
            return "Row index out of bounds: " + row;
        }
        return null;
    }

    private String timestampTypeMismatchMessage(boolean requireTimezone) {
        if (requireTimezone) {
            if (typeInfo.columnType != DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
                return "Expected vector type TIMESTAMP WITH TIME ZONE, found " + typeInfo.columnType;
            }
            return null;
        }
        switch (typeInfo.columnType) {
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return null;
        default:
            return "Expected vector type TIMESTAMP*, found " + typeInfo.columnType;
        }
    }

    private long encodeLocalDateTime(LocalDateTime value) throws SQLException {
        Instant instant;
        if (typeInfo.columnType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
            instant = value.atZone(ZoneId.systemDefault()).toInstant();
        } else {
            instant = value.toInstant(ZoneOffset.UTC);
        }
        return encodeInstant(instant);
    }

    private long encodeJavaUtilDate(java.util.Date value) throws SQLException {
        return encodeInstant(Instant.ofEpochMilli(value.getTime()));
    }

    private long encodeInstant(Instant instant) throws SQLException {
        long epochSeconds = instant.getEpochSecond();
        int nano = instant.getNano();
        switch (typeInfo.capiType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            return epochSeconds;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return Math.addExact(Math.multiplyExact(epochSeconds, 1_000L), nano / 1_000_000L);
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            return Math.addExact(Math.multiplyExact(epochSeconds, 1_000_000L), nano / 1_000L);
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return Math.addExact(Math.multiplyExact(epochSeconds, 1_000_000_000L), nano);
        default:
            throw new SQLException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
        }
    }

    private static String unsignedRangeErrorMessage(String typeName, long value, long maxValue) {
        if (value < 0 || value > maxValue) {
            return "Value out of range for " + typeName + ": " + value;
        }
        return null;
    }

    private SQLException decimalOutOfRange(BigDecimal value) {
        return new SQLException("Value out of range for " + decimalTypeName() + ": " + value);
    }

    private SQLException decimalOutOfRange(BigDecimal value, ArithmeticException cause) {
        SQLException exception = decimalOutOfRange(value);
        exception.initCause(cause);
        return exception;
    }

    private String decimalTypeName() {
        return "DECIMAL(" + typeInfo.decimalMeta.width + "," + typeInfo.decimalMeta.scale + ")";
    }

    private int checkedRowIndex(long row) {
        return Math.toIntExact(row);
    }

    private int checkedByteOffset(long row, int elementWidth) {
        return Math.toIntExact(Math.multiplyExact(row, (long) elementWidth));
    }

    private void advanceAppendIndex(long row) {
        appendIndex = Math.max(appendIndex, Math.addExact(row, 1));
    }

    private long nextAppendRow() {
        if (appendIndex >= rowCount) {
            throw new IndexOutOfBoundsException("Append index out of bounds: " + appendIndex);
        }
        return appendIndex;
    }
}

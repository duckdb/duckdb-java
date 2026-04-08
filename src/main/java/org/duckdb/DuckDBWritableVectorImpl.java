package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

final class DuckDBWritableVectorImpl extends DuckDBWritableVector {
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private ByteBuffer validity;
    private long appendIndex;

    DuckDBWritableVectorImpl(ByteBuffer vectorRef, long rowCount) {
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
        this.data = duckdb_vector_get_data(vectorRef, Math.multiplyExact(rowCount, typeInfo.widthBytes)).order(NATIVE_ORDER);
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
    public void addNull() {
        setNull(nextAppendRow());
    }

    @Override
    public void setNull(long row) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        ensureValidity();
        setRowValidity(row, false);
        advanceAppendIndex(row);
    }

    @Override
    public void addBoolean(boolean value) {
        setBoolean(nextAppendRow(), value);
    }

    @Override
    public void setBoolean(long row, boolean value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BOOLEAN);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.put(checkedRowIndex(row), value ? (byte) 1 : (byte) 0);
        markValid(row);
    }

    @Override
    public void addByte(byte value) {
        setByte(nextAppendRow(), value);
    }

    @Override
    public void setByte(long row, byte value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.TINYINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.put(checkedRowIndex(row), value);
        markValid(row);
    }

    @Override
    public void addShort(short value) {
        setShort(nextAppendRow(), value);
    }

    @Override
    public void setShort(long row, short value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.SMALLINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.putShort(checkedByteOffset(row, Short.BYTES), value);
        markValid(row);
    }

    @Override
    public void addUint8(int value) {
        setUint8(nextAppendRow(), value);
    }

    @Override
    public void setUint8(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UTINYINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UTINYINT", value, 0xFFL);
        if (rangeError != null) {
            throw new DuckDBFunctionException(rangeError);
        }
        data.put(checkedRowIndex(row), (byte) value);
        markValid(row);
    }

    @Override
    public void addUint16(int value) {
        setUint16(nextAppendRow(), value);
    }

    @Override
    public void setUint16(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.USMALLINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("USMALLINT", value, 0xFFFFL);
        if (rangeError != null) {
            throw new DuckDBFunctionException(rangeError);
        }
        data.putShort(checkedByteOffset(row, Short.BYTES), (short) value);
        markValid(row);
    }

    @Override
    public void addInt(int value) {
        setInt(nextAppendRow(), value);
    }

    @Override
    public void setInt(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.INTEGER);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), value);
        markValid(row);
    }

    @Override
    public void addUint32(long value) {
        setUint32(nextAppendRow(), value);
    }

    @Override
    public void setUint32(long row, long value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UINTEGER);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UINTEGER", value, 0xFFFFFFFFL);
        if (rangeError != null) {
            throw new DuckDBFunctionException(rangeError);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), (int) value);
        markValid(row);
    }

    @Override
    public void addLong(long value) {
        setLong(nextAppendRow(), value);
    }

    @Override
    public void setLong(long row, long value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BIGINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), value);
        markValid(row);
    }

    @Override
    public void addHugeInt(BigInteger value) {
        setHugeInt(nextAppendRow(), value);
    }

    @Override
    public void setHugeInt(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.HUGEINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        DuckDBHugeInt hugeInt;
        try {
            hugeInt = new DuckDBHugeInt(value);
        } catch (java.sql.SQLException exception) {
            throw new DuckDBFunctionException("Value out of range for HUGEINT: " + value, exception);
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        data.putLong(offset, hugeInt.lower());
        data.putLong(offset + Long.BYTES, hugeInt.upper());
        markValid(row);
    }

    @Override
    public void addUHugeInt(BigInteger value) {
        setUHugeInt(nextAppendRow(), value);
    }

    @Override
    public void setUHugeInt(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UHUGEINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value.signum() < 0 || value.compareTo(DuckDBHugeInt.UHUGE_INT_MAX) > 0) {
            throw new DuckDBFunctionException("Value out of range for UHUGEINT: " + value);
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        data.putLong(offset, value.longValue());
        data.putLong(offset + Long.BYTES, value.shiftRight(Long.SIZE).longValue());
        markValid(row);
    }

    @Override
    public void addUint64(BigInteger value) {
        setUint64(nextAppendRow(), value);
    }

    @Override
    public void setUint64(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UBIGINT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value.signum() < 0 || value.compareTo(UINT64_MAX) > 0) {
            throw new DuckDBFunctionException("Value out of range for UBIGINT: " + value);
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), value.longValue());
        markValid(row);
    }

    @Override
    public void addFloat(float value) {
        setFloat(nextAppendRow(), value);
    }

    @Override
    public void setFloat(long row, float value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.FLOAT);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.putFloat(checkedByteOffset(row, Float.BYTES), value);
        markValid(row);
    }

    @Override
    public void addDouble(double value) {
        setDouble(nextAppendRow(), value);
    }

    @Override
    public void setDouble(long row, double value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DOUBLE);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        data.putDouble(checkedByteOffset(row, Double.BYTES), value);
        markValid(row);
    }

    @Override
    public void addDate(LocalDate value) {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, LocalDate value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DATE);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new DuckDBFunctionException("Value out of range for DATE: " + value);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), (int) days);
        markValid(row);
    }

    @Override
    public void addDate(java.sql.Date value) {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, java.sql.Date value) {
        setDate(row, value == null ? null : value.toLocalDate());
    }

    @Override
    public void addDate(java.util.Date value) {
        setDate(nextAppendRow(), value);
    }

    @Override
    public void setDate(long row, java.util.Date value) {
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
    public void addTimestamp(LocalDateTime value) {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, LocalDateTime value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), encodeLocalDateTime(value));
        markValid(row);
    }

    @Override
    public void addTimestamp(Timestamp value) {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, Timestamp value) {
        if (value == null) {
            setNull(row);
            return;
        }
        if (typeInfo.columnType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
            String rowError = rowIndexErrorMessage(row);
            if (rowError != null) {
                throw new IndexOutOfBoundsException(rowError);
            }
            data.putLong(checkedByteOffset(row, Long.BYTES), encodeInstant(value.toInstant()));
            markValid(row);
            return;
        }
        setTimestamp(row, value.toLocalDateTime());
    }

    @Override
    public void addTimestamp(java.util.Date value) {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, java.util.Date value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value instanceof Timestamp) {
            setTimestamp(row, (Timestamp) value);
            return;
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), encodeJavaUtilDate(value));
        markValid(row);
    }

    @Override
    public void addTimestamp(LocalDate value) {
        setTimestamp(nextAppendRow(), value);
    }

    @Override
    public void setTimestamp(long row, LocalDate value) {
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
            data.putLong(checkedByteOffset(row, Long.BYTES), encodeInstant(instant));
            markValid(row);
            return;
        }
        setTimestamp(row, value.atStartOfDay());
    }

    @Override
    public void addOffsetDateTime(OffsetDateTime value) {
        setOffsetDateTime(nextAppendRow(), value);
    }

    @Override
    public void setOffsetDateTime(long row, OffsetDateTime value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(true);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.putLong(checkedByteOffset(row, Long.BYTES),
                     DuckDBTimestamp.localDateTime2Micros(value.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()));
        markValid(row);
    }

    @Override
    public void addBigDecimal(BigDecimal value) {
        setBigDecimal(nextAppendRow(), value);
    }

    @Override
    public void setBigDecimal(long row, BigDecimal value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DECIMAL);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
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
                data.putShort(checkedByteOffset(row, Short.BYTES), scaled.unscaledValue().shortValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_INTEGER:
            try {
                data.putInt(checkedByteOffset(row, Integer.BYTES), scaled.unscaledValue().intValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_BIGINT:
            try {
                data.putLong(checkedByteOffset(row, Long.BYTES), scaled.unscaledValue().longValueExact());
            } catch (ArithmeticException e) {
                throw decimalOutOfRange(value, e);
            }
            break;
        case DUCKDB_TYPE_HUGEINT: {
            BigInteger unscaled = scaled.unscaledValue();
            int offset = checkedByteOffset(row, typeInfo.widthBytes);
            data.putLong(offset, unscaled.longValue());
            data.putLong(offset + Long.BYTES, unscaled.shiftRight(Long.SIZE).longValue());
            break;
        }
        default:
            throw new DuckDBFunctionException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
        markValid(row);
    }

    @Override
    public void addString(String value) {
        setString(nextAppendRow(), value);
    }

    @Override
    public void setString(long row, String value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.VARCHAR);
        if (typeError != null) {
            throw new DuckDBFunctionException(typeError);
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

    private void ensureValidity() {
        if (validity != null) {
            return;
        }
        duckdb_vector_ensure_validity_writable(vectorRef);
        validity = duckdb_vector_get_validity(vectorRef, rowCount);
        if (validity == null) {
            throw new DuckDBFunctionException("Cannot initialize vector validity");
        }
        validity = validity.order(NATIVE_ORDER);
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
        int entryOffset = Math.toIntExact(Math.multiplyExact(row / Long.SIZE, (long) Long.BYTES));
        long bitIndex = row % Long.SIZE;
        long mask = 1L << bitIndex;
        long entry = validity.getLong(entryOffset);
        if (valid) {
            entry |= mask;
        } else {
            entry &= ~mask;
        }
        validity.putLong(entryOffset, entry);
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

    private long encodeLocalDateTime(LocalDateTime value) {
        Instant instant;
        if (typeInfo.columnType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
            instant = value.atZone(ZoneId.systemDefault()).toInstant();
        } else {
            instant = value.toInstant(ZoneOffset.UTC);
        }
        return encodeInstant(instant);
    }

    private long encodeJavaUtilDate(java.util.Date value) {
        return encodeInstant(Instant.ofEpochMilli(value.getTime()));
    }

    private long encodeInstant(Instant instant) {
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
            throw new DuckDBFunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
        }
    }

    private static String unsignedRangeErrorMessage(String typeName, long value, long maxValue) {
        if (value < 0 || value > maxValue) {
            return "Value out of range for " + typeName + ": " + value;
        }
        return null;
    }

    private DuckDBFunctionException decimalOutOfRange(BigDecimal value) {
        return new DuckDBFunctionException("Value out of range for " + decimalTypeName() + ": " + value);
    }

    private DuckDBFunctionException decimalOutOfRange(BigDecimal value, ArithmeticException cause) {
        DuckDBFunctionException exception = decimalOutOfRange(value);
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

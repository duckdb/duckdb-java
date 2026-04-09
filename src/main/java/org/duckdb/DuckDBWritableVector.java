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
import org.duckdb.DuckDBFunctions.FunctionException;

public final class DuckDBWritableVector {
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private final ByteBuffer vectorRef;
    private final long rowCount;
    private final DuckDBVectorTypeInfo typeInfo;
    private final ByteBuffer data;
    private final ByteBuffer validity;

    DuckDBWritableVector(ByteBuffer vectorRef, long rowCount) {
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
        duckdb_vector_ensure_validity_writable(vectorRef);
        this.validity = duckdb_vector_get_validity(vectorRef, rowCount);
        this.validity.order(NATIVE_ORDER);
    }

    public DuckDBColumnType getType() {
        return typeInfo.columnType;
    }

    public long rowCount() {
        return rowCount;
    }

    public void setNull(long row) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        setRowValidity(row, false);
    }

    public void setBoolean(long row, boolean value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BOOLEAN);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.put(checkedRowIndex(row), value ? (byte) 1 : (byte) 0);
        markValid(row);
    }

    public void setByte(long row, byte value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.TINYINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.put(checkedRowIndex(row), value);
        markValid(row);
    }

    public void setShort(long row, short value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.SMALLINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.putShort(checkedByteOffset(row, Short.BYTES), value);
        markValid(row);
    }

    public void setUint8(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UTINYINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UTINYINT", value, 0xFFL);
        if (rangeError != null) {
            throw new FunctionException(rangeError);
        }
        data.put(checkedRowIndex(row), (byte) value);
        markValid(row);
    }

    public void setUint16(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.USMALLINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("USMALLINT", value, 0xFFFFL);
        if (rangeError != null) {
            throw new FunctionException(rangeError);
        }
        data.putShort(checkedByteOffset(row, Short.BYTES), (short) value);
        markValid(row);
    }

    public void setInt(long row, int value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.INTEGER);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), value);
        markValid(row);
    }

    public void setUint32(long row, long value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UINTEGER);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        String rangeError = unsignedRangeErrorMessage("UINTEGER", value, 0xFFFFFFFFL);
        if (rangeError != null) {
            throw new FunctionException(rangeError);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), (int) value);
        markValid(row);
    }

    public void setLong(long row, long value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.BIGINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), value);
        markValid(row);
    }

    public void setHugeInt(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.HUGEINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        DuckDBHugeInt hugeInt;
        try {
            hugeInt = new DuckDBHugeInt(value);
        } catch (java.sql.SQLException exception) {
            throw new FunctionException("Value out of range for HUGEINT: " + value, exception);
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        data.putLong(offset, hugeInt.lower());
        data.putLong(offset + Long.BYTES, hugeInt.upper());
        markValid(row);
    }

    public void setUHugeInt(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UHUGEINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value.signum() < 0 || value.compareTo(DuckDBHugeInt.UHUGE_INT_MAX) > 0) {
            throw new FunctionException("Value out of range for UHUGEINT: " + value);
        }
        int offset = checkedByteOffset(row, typeInfo.widthBytes);
        data.putLong(offset, value.longValue());
        data.putLong(offset + Long.BYTES, value.shiftRight(Long.SIZE).longValue());
        markValid(row);
    }

    public void setUint64(long row, BigInteger value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.UBIGINT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        if (value.signum() < 0 || value.compareTo(UINT64_MAX) > 0) {
            throw new FunctionException("Value out of range for UBIGINT: " + value);
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), value.longValue());
        markValid(row);
    }

    public void setFloat(long row, float value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.FLOAT);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.putFloat(checkedByteOffset(row, Float.BYTES), value);
        markValid(row);
    }

    public void setDouble(long row, double value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DOUBLE);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        data.putDouble(checkedByteOffset(row, Double.BYTES), value);
        markValid(row);
    }

    public void setDate(long row, LocalDate value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DATE);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new FunctionException("Value out of range for DATE: " + value);
        }
        data.putInt(checkedByteOffset(row, Integer.BYTES), (int) days);
        markValid(row);
    }

    public void setDate(long row, java.sql.Date value) {
        setDate(row, value == null ? null : value.toLocalDate());
    }

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

    public void setTimestamp(long row, LocalDateTime value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.putLong(checkedByteOffset(row, Long.BYTES), encodeLocalDateTime(value));
        markValid(row);
    }

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

    public void setTimestamp(long row, java.util.Date value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(false);
        if (typeError != null) {
            throw new FunctionException(typeError);
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

    public void setOffsetDateTime(long row, OffsetDateTime value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = timestampTypeMismatchMessage(true);
        if (typeError != null) {
            throw new FunctionException(typeError);
        }
        if (value == null) {
            setNull(row);
            return;
        }
        data.putLong(
            checkedByteOffset(row, Long.BYTES),
            DuckDBTimestamp.localDateTime2Micros(value.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()));
        markValid(row);
    }

    public void setBigDecimal(long row, BigDecimal value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.DECIMAL);
        if (typeError != null) {
            throw new FunctionException(typeError);
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
            throw new FunctionException("Unsupported DECIMAL storage type: " + typeInfo.storageType);
        }
        markValid(row);
    }

    public void setString(long row, String value) {
        String rowError = rowIndexErrorMessage(row);
        if (rowError != null) {
            throw new IndexOutOfBoundsException(rowError);
        }
        String typeError = typeMismatchMessage(DuckDBColumnType.VARCHAR);
        if (typeError != null) {
            throw new FunctionException(typeError);
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

    private void markValid(long row) {
        setRowValidity(row, true);
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
            throw new FunctionException("Expected vector type TIMESTAMP*, found " + typeInfo.columnType);
        }
    }

    private static String unsignedRangeErrorMessage(String typeName, long value, long maxValue) {
        if (value < 0 || value > maxValue) {
            return "Value out of range for " + typeName + ": " + value;
        }
        return null;
    }

    private FunctionException decimalOutOfRange(BigDecimal value) {
        return new FunctionException("Value out of range for " + decimalTypeName() + ": " + value);
    }

    private FunctionException decimalOutOfRange(BigDecimal value, ArithmeticException cause) {
        FunctionException exception = decimalOutOfRange(value);
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
}

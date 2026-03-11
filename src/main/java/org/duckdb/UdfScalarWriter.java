package org.duckdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class UdfScalarWriter {
    private static final long UNSIGNED_INT_MAX = 0xFFFF_FFFFL;
    private static final LocalDateTime EPOCH_DATE_TIME = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
    private static final int MAX_TZ_SECONDS = 16 * 60 * 60 - 1;

    private final DuckDBColumnType type;
    private final ByteBuffer data;
    private final ByteBuffer vectorRef;
    private final ByteBuffer validity;
    private final int rowCount;

    public UdfScalarWriter(int capiTypeId, ByteBuffer data, ByteBuffer vectorRef, ByteBuffer validity, int rowCount) {
        this(resolveType(capiTypeId), data, vectorRef, validity, rowCount);
    }

    private UdfScalarWriter(DuckDBColumnType type, ByteBuffer data, ByteBuffer vectorRef, ByteBuffer validity,
                            int rowCount) {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (!UdfTypeCatalog.isScalarUdfImplemented(type)) {
            throw new IllegalArgumentException("Unsupported scalar UDF output type: " + type);
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (UdfTypeCatalog.requiresVectorRef(type)) {
            if (vectorRef == null) {
                throw new IllegalArgumentException("vectorRef is required for vectors backed by native accessors");
            }
        } else if (data == null) {
            throw new IllegalArgumentException("data is required for fixed-size vectors");
        }

        this.type = type;
        this.data = data == null ? null : data.order(ByteOrder.nativeOrder());
        this.vectorRef = vectorRef;
        this.validity = validity;
        this.rowCount = rowCount;
    }

    private static DuckDBColumnType resolveType(int capiTypeId) {
        try {
            return UdfTypeCatalog.fromCapiTypeId(capiTypeId);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Unsupported scalar UDF C API type id: " + capiTypeId, e);
        }
    }

    private void checkIndex(int row) {
        if (row < 0 || row >= rowCount) {
            throw new IndexOutOfBoundsException("row=" + row + ", rowCount=" + rowCount);
        }
    }

    private void requireAccessor(UdfTypeCatalog.Accessor accessor, String method) {
        if (!UdfTypeCatalog.supportsAccessor(type, accessor)) {
            throw new UnsupportedOperationException(method + " is not supported for " + type + " vectors");
        }
    }

    private void markValid(int row) {
        if (validity == null) {
            return;
        }
        int byteIndex = row / 8;
        int bitIndex = row % 8;
        int current = validity.get(byteIndex) & 0xFF;
        validity.put(byteIndex, (byte) (current | (1 << bitIndex)));
    }

    private int fixedWidthBytesForByteAccessor() {
        switch (type) {
        case HUGEINT:
        case UHUGEINT:
        case UUID:
            return 16;
        default:
            throw new IllegalStateException("Unexpected type for byte accessor: " + type);
        }
    }

    private byte[] readFixedWidthBytes(int row) {
        int width = fixedWidthBytesForByteAccessor();
        byte[] value = new byte[width];
        ByteBuffer buffer = data.duplicate();
        buffer.position(row * width);
        buffer.get(value);
        return value;
    }

    private void writeFixedWidthBytes(int row, byte[] value) {
        int width = fixedWidthBytesForByteAccessor();
        if (value.length != width) {
            throw new IllegalArgumentException("Expected " + width + " bytes for " + type + " value, got " +
                                               value.length);
        }
        ByteBuffer buffer = data.duplicate();
        buffer.position(row * width);
        buffer.put(value);
    }

    public DuckDBColumnType getType() {
        return type;
    }

    public boolean isNull(int row) {
        checkIndex(row);
        if (validity == null) {
            return false;
        }
        int byteIndex = row / 8;
        int bitIndex = row % 8;
        int mask = 1 << bitIndex;
        return (validity.get(byteIndex) & mask) == 0;
    }

    public void setNull(int row) {
        checkIndex(row);
        if (validity == null) {
            throw new UnsupportedOperationException("setNull requires a writable validity buffer");
        }
        int byteIndex = row / 8;
        int bitIndex = row % 8;
        int mask = ~(1 << bitIndex);
        int current = validity.get(byteIndex) & 0xFF;
        validity.put(byteIndex, (byte) (current & mask));
    }

    public int getInt(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_INT, "getInt");
        switch (type) {
        case TINYINT:
            return data.get(row);
        case UTINYINT:
            return Byte.toUnsignedInt(data.get(row));
        case SMALLINT:
            return data.getShort(row * Short.BYTES);
        case USMALLINT:
            return Short.toUnsignedInt(data.getShort(row * Short.BYTES));
        case INTEGER:
        case DATE:
            return data.getInt(row * Integer.BYTES);
        default:
            throw new IllegalStateException("Unexpected type for getInt: " + type);
        }
    }

    public long getLong(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_LONG, "getLong");
        switch (type) {
        case BIGINT:
        case UBIGINT:
        case TIME:
        case TIME_NS:
        case TIME_WITH_TIME_ZONE:
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return data.getLong(row * Long.BYTES);
        case UINTEGER:
            return Integer.toUnsignedLong(data.getInt(row * Integer.BYTES));
        default:
            throw new IllegalStateException("Unexpected type for getLong: " + type);
        }
    }

    public float getFloat(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_FLOAT, "getFloat");
        return data.getFloat(row * Float.BYTES);
    }

    public double getDouble(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_DOUBLE, "getDouble");
        return data.getDouble(row * Double.BYTES);
    }

    public BigDecimal getBigDecimal(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_DECIMAL, "getBigDecimal");
        if (isNull(row)) {
            return null;
        }
        try {
            return UdfNative.getDecimal(vectorRef, row);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Date getDate(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case DATE:
            return java.sql.Date.valueOf(getLocalDate(row));
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime localDateTime = getLocalDateTime(row);
            Timestamp dayTimestamp = Timestamp.valueOf(localDateTime.truncatedTo(ChronoUnit.DAYS));
            return new Date(dayTimestamp.getTime());
        }
        default:
            throw new UnsupportedOperationException("getDate is not supported for " + type + " vectors");
        }
    }

    public LocalDate getLocalDate(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case DATE:
            return LocalDate.ofEpochDay(getInt(row));
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return getLocalDateTime(row).toLocalDate();
        default:
            throw new UnsupportedOperationException("getLocalDate is not supported for " + type + " vectors");
        }
    }

    public LocalTime getLocalTime(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case TIME:
            return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(getLong(row)));
        case TIME_NS:
            return LocalTime.ofNanoOfDay(getLong(row));
        case TIME_WITH_TIME_ZONE:
            return getOffsetTime(row).toLocalTime();
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return getLocalDateTime(row).toLocalTime();
        default:
            throw new UnsupportedOperationException("getLocalTime is not supported for " + type + " vectors");
        }
    }

    public OffsetTime getOffsetTime(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case TIME:
        case TIME_NS:
            return getLocalTime(row).atOffset(ZoneOffset.UTC);
        case TIME_WITH_TIME_ZONE:
            return DuckDBTimestamp.toOffsetTime(getLong(row));
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return getOffsetDateTime(row).toOffsetTime();
        default:
            throw new UnsupportedOperationException("getOffsetTime is not supported for " + type + " vectors");
        }
    }

    public LocalDateTime getLocalDateTime(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case DATE:
            return LocalDate.ofEpochDay(getInt(row)).atStartOfDay();
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            return timestampToLocalDateTime(row);
        default:
            throw new UnsupportedOperationException("getLocalDateTime is not supported for " + type + " vectors");
        }
    }

    public OffsetDateTime getOffsetDateTime(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        switch (type) {
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime localDateTime = getLocalDateTime(row);
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
            ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(instant);
            return localDateTime.atOffset(zoneOffset);
        }
        default:
            throw new UnsupportedOperationException("getOffsetDateTime is not supported for " + type + " vectors");
        }
    }

    public UUID getUUID(int row) {
        checkIndex(row);
        if (isNull(row)) {
            return null;
        }
        if (type != DuckDBColumnType.UUID) {
            throw new UnsupportedOperationException("getUUID is not supported for " + type + " vectors");
        }
        byte[] uuidBytes = getBytes(row);
        ByteBuffer buffer = ByteBuffer.wrap(uuidBytes).order(ByteOrder.nativeOrder());
        long leastSignificantBits = buffer.getLong();
        long mostSignificantBits = buffer.getLong() ^ Long.MIN_VALUE;
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    private LocalDateTime timestampToLocalDateTime(int row) {
        try {
            long value = getLong(row);
            switch (type) {
            case TIMESTAMP:
                return DuckDBTimestamp.localDateTimeFromTimestamp(value, ChronoUnit.MICROS, null);
            case TIMESTAMP_MS:
                return DuckDBTimestamp.localDateTimeFromTimestamp(value, ChronoUnit.MILLIS, null);
            case TIMESTAMP_NS:
                return DuckDBTimestamp.localDateTimeFromTimestamp(value, ChronoUnit.NANOS, null);
            case TIMESTAMP_S:
                return DuckDBTimestamp.localDateTimeFromTimestamp(value, ChronoUnit.SECONDS, null);
            case TIMESTAMP_WITH_TIME_ZONE:
                return DuckDBTimestamp.localDateTimeFromTimestampWithTimezone(value, ChronoUnit.MICROS, null);
            default:
                throw new UnsupportedOperationException("timestampToLocalDateTime is not supported for " + type +
                                                        " vectors");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean getBoolean(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_BOOLEAN, "getBoolean");
        return data.get(row) != 0;
    }

    public String getString(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_STRING, "getString");
        if (isNull(row)) {
            return null;
        }
        try {
            return UdfNative.getVarchar(vectorRef, row);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getBytes(int row) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.GET_BYTES, "getBytes");
        if (isNull(row)) {
            return null;
        }
        if (type == DuckDBColumnType.BLOB) {
            try {
                return UdfNative.getBlob(vectorRef, row);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return readFixedWidthBytes(row);
    }

    public void setInt(int row, int value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_INT, "setInt");
        switch (type) {
        case TINYINT:
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("Value out of range for TINYINT: " + value);
            }
            data.put(row, (byte) value);
            break;
        case UTINYINT:
            if (value < 0 || value > 0xFF) {
                throw new IllegalArgumentException("Value out of range for UTINYINT: " + value);
            }
            data.put(row, (byte) value);
            break;
        case SMALLINT:
            if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                throw new IllegalArgumentException("Value out of range for SMALLINT: " + value);
            }
            data.putShort(row * Short.BYTES, (short) value);
            break;
        case USMALLINT:
            if (value < 0 || value > 0xFFFF) {
                throw new IllegalArgumentException("Value out of range for USMALLINT: " + value);
            }
            data.putShort(row * Short.BYTES, (short) value);
            break;
        case INTEGER:
        case DATE:
            data.putInt(row * Integer.BYTES, value);
            break;
        default:
            throw new IllegalStateException("Unexpected type for setInt: " + type);
        }
        markValid(row);
    }

    public void setLong(int row, long value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_LONG, "setLong");
        switch (type) {
        case BIGINT:
        case UBIGINT:
        case TIME:
        case TIME_NS:
        case TIME_WITH_TIME_ZONE:
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            data.putLong(row * Long.BYTES, value);
            break;
        case UINTEGER:
            if (value < 0 || value > UNSIGNED_INT_MAX) {
                throw new IllegalArgumentException("Value out of range for UINTEGER: " + value);
            }
            data.putInt(row * Integer.BYTES, (int) value);
            break;
        default:
            throw new IllegalStateException("Unexpected type for setLong: " + type);
        }
        markValid(row);
    }

    public void setFloat(int row, float value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_FLOAT, "setFloat");
        data.putFloat(row * Float.BYTES, value);
        markValid(row);
    }

    public void setDouble(int row, double value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_DOUBLE, "setDouble");
        data.putDouble(row * Double.BYTES, value);
        markValid(row);
    }

    public void setBigDecimal(int row, BigDecimal value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_DECIMAL, "setBigDecimal");
        if (value == null) {
            setNull(row);
            return;
        }
        try {
            UdfNative.setDecimal(vectorRef, row, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        markValid(row);
    }

    public void setBoolean(int row, boolean value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_BOOLEAN, "setBoolean");
        data.put(row, (byte) (value ? 1 : 0));
        markValid(row);
    }

    public void setString(int row, String value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_STRING, "setString");
        if (value == null) {
            setNull(row);
            return;
        }
        try {
            UdfNative.setVarchar(vectorRef, row, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        markValid(row);
    }

    public void setBytes(int row, byte[] value) {
        checkIndex(row);
        requireAccessor(UdfTypeCatalog.Accessor.SET_BYTES, "setBytes");
        if (value == null) {
            setNull(row);
            return;
        }
        if (type == DuckDBColumnType.BLOB) {
            try {
                UdfNative.setBlob(vectorRef, row, value);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            writeFixedWidthBytes(row, value);
        }
        markValid(row);
    }

    public void setObject(int row, Object value) {
        checkIndex(row);
        if (value == null) {
            setNull(row);
            return;
        }
        switch (type) {
        case BOOLEAN:
            setBoolean(row, requireBoolean(value));
            return;
        case TINYINT:
            setInt(row, (int) requireSignedLongInRange(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT"));
            return;
        case UTINYINT:
            setInt(row, (int) requireSignedLongInRange(value, 0, 0xFFL, "UTINYINT"));
            return;
        case SMALLINT:
            setInt(row, (int) requireSignedLongInRange(value, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT"));
            return;
        case USMALLINT:
            setInt(row, (int) requireSignedLongInRange(value, 0, 0xFFFFL, "USMALLINT"));
            return;
        case INTEGER:
            setInt(row, (int) requireSignedLongInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER"));
            return;
        case DATE:
            setInt(row, requireDateEpochDays(value));
            return;
        case UINTEGER:
            setLong(row, requireSignedLongInRange(value, 0, UNSIGNED_INT_MAX, "UINTEGER"));
            return;
        case BIGINT:
        case UBIGINT:
        case TIME:
        case TIME_NS:
        case TIME_WITH_TIME_ZONE:
        case TIMESTAMP:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_WITH_TIME_ZONE:
            setLong(row, requireLongOrTemporal(value, type));
            return;
        case FLOAT:
            setFloat(row, (float) requireDouble(value));
            return;
        case DOUBLE:
            setDouble(row, requireDouble(value));
            return;
        case DECIMAL:
            setBigDecimal(row, requireBigDecimal(value));
            return;
        case VARCHAR:
            setString(row, requireString(value));
            return;
        case BLOB:
            setBytes(row, requireBytes(value));
            return;
        case HUGEINT:
        case UHUGEINT:
            setBytes(row, requireFixedWidthBytes(value, 16, type.toString()));
            return;
        case UUID:
            if (value instanceof UUID) {
                setBytes(row, uuidToBytes((UUID) value));
            } else {
                setBytes(row, requireFixedWidthBytes(value, 16, "UUID"));
            }
            return;
        default:
            throw new IllegalArgumentException("Unsupported output type for setObject: " + type);
        }
    }

    public void setLocalDate(int row, LocalDate value) {
        setObject(row, value);
    }

    public void setLocalTime(int row, LocalTime value) {
        setObject(row, value);
    }

    public void setOffsetTime(int row, OffsetTime value) {
        setObject(row, value);
    }

    public void setLocalDateTime(int row, LocalDateTime value) {
        setObject(row, value);
    }

    public void setDate(int row, Date value) {
        setObject(row, value);
    }

    public void setOffsetDateTime(int row, OffsetDateTime value) {
        setObject(row, value);
    }

    public void setUUID(int row, UUID value) {
        setObject(row, value);
    }

    private static boolean requireBoolean(Object value) {
        if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("Expected Boolean value but got " + value.getClass().getName());
        }
        return (Boolean) value;
    }

    private static long requireLong(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Expected numeric value but got " + value.getClass().getName());
        }
        return ((Number) value).longValue();
    }

    private static double requireDouble(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Expected numeric value but got " + value.getClass().getName());
        }
        return ((Number) value).doubleValue();
    }

    private static String requireString(Object value) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Expected String value but got " + value.getClass().getName());
        }
        return (String) value;
    }

    private static byte[] requireBytes(Object value) {
        if (!(value instanceof byte[])) {
            throw new IllegalArgumentException("Expected byte[] value but got " + value.getClass().getName());
        }
        return (byte[]) value;
    }

    private static byte[] requireFixedWidthBytes(Object value, int width, String typeName) {
        byte[] bytes = requireBytes(value);
        if (bytes.length != width) {
            throw new IllegalArgumentException("Expected " + width + " bytes for " + typeName + " value, got " +
                                               bytes.length);
        }
        return bytes;
    }

    private static BigDecimal requireBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        throw new IllegalArgumentException("Expected BigDecimal/Number value but got " + value.getClass().getName());
    }

    private static long requireSignedLongInRange(Object value, long min, long max, String typeName) {
        long num = requireLong(value);
        if (num < min || num > max) {
            throw new IllegalArgumentException("Value out of range for " + typeName + ": " + num);
        }
        return num;
    }

    private static int requireDateEpochDays(Object value) {
        if (value instanceof LocalDate) {
            long days = ((LocalDate) value).toEpochDay();
            if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Expected LocalDate epoch day to fit int32 but got " + days);
            }
            return (int) days;
        }
        if (value instanceof Date) {
            LocalDate localDate = Instant.ofEpochMilli(((Date) value).getTime()).atOffset(ZoneOffset.UTC).toLocalDate();
            long days = localDate.toEpochDay();
            if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Expected Date epoch day to fit int32 but got " + days);
            }
            return (int) days;
        }
        return (int) requireSignedLongInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, "DATE");
    }

    private static long requireLongOrTemporal(Object value, DuckDBColumnType colType) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        switch (colType) {
        case TIME:
            if (value instanceof LocalTime) {
                return ((LocalTime) value).toNanoOfDay() / 1000L;
            }
            break;
        case TIME_NS:
            if (value instanceof LocalTime) {
                return ((LocalTime) value).toNanoOfDay();
            }
            break;
        case TIME_WITH_TIME_ZONE:
            if (value instanceof OffsetTime) {
                OffsetTime time = (OffsetTime) value;
                return packTimeTzMicros(time.toLocalTime().toNanoOfDay() / 1000L, time.getOffset().getTotalSeconds());
            }
            break;
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP:
        case TIMESTAMP_NS:
            if (value instanceof LocalDateTime) {
                return localDateTimeToMoment((LocalDateTime) value, colType);
            }
            if (value instanceof Date) {
                return dateToMoment((Date) value, colType);
            }
            break;
        case TIMESTAMP_WITH_TIME_ZONE:
            if (value instanceof OffsetDateTime) {
                LocalDateTime utcDateTime =
                    ((OffsetDateTime) value).atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
                return EPOCH_DATE_TIME.until(utcDateTime, ChronoUnit.MICROS);
            }
            if (value instanceof Date) {
                return Math.multiplyExact(((Date) value).getTime(), 1000L);
            }
            break;
        default:
            break;
        }
        throw new IllegalArgumentException("Expected numeric/temporal value compatible with " + colType + " but got " +
                                           value.getClass().getName());
    }

    private static long localDateTimeToMoment(LocalDateTime value, DuckDBColumnType colType) {
        switch (colType) {
        case TIMESTAMP_S:
            return EPOCH_DATE_TIME.until(value, ChronoUnit.SECONDS);
        case TIMESTAMP_MS:
            return EPOCH_DATE_TIME.until(value, ChronoUnit.MILLIS);
        case TIMESTAMP:
            return EPOCH_DATE_TIME.until(value, ChronoUnit.MICROS);
        case TIMESTAMP_NS:
            return EPOCH_DATE_TIME.until(value, ChronoUnit.NANOS);
        default:
            throw new IllegalArgumentException("Unsupported LocalDateTime conversion for " + colType);
        }
    }

    private static long dateToMoment(Date value, DuckDBColumnType colType) {
        long millis = value.getTime();
        switch (colType) {
        case TIMESTAMP_S:
            return millis / 1000L;
        case TIMESTAMP_MS:
            return millis;
        case TIMESTAMP:
            return Math.multiplyExact(millis, 1000L);
        case TIMESTAMP_NS:
            return Math.multiplyExact(millis, 1000000L);
        default:
            throw new IllegalArgumentException("Unsupported Date conversion for " + colType);
        }
    }

    private static long packTimeTzMicros(long micros, int offsetSeconds) {
        long normalizedOffset = MAX_TZ_SECONDS - offsetSeconds;
        return ((micros & 0xFFFFFFFFFFL) << 24) | (normalizedOffset & 0xFFFFFFL);
    }

    private static byte[] uuidToBytes(UUID value) {
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.nativeOrder());
        buffer.putLong(value.getLeastSignificantBits());
        buffer.putLong(value.getMostSignificantBits() ^ Long.MIN_VALUE);
        return buffer.array();
    }
}

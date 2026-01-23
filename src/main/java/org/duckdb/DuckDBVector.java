package org.duckdb;

import static java.time.temporal.ChronoUnit.*;
import static org.duckdb.DuckDBTimestamp.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

class DuckDBVector {
    // Constant to construct BigDecimals from hugeint_t
    private final static BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");
    private final static DateTimeFormatter ERA_FORMAT =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR_OF_ERA)
            .appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR)
            .appendLiteral("-")
            .appendValue(ChronoField.DAY_OF_MONTH)
            .appendOptional(new DateTimeFormatterBuilder()
                                .appendLiteral(" (")
                                .appendText(ChronoField.ERA, TextStyle.SHORT)
                                .appendLiteral(")")
                                .toFormatter())
            .toFormatter();

    private final DuckDBColumnTypeMetaData meta;
    protected final DuckDBColumnType duckdb_type;
    final int length;
    private final boolean[] nullmask;
    private ByteBuffer constlen_data = null;
    private Object[] varlen_data = null;
    String[] string_data = null;

    DuckDBVector(String duckdb_type, int length, boolean[] nullmask) {
        super();
        this.duckdb_type = DuckDBResultSetMetaData.TypeNameToType(duckdb_type);
        this.meta = this.duckdb_type == DuckDBColumnType.DECIMAL
                        ? DuckDBColumnTypeMetaData.parseColumnTypeMetadata(duckdb_type)
                        : null;
        this.length = length;
        this.nullmask = nullmask;
    }

    Object getObject(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        switch (duckdb_type) {
        case BOOLEAN:
            return getBoolean(idx);
        case TINYINT:
            return getByte(idx);
        case SMALLINT:
            return getShort(idx);
        case INTEGER:
            return getInt(idx);
        case BIGINT:
            return getLong(idx);
        case HUGEINT:
            return getHugeint(idx);
        case UHUGEINT:
            return getUhugeint(idx);
        case UTINYINT:
            return getUint8(idx);
        case USMALLINT:
            return getUint16(idx);
        case UINTEGER:
            return getUint32(idx);
        case UBIGINT:
            return getUint64(idx);
        case FLOAT:
            return getFloat(idx);
        case DOUBLE:
            return getDouble(idx);
        case DECIMAL:
            return getBigDecimal(idx);
        case TIME:
        case TIME_NS:
            return getLocalTime(idx);
        case TIME_WITH_TIME_ZONE:
            return getOffsetTime(idx);
        case DATE:
            return getLocalDate(idx);
        case TIMESTAMP:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
            return getTimestamp(idx);
        case TIMESTAMP_WITH_TIME_ZONE:
            return getOffsetDateTime(idx);
        case JSON:
            return getJsonObject(idx);
        case BLOB:
            return getBlob(idx);
        case UUID:
            return getUuid(idx);
        case MAP:
            return getMap(idx);
        case LIST:
        case ARRAY:
            return getArray(idx);
        case STRUCT:
            return getStruct(idx);
        case UNION:
            return getUnion(idx);
        default:
            return getLazyString(idx);
        }
    }

    LocalTime getLocalTime(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case TIME: {
            long microseconds = getLongFromConstlen(idx);
            long nanoseconds = TimeUnit.MICROSECONDS.toNanos(microseconds);
            return LocalTime.ofNanoOfDay(nanoseconds);
        }
        case TIME_NS: {
            long nanoseconds = getLongFromConstlen(idx);
            return LocalTime.ofNanoOfDay(nanoseconds);
        }
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime ldt = getLocalDateTimeFromTimestamp(idx, null);
            return ldt.toLocalTime();
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return LocalTime.parse(lazyString);
    }

    LocalDate getLocalDate(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case DATE: {
            int day = getbuf(idx, 4).getInt();
            return LocalDate.ofEpochDay(day);
        }
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime ldt = getLocalDateTimeFromTimestamp(idx, null);
            return ldt.toLocalDate();
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        if ("infinity".equals(lazyString)) {
            return LocalDate.MAX;
        } else if ("-infinity".equals(lazyString)) {
            return LocalDate.MIN;
        }

        return LocalDate.from(ERA_FORMAT.parse(lazyString));
    }

    BigDecimal getBigDecimal(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.DECIMAL)) {
            switch (meta.type_size) {
            case 16:
                return new BigDecimal((int) getbuf(idx, 2).getShort()).scaleByPowerOfTen(meta.scale * -1);
            case 32:
                return new BigDecimal(getbuf(idx, 4).getInt()).scaleByPowerOfTen(meta.scale * -1);
            case 64:
                return new BigDecimal(getbuf(idx, 8).getLong()).scaleByPowerOfTen(meta.scale * -1);
            case 128:
                ByteBuffer buf = getbuf(idx, 16);
                long lower = buf.getLong();
                long upper = buf.getLong();
                return new BigDecimal(upper)
                    .multiply(ULONG_MULTIPLIER)
                    .add(new BigDecimal(Long.toUnsignedString(lower)))
                    .scaleByPowerOfTen(meta.scale * -1);
            }
        }
        Object o = getObject(idx);
        return new BigDecimal(o.toString());
    }

    OffsetDateTime getOffsetDateTime(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime ldt = getLocalDateTimeFromTimestamp(idx, null);
            Instant instant = ldt.toInstant(ZoneOffset.UTC);
            ZoneId systemZone = ZoneId.systemDefault();
            ZoneOffset zoneOffset = systemZone.getRules().getOffset(instant);
            return ldt.atOffset(zoneOffset);
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return OffsetDateTime.parse(lazyString);
    }

    Timestamp getTimestamp(int idx) throws SQLException {
        return getTimestamp(idx, null);
    }

    UUID getUuid(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        if (isType(DuckDBColumnType.UUID)) {
            ByteBuffer buffer = getbuf(idx, 16);
            long leastSignificantBits = buffer.getLong();
            long mostSignificantBits = buffer.getLong();
            // Account for the following logic in UUID::FromString:
            // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
            mostSignificantBits ^= Long.MIN_VALUE;
            return new UUID(mostSignificantBits, leastSignificantBits);
        }
        Object o = getObject(idx);
        return UUID.fromString(o.toString());
    }

    String getLazyString(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        return varlen_data[idx].toString();
    }

    Array getArray(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.LIST) || isType(DuckDBColumnType.ARRAY)) {
            return (Array) varlen_data[idx];
        }
        throw new SQLFeatureNotSupportedException("getArray");
    }

    Map<Object, Object> getMap(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (!isType(DuckDBColumnType.MAP)) {
            throw new SQLFeatureNotSupportedException("getMap");
        }

        Object[] entries = (Object[]) (((Array) varlen_data[idx]).getArray());
        Map<Object, Object> result = new LinkedHashMap<>();

        for (Object entry : entries) {
            Object[] entry_val = ((Struct) entry).getAttributes();
            result.put(entry_val[0], entry_val[1]);
        }

        return result;
    }

    Blob getBlob(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.BLOB)) {
            return new DuckDBResultSet.DuckDBBlobResult(ByteBuffer.wrap((byte[]) varlen_data[idx]));
        }

        throw new SQLFeatureNotSupportedException("getBlob");
    }

    byte[] getBytes(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        if (isType(DuckDBColumnType.BLOB)) {
            return (byte[]) varlen_data[idx];
        }

        throw new SQLFeatureNotSupportedException("getBytes");
    }

    JsonNode getJsonObject(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        String result = getLazyString(idx);
        return result == null ? null : new JsonNode(result);
    }

    Date getDate(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case DATE:
            return Date.valueOf(this.getLocalDate(idx));
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            LocalDateTime ldt = getLocalDateTimeFromTimestamp(idx, null);
            LocalDateTime ldtTruncated = ldt.truncatedTo(ChronoUnit.DAYS);
            Timestamp tsTruncated = Timestamp.valueOf(ldtTruncated);
            return new Date(tsTruncated.getTime());
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return Date.valueOf(lazyString);
    }

    OffsetTime getOffsetTime(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case TIME:
        case TIME_NS: {
            LocalTime lt = getLocalTime(idx);
            return lt.atOffset(ZoneOffset.UTC);
        }
        case TIME_WITH_TIME_ZONE: {
            long micros = getLongFromConstlen(idx);
            return DuckDBTimestamp.toOffsetTime(micros);
        }
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            OffsetDateTime odt = getOffsetDateTime(idx);
            return odt.toOffsetTime();
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return OffsetTime.parse(lazyString);
    }

    Time getTime(int idx) throws SQLException {
        return getTime(idx, null);
    }

    Time getTime(int idx, Calendar cal) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        switch (duckdb_type) {
        case TIME:
        case TIME_NS: {
            LocalTime lt = getLocalTime(idx);
            return Time.valueOf(lt);
        }
        case TIME_WITH_TIME_ZONE: {
            long micros = getLongFromConstlen(idx);
            OffsetTime ot = DuckDBTimestamp.toOffsetTime(micros);
            LocalTime lt = ot.toLocalTime();
            return Time.valueOf(lt);
        }
        case TIMESTAMP:
        case TIMESTAMP_MS:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_WITH_TIME_ZONE: {
            Timestamp ts = getTimestamp(idx, cal);
            return new Time(ts.getTime());
        }
        }

        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return Time.valueOf(lazyString);
    }

    Boolean getBoolean(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return false;
        }
        if (isType(DuckDBColumnType.BOOLEAN)) {
            return getbuf(idx, 1).get() == 1;
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).byteValue() == 1;
        }

        return Boolean.parseBoolean(o.toString());
    }

    protected ByteBuffer getbuf(int idx, int typeWidth) {
        ByteBuffer buf = constlen_data;
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.position(idx * typeWidth);
        return buf;
    }

    private long getLongFromConstlen(int idx) {
        ByteBuffer buf = getbuf(idx, 8);
        return buf.getLong();
    }

    protected boolean check_and_null(int idx) {
        return nullmask[idx];
    }

    long getLong(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.BIGINT) || isType(DuckDBColumnType.TIMESTAMP) ||
            isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return getbuf(idx, 8).getLong();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        return Long.parseLong(o.toString());
    }

    int getInt(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.INTEGER)) {
            return getbuf(idx, 4).getInt();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        return Integer.parseInt(o.toString());
    }

    short getUint8(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.UTINYINT)) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            getbuf(idx, 1).get(buf.array(), 1, 1);
            return buf.getShort();
        }
        throw new SQLFeatureNotSupportedException("getUint8");
    }

    long getUint32(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.UINTEGER)) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            getbuf(idx, 4).get(buf.array(), 0, 4);
            return buf.getLong();
        }
        throw new SQLFeatureNotSupportedException("getUint32");
    }

    int getUint16(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.USMALLINT)) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            getbuf(idx, 2).get(buf.array(), 0, 2);
            return buf.getInt();
        }
        throw new SQLFeatureNotSupportedException("getUint16");
    }

    BigInteger getUint64(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return BigInteger.ZERO;
        }
        if (isType(DuckDBColumnType.UBIGINT)) {
            byte[] buf_res = new byte[16];
            byte[] buf = new byte[8];
            getbuf(idx, 8).get(buf);
            for (int i = 0; i < 8; i++) {
                buf_res[i + 8] = buf[7 - i];
            }
            return new BigInteger(buf_res);
        }
        throw new SQLFeatureNotSupportedException("getUint64");
    }

    double getDouble(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return Double.NaN;
        }
        if (isType(DuckDBColumnType.DOUBLE)) {
            return getbuf(idx, 8).getDouble();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        return Double.parseDouble(o.toString());
    }

    byte getByte(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.TINYINT)) {
            return getbuf(idx, 1).get();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).byteValue();
        }
        return Byte.parseByte(o.toString());
    }

    short getShort(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.SMALLINT)) {
            return getbuf(idx, 2).getShort();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).shortValue();
        }
        return Short.parseShort(o.toString());
    }

    BigInteger getHugeint(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return BigInteger.ZERO;
        }
        if (isType(DuckDBColumnType.HUGEINT)) {
            byte[] buf = new byte[16];
            getbuf(idx, 16).get(buf);
            for (int i = 0; i < 8; i++) {
                byte keep = buf[i];
                buf[i] = buf[15 - i];
                buf[15 - i] = keep;
            }
            return new BigInteger(buf);
        }
        Object o = getObject(idx);
        return new BigInteger(o.toString());
    }

    BigInteger getUhugeint(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return BigInteger.ZERO;
        }
        if (isType(DuckDBColumnType.UHUGEINT)) {
            byte[] buf = new byte[16];
            getbuf(idx, 16).get(buf);
            for (int i = 0; i < 8; i++) {
                byte keep = buf[i];
                buf[i] = buf[15 - i];
                buf[15 - i] = keep;
            }
            return new BigInteger(1, buf);
        }
        Object o = getObject(idx);
        return new BigInteger(o.toString());
    }

    float getFloat(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return Float.NaN;
        }
        if (isType(DuckDBColumnType.FLOAT)) {
            return getbuf(idx, 4).getFloat();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).floatValue();
        }
        return Float.parseFloat(o.toString());
    }

    private boolean isType(DuckDBColumnType columnType) {
        return duckdb_type == columnType;
    }

    private LocalDateTime getLocalDateTimeFromDate(int idx) throws SQLException {
        LocalDate ld = getLocalDate(idx);
        if (ld == null) {
            return null;
        }
        return ld.atStartOfDay();
    }

    Timestamp getTimestamp(int idx, Calendar calNullable) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        final LocalDateTime ldt;
        if (duckdb_type == DuckDBColumnType.DATE) {
            ldt = getLocalDateTimeFromDate(idx);
        } else {
            ldt = getLocalDateTimeFromTimestamp(idx, calNullable);
        }
        if (ldt != null) {
            return Timestamp.valueOf(ldt);
        }
        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return Timestamp.valueOf(lazyString);
    }

    LocalDateTime getLocalDateTime(int idx) throws SQLException {
        final LocalDateTime ldt;
        if (duckdb_type == DuckDBColumnType.DATE) {
            ldt = getLocalDateTimeFromDate(idx);
        } else {
            ldt = getLocalDateTimeFromTimestamp(idx, null);
        }
        if (ldt != null) {
            return ldt;
        }
        String lazyString = getLazyString(idx);
        if (lazyString == null) {
            return null;
        }
        return LocalDateTime.parse(lazyString);
    }

    private LocalDateTime getLocalDateTimeFromTimestamp(int idx, Calendar calNullable) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        ZoneId zoneIdNullable = calNullable != null ? calNullable.getTimeZone().toZoneId() : null;
        switch (duckdb_type) {
        case TIMESTAMP:
            return localDateTimeFromTimestamp(getLongFromConstlen(idx), MICROS, zoneIdNullable);
        case TIMESTAMP_MS:
            return localDateTimeFromTimestamp(getLongFromConstlen(idx), MILLIS, zoneIdNullable);
        case TIMESTAMP_NS:
            return localDateTimeFromTimestamp(getLongFromConstlen(idx), NANOS, zoneIdNullable);
        case TIMESTAMP_S:
            return localDateTimeFromTimestamp(getLongFromConstlen(idx), SECONDS, zoneIdNullable);
        case TIMESTAMP_WITH_TIME_ZONE: {
            return localDateTimeFromTimestampWithTimezone(getLongFromConstlen(idx), MICROS, zoneIdNullable);
        }
        }
        return null;
    }

    Struct getStruct(int idx) {
        return check_and_null(idx) ? null : (Struct) varlen_data[idx];
    }

    Object getUnion(int idx) throws SQLException {
        if (check_and_null(idx))
            return null;

        Struct struct = getStruct(idx);

        Object[] attributes = struct.getAttributes();

        short tag = (short) attributes[0];

        return attributes[1 + tag];
    }
}

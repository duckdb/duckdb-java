package org.duckdb;

import static org.duckdb.JdbcUtils.createSQLException;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;

public class DuckDBTimestamp {

    public DuckDBTimestamp(long timeMicros) {
        this.timeMicros = timeMicros;
    }

    public DuckDBTimestamp(LocalDateTime localDateTime) {
        this.timeMicros = localDateTime2Micros(localDateTime);
    }

    public DuckDBTimestamp(OffsetDateTime offsetDateTime) {
        this.timeMicros = localDateTime2Micros(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
    }

    public DuckDBTimestamp(Timestamp sqlTimestamp) {
        this.timeMicros = localDateTime2Micros(sqlTimestamp.toLocalDateTime());
    }

    protected long timeMicros;

    private static Instant createInstant(long value, ChronoUnit unit) throws SQLException {
        switch (unit) {
        case SECONDS:
            return Instant.ofEpochSecond(value);
        case MILLIS:
            return Instant.ofEpochMilli(value);
        case MICROS: {
            long epochSecond = value / 1_000_000L;
            long nanoAdjustment = (value % 1_000_000L) * 1000L;
            return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
        }
        case NANOS: {
            long epochSecond = value / 1_000_000_000L;
            long nanoAdjustment = value % 1_000_000_000L;
            return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
        }
        default:
            throw createSQLException("Unsupported unit type: [" + unit + "]", ErrorCode.TIMESTAMP_UNSUPPORTED_UNIT);
        }
    }

    public static LocalDateTime localDateTimeFromTimestampWithTimezone(long value, ChronoUnit unit,
                                                                       ZoneId zoneIdNullable) throws SQLException {
        Instant instant = createInstant(value, unit);
        ZoneId zoneId = zoneIdNullable != null ? zoneIdNullable : ZoneId.systemDefault();
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    static LocalDateTime localDateTimeFromTimestamp(long value, ChronoUnit unit) throws SQLException {
        return localDateTimeFromTimestamp(value, unit, null);
    }

    public static LocalDateTime localDateTimeFromTimestamp(long value, ChronoUnit unit, ZoneId zoneIdNullable)
        throws SQLException {
        Instant instant = createInstant(value, unit);
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        if (null == zoneIdNullable) {
            return ldt;
        }
        ZoneId zoneIdDefault = ZoneId.systemDefault();
        LocalDateTime ldtDefault = LocalDateTime.ofInstant(instant, zoneIdDefault);
        LocalDateTime ldtZoned = LocalDateTime.ofInstant(instant, zoneIdNullable);
        Duration duration = Duration.between(ldtZoned, ldtDefault);
        LocalDateTime ldtAdjusted = ldt.plus(duration);
        return ldtAdjusted;
    }

    public static OffsetTime toOffsetTime(long timeBits) {
        long timeMicros = timeBits >> 24;   // High 40 bits are micros
        long offset = timeBits & 0x0FFFFFF; // Low 24 bits are inverted biased offset in seconds
        long max_offset = 16 * 60 * 60 - 1; // ±15:59:59
        offset = max_offset - offset;
        int sign = (offset < 0) ? -1 : 1;
        offset = Math.abs(offset);

        int ss = (int) offset % 60;
        offset = offset / 60;

        int mm = (int) offset % 60;
        int hh = (int) offset / 60;

        if (hh > 15) {
            return OffsetTime.of(toLocalTime(timeMicros), ZoneOffset.UTC);
        } else {
            return OffsetTime.of(toLocalTime(timeMicros),
                                 ZoneOffset.ofHoursMinutesSeconds(sign * hh, sign * mm, sign * ss));
        }
    }

    private static LocalTime toLocalTime(long timeMicros) {
        return LocalTime.ofNanoOfDay(timeMicros * 1000);
    }

    public static long localDateTime2Micros(LocalDateTime localDateTime) {
        // Whole microseconds since the epoch, rounded toward negative infinity (floor).
        //
        // A naive RefLocalDateTime.until(localDateTime, ChronoUnit.MICROS) truncates the
        // interval toward zero, so a pre-epoch timestamp whose sub-microsecond remainder is
        // not exact (e.g. 1969-12-31T23:59:59.987654321) is rounded up by one microsecond
        // (to .987655 instead of the expected floor .987654). Computing from the UTC epoch
        // second (which is already floored) plus the floored microsecond part of the nanos
        // keeps the negative floor exact for every path (Timestamp/LocalDateTime/OffsetDateTime).
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        int nanoOfSecond = localDateTime.getNano();
        return Math.addExact(Math.multiplyExact(epochSecond, 1_000_000L), nanoOfSecond / 1000L);
    }

    // TODO: move this to C++ side
    public static Object valueOf(Object x) {
        // Change sql.Timestamp to DuckDBTimestamp
        if (x instanceof Timestamp) {
            x = new DuckDBTimestamp((Timestamp) x);
        } else if (x instanceof LocalDateTime) {
            x = new DuckDBTimestamp((LocalDateTime) x);
        } else if (x instanceof LocalDate) {
            x = new DuckDBDate(Date.valueOf((LocalDate) x));
        } else if (x instanceof OffsetDateTime) {
            x = new DuckDBTimestampTZ((OffsetDateTime) x);
        } else if (x instanceof Date) {
            x = new DuckDBDate((Date) x);
        } else if (x instanceof Time) {
            x = new DuckDBTime((Time) x);
        } else if (x instanceof LocalTime) {
            x = new DuckDBTime((LocalTime) x);
        }
        return x;
    }

    public Timestamp toSqlTimestamp() {
        return Timestamp.valueOf(this.toLocalDateTime());
    }

    public LocalDateTime toLocalDateTime() {
        return LocalDateTime.ofEpochSecond(micros2seconds(timeMicros), nanosPartMicros(timeMicros), ZoneOffset.UTC);
    }

    public OffsetDateTime toOffsetDateTime() {
        return OffsetDateTime.of(toLocalDateTime(), ZoneOffset.UTC);
    }

    public static long getMicroseconds(Timestamp sqlTimestamp) {
        return localDateTime2Micros(sqlTimestamp.toLocalDateTime());
    }

    public long getMicrosEpoch() {
        return this.timeMicros;
    }

    public String toString() {
        return this.toLocalDateTime().toString();
    }

    private static long micros2seconds(long micros) {
        if ((micros % 1000_000L) >= 0) {
            return micros / 1000_000L;
        } else {
            return (micros / 1000_000L) - 1;
        }
    }

    private static int nanosPartMicros(long micros) {
        if ((micros % 1000_000L) >= 0) {
            return (int) ((micros % 1000_000L) * 1000);
        } else {
            return (int) ((1000_000L + (micros % 1000_000L)) * 1000);
        }
    }
}

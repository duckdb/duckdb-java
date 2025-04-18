package org.duckdb;

import java.sql.Time;
import java.time.*;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DuckDBTime extends DuckDBTimestamp {
    public DuckDBTime(Time time) {
        super(TimeUnit.MILLISECONDS.toMicros(timeToMillis(time)));
    }

    public DuckDBTime(LocalTime lt) {
        super(TimeUnit.NANOSECONDS.toMicros(lt.toNanoOfDay()));
    }

    private static long timeToMillis(Time time) {
        // Per JDBC spec PreparedStatement#setTime() must NOT use
        // default JVM time zone.
        // The Time we have is already shifted with the default time zone,
        // so we need to shift it back to keep hours/minutes/seconds
        // values intact.
        Instant instant = Instant.ofEpochMilli(time.getTime());
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, zoneId);
        ZonedDateTime utc = ldt.atZone(ZoneId.of("UTC"));
        return utc.toInstant().toEpochMilli();
    }
}

package org.duckdb;

import static org.duckdb.JdbcUtils.createSQLException;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Internal carrier for a TIMESTAMP_NS parameter value.
 *
 * <p>{@link DuckDBTimestamp} only keeps microseconds since the epoch, so binding a raw nanosecond value through it
 * silently loses the sub-microsecond part. This subclass keeps the exact epoch nanoseconds and is converted by the JNI
 * layer into a native {@code TIMESTAMP_NS} value instead of a {@code TIMESTAMP} one.
 *
 * <p>Package private on purpose: instances are only created by {@link DuckDBPreparedStatement} for parameters whose
 * metadata type is exactly TIMESTAMP_NS, and only read back through {@link #getNanosEpoch()} from the JNI layer.
 */
final class DuckDBTimestampNS extends DuckDBTimestamp {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    private final long epochNanos;

    DuckDBTimestampNS(LocalDateTime localDateTime) throws SQLException {
        this(toEpochNanos(localDateTime));
    }

    private DuckDBTimestampNS(long epochNanos) throws SQLException {
        // The infinity sentinels are rejected before the microsecond floor of the super constructor runs, so an
        // unsupported value is always reported as a TIMESTAMP_NS range error.
        super(Math.floorDiv(checkedNanos(epochNanos), NANOS_PER_MICRO));
        this.epochNanos = epochNanos;
    }

    long getNanosEpoch() {
        return epochNanos;
    }

    private static long toEpochNanos(LocalDateTime localDateTime) throws SQLException {
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        int nanoOfSecond = localDateTime.getNano();
        try {
            if (epochSecond < 0 && nanoOfSecond > 0) {
                // epochSecond * 1e9 alone overflows for representable values near the negative boundary, so move one
                // second into the nanosecond part and let the exact integer arithmetic do the carry.
                return Math.addExact(Math.multiplyExact(epochSecond + 1, NANOS_PER_SECOND),
                                     nanoOfSecond - NANOS_PER_SECOND);
            }
            return Math.addExact(Math.multiplyExact(epochSecond, NANOS_PER_SECOND), (long) nanoOfSecond);
        } catch (ArithmeticException e) {
            throw outOfRange(localDateTime, e);
        }
    }

    private static long checkedNanos(long epochNanos) throws SQLException {
        // Only +/-Long.MAX_VALUE are reserved: native timestamp_base_t uses them as the infinity sentinels, every
        // other int64 value (including Long.MIN_VALUE) is a finite TIMESTAMP_NS.
        if (epochNanos == Long.MAX_VALUE || epochNanos == -Long.MAX_VALUE) {
            throw outOfRange(null, null);
        }
        return epochNanos;
    }

    private static SQLException outOfRange(LocalDateTime localDateTime, Throwable cause) {
        String value = localDateTime == null ? "" : " (value: " + localDateTime + ")";
        return createSQLException("Value is out of range for TIMESTAMP_NS: nanoseconds since the epoch must be a "
                                      + "finite timestamp" + value,
                                  ErrorCode.PREPARED_CONVERSION, cause);
    }
}

package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.stream.LongStream;

/**
 * Read-only scalar callback view over a DuckDB vector.
 *
 * <p>Implementations throw {@link DuckDBFunctionException} for callback-time type/value errors.
 * Invalid row indexes throw {@link IndexOutOfBoundsException}.
 */
public abstract class DuckDBReadableVector {
    public abstract DuckDBColumnType getType();

    public abstract long rowCount();

    public abstract LongStream rowIndexStream();

    public abstract boolean isNull(long row);

    public abstract boolean getBoolean(long row);

    public abstract boolean getBoolean(long row, boolean defaultVal);

    public abstract byte getByte(long row);

    public abstract byte getByte(long row, byte defaultVal);

    public abstract short getShort(long row);

    public abstract short getShort(long row, short defaultVal);

    public abstract short getUint8(long row);

    public abstract short getUint8(long row, short defaultVal);

    public abstract int getUint16(long row);

    public abstract int getUint16(long row, int defaultVal);

    public abstract int getInt(long row);

    public abstract int getInt(long row, int defaultVal);

    public abstract long getUint32(long row);

    public abstract long getUint32(long row, long defaultVal);

    public abstract long getLong(long row);

    public abstract long getLong(long row, long defaultVal);

    public abstract BigInteger getHugeInt(long row);

    public abstract BigInteger getUHugeInt(long row);

    public abstract BigInteger getUint64(long row);

    public abstract float getFloat(long row);

    public abstract float getFloat(long row, float defaultVal);

    public abstract double getDouble(long row);

    public abstract double getDouble(long row, double defaultVal);

    public abstract LocalDate getLocalDate(long row);

    public abstract Date getDate(long row);

    public abstract LocalDateTime getLocalDateTime(long row);

    public abstract Timestamp getTimestamp(long row);

    public abstract OffsetDateTime getOffsetDateTime(long row);

    public abstract BigDecimal getBigDecimal(long row);

    public abstract String getString(long row);
}

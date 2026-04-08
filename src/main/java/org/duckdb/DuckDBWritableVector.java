package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * Mutable scalar callback view over a DuckDB output vector.
 *
 * <p>Implementations throw {@link DuckDBFunctionException} for callback-time type/value write
 * errors. Invalid row indexes throw {@link IndexOutOfBoundsException}.
 */
public abstract class DuckDBWritableVector {
    public abstract DuckDBColumnType getType();

    public abstract long rowCount();

    public abstract void addNull();

    public abstract void setNull(long row);

    public abstract void addBoolean(boolean value);

    public abstract void setBoolean(long row, boolean value);

    public abstract void addByte(byte value);

    public abstract void setByte(long row, byte value);

    public abstract void addShort(short value);

    public abstract void setShort(long row, short value);

    public abstract void addUint8(int value);

    public abstract void setUint8(long row, int value);

    public abstract void addUint16(int value);

    public abstract void setUint16(long row, int value);

    public abstract void addInt(int value);

    public abstract void setInt(long row, int value);

    public abstract void addUint32(long value);

    public abstract void setUint32(long row, long value);

    public abstract void addLong(long value);

    public abstract void setLong(long row, long value);

    public abstract void addHugeInt(BigInteger value);

    public abstract void setHugeInt(long row, BigInteger value);

    public abstract void addUHugeInt(BigInteger value);

    public abstract void setUHugeInt(long row, BigInteger value);

    public abstract void addUint64(BigInteger value);

    public abstract void setUint64(long row, BigInteger value);

    public abstract void addFloat(float value);

    public abstract void setFloat(long row, float value);

    public abstract void addDouble(double value);

    public abstract void setDouble(long row, double value);

    public abstract void addDate(LocalDate value);

    public abstract void setDate(long row, LocalDate value);

    public abstract void addDate(java.sql.Date value);

    public abstract void setDate(long row, java.sql.Date value);

    public abstract void addDate(java.util.Date value);

    public abstract void setDate(long row, java.util.Date value);

    public abstract void addTimestamp(LocalDateTime value);

    public abstract void setTimestamp(long row, LocalDateTime value);

    public abstract void addTimestamp(Timestamp value);

    public abstract void setTimestamp(long row, Timestamp value);

    public abstract void addTimestamp(java.util.Date value);

    public abstract void setTimestamp(long row, java.util.Date value);

    public abstract void addTimestamp(LocalDate value);

    public abstract void setTimestamp(long row, LocalDate value);

    public abstract void addOffsetDateTime(OffsetDateTime value);

    public abstract void setOffsetDateTime(long row, OffsetDateTime value);

    public abstract void addBigDecimal(BigDecimal value);

    public abstract void setBigDecimal(long row, BigDecimal value);

    public abstract void addString(String value);

    public abstract void setString(long row, String value);
}

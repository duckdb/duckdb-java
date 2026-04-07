package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public interface DuckDBWritableVector {
    DuckDBColumnType getType();

    long rowCount();

    void addNull() throws SQLException;

    void setNull(long row) throws SQLException;

    void addBoolean(boolean value) throws SQLException;

    void setBoolean(long row, boolean value) throws SQLException;

    void addByte(byte value) throws SQLException;

    void setByte(long row, byte value) throws SQLException;

    void addShort(short value) throws SQLException;

    void setShort(long row, short value) throws SQLException;

    void addUint8(int value) throws SQLException;

    void setUint8(long row, int value) throws SQLException;

    void addUint16(int value) throws SQLException;

    void setUint16(long row, int value) throws SQLException;

    void addInt(int value) throws SQLException;

    void setInt(long row, int value) throws SQLException;

    void addUint32(long value) throws SQLException;

    void setUint32(long row, long value) throws SQLException;

    void addLong(long value) throws SQLException;

    void setLong(long row, long value) throws SQLException;

    void addUint64(BigInteger value) throws SQLException;

    void setUint64(long row, BigInteger value) throws SQLException;

    void addFloat(float value) throws SQLException;

    void setFloat(long row, float value) throws SQLException;

    void addDouble(double value) throws SQLException;

    void setDouble(long row, double value) throws SQLException;

    void addDate(LocalDate value) throws SQLException;

    void setDate(long row, LocalDate value) throws SQLException;

    void addDate(java.sql.Date value) throws SQLException;

    void setDate(long row, java.sql.Date value) throws SQLException;

    void addDate(java.util.Date value) throws SQLException;

    void setDate(long row, java.util.Date value) throws SQLException;

    void addTimestamp(LocalDateTime value) throws SQLException;

    void setTimestamp(long row, LocalDateTime value) throws SQLException;

    void addTimestamp(Timestamp value) throws SQLException;

    void setTimestamp(long row, Timestamp value) throws SQLException;

    void addTimestamp(java.util.Date value) throws SQLException;

    void setTimestamp(long row, java.util.Date value) throws SQLException;

    void addTimestamp(LocalDate value) throws SQLException;

    void setTimestamp(long row, LocalDate value) throws SQLException;

    void addOffsetDateTime(OffsetDateTime value) throws SQLException;

    void setOffsetDateTime(long row, OffsetDateTime value) throws SQLException;

    void addBigDecimal(BigDecimal value) throws SQLException;

    void setBigDecimal(long row, BigDecimal value) throws SQLException;

    void addString(String value) throws SQLException;

    void setString(long row, String value) throws SQLException;
}

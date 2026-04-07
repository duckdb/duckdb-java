package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.stream.LongStream;

public interface DuckDBReadableVector {
    DuckDBColumnType getType();

    long rowCount();

    LongStream rowIndexStream();

    boolean isNull(long row);

    boolean getBoolean(long row) throws SQLException;

    byte getByte(long row) throws SQLException;

    short getShort(long row) throws SQLException;

    short getUint8(long row) throws SQLException;

    int getUint16(long row) throws SQLException;

    int getInt(long row) throws SQLException;

    long getUint32(long row) throws SQLException;

    long getLong(long row) throws SQLException;

    BigInteger getUint64(long row) throws SQLException;

    float getFloat(long row) throws SQLException;

    double getDouble(long row) throws SQLException;

    LocalDate getLocalDate(long row) throws SQLException;

    Date getDate(long row) throws SQLException;

    LocalDateTime getLocalDateTime(long row) throws SQLException;

    Timestamp getTimestamp(long row) throws SQLException;

    OffsetDateTime getOffsetDateTime(long row) throws SQLException;

    BigDecimal getBigDecimal(long row) throws SQLException;

    String getString(long row) throws SQLException;
}

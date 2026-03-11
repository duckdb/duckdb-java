package org.duckdb;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Date;
import java.util.UUID;

public interface UdfReader {
    DuckDBColumnType getType();

    boolean isNull(int row);

    int getInt(int row);

    long getLong(int row);

    float getFloat(int row);

    double getDouble(int row);

    BigDecimal getBigDecimal(int row);

    Date getDate(int row);

    LocalDate getLocalDate(int row);

    LocalTime getLocalTime(int row);

    OffsetTime getOffsetTime(int row);

    LocalDateTime getLocalDateTime(int row);

    OffsetDateTime getOffsetDateTime(int row);

    UUID getUUID(int row);

    boolean getBoolean(int row);

    String getString(int row);

    byte[] getBytes(int row);
}

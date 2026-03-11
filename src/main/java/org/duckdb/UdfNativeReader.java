package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Date;
import java.util.UUID;

final class UdfNativeReader implements UdfReader {
    private final UdfScalarWriter vector;

    UdfNativeReader(int capiTypeId, ByteBuffer data, ByteBuffer vectorRef, ByteBuffer validity, int rowCount) {
        this.vector = new UdfScalarWriter(capiTypeId, data, vectorRef, validity, rowCount);
    }

    @Override
    public DuckDBColumnType getType() {
        return vector.getType();
    }

    @Override
    public boolean isNull(int row) {
        return vector.isNull(row);
    }

    @Override
    public int getInt(int row) {
        return vector.getInt(row);
    }

    @Override
    public long getLong(int row) {
        return vector.getLong(row);
    }

    @Override
    public float getFloat(int row) {
        return vector.getFloat(row);
    }

    @Override
    public double getDouble(int row) {
        return vector.getDouble(row);
    }

    @Override
    public BigDecimal getBigDecimal(int row) {
        return vector.getBigDecimal(row);
    }

    @Override
    public BigInteger getBigInteger(int row) {
        return vector.getBigInteger(row);
    }

    @Override
    public Date getDate(int row) {
        return vector.getDate(row);
    }

    @Override
    public LocalDate getLocalDate(int row) {
        return vector.getLocalDate(row);
    }

    @Override
    public LocalTime getLocalTime(int row) {
        return vector.getLocalTime(row);
    }

    @Override
    public OffsetTime getOffsetTime(int row) {
        return vector.getOffsetTime(row);
    }

    @Override
    public LocalDateTime getLocalDateTime(int row) {
        return vector.getLocalDateTime(row);
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int row) {
        return vector.getOffsetDateTime(row);
    }

    @Override
    public UUID getUUID(int row) {
        return vector.getUUID(row);
    }

    @Override
    public boolean getBoolean(int row) {
        return vector.getBoolean(row);
    }

    @Override
    public String getString(int row) {
        return vector.getString(row);
    }

    @Override
    public byte[] getBytes(int row) {
        return vector.getBytes(row);
    }
}

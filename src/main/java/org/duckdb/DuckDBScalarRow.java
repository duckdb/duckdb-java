package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public final class DuckDBScalarRow {
    private final DuckDBScalarContext context;
    private final long rowIndex;

    DuckDBScalarRow(DuckDBScalarContext context, long rowIndex) {
        this.context = context;
        this.rowIndex = rowIndex;
    }

    public long index() {
        return rowIndex;
    }

    public boolean isNull(int columnIndex) {
        return input(columnIndex).isNull(rowIndex);
    }

    public boolean getBoolean(int columnIndex) {
        try {
            return input(columnIndex).getBoolean(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("BOOLEAN", columnIndex, exception);
        }
    }

    public boolean getBoolean(int columnIndex, boolean defaultVal) {
        try {
            return input(columnIndex).getBoolean(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("BOOLEAN", columnIndex, exception);
        }
    }

    public byte getByte(int columnIndex) {
        try {
            return input(columnIndex).getByte(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("TINYINT", columnIndex, exception);
        }
    }

    public byte getByte(int columnIndex, byte defaultVal) {
        try {
            return input(columnIndex).getByte(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("TINYINT", columnIndex, exception);
        }
    }

    public short getShort(int columnIndex) {
        try {
            return input(columnIndex).getShort(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("SMALLINT", columnIndex, exception);
        }
    }

    public short getShort(int columnIndex, short defaultVal) {
        try {
            return input(columnIndex).getShort(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("SMALLINT", columnIndex, exception);
        }
    }

    public short getUint8(int columnIndex) {
        try {
            return input(columnIndex).getUint8(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UTINYINT", columnIndex, exception);
        }
    }

    public short getUint8(int columnIndex, short defaultVal) {
        try {
            return input(columnIndex).getUint8(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UTINYINT", columnIndex, exception);
        }
    }

    public int getUint16(int columnIndex) {
        try {
            return input(columnIndex).getUint16(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("USMALLINT", columnIndex, exception);
        }
    }

    public int getUint16(int columnIndex, int defaultVal) {
        try {
            return input(columnIndex).getUint16(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("USMALLINT", columnIndex, exception);
        }
    }

    public int getInt(int columnIndex) {
        try {
            return input(columnIndex).getInt(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("INTEGER", columnIndex, exception);
        }
    }

    public int getInt(int columnIndex, int defaultVal) {
        try {
            return input(columnIndex).getInt(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("INTEGER", columnIndex, exception);
        }
    }

    public long getUint32(int columnIndex) {
        try {
            return input(columnIndex).getUint32(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UINTEGER", columnIndex, exception);
        }
    }

    public long getUint32(int columnIndex, long defaultVal) {
        try {
            return input(columnIndex).getUint32(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UINTEGER", columnIndex, exception);
        }
    }

    public long getLong(int columnIndex) {
        try {
            return input(columnIndex).getLong(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("BIGINT", columnIndex, exception);
        }
    }

    public long getLong(int columnIndex, long defaultVal) {
        try {
            return input(columnIndex).getLong(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("BIGINT", columnIndex, exception);
        }
    }

    public BigInteger getHugeInt(int columnIndex) {
        try {
            return input(columnIndex).getHugeInt(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("HUGEINT", columnIndex, exception);
        }
    }

    public BigInteger getUHugeInt(int columnIndex) {
        try {
            return input(columnIndex).getUHugeInt(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UHUGEINT", columnIndex, exception);
        }
    }

    public BigInteger getUint64(int columnIndex) {
        try {
            return input(columnIndex).getUint64(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("UBIGINT", columnIndex, exception);
        }
    }

    public float getFloat(int columnIndex) {
        try {
            return input(columnIndex).getFloat(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("FLOAT", columnIndex, exception);
        }
    }

    public float getFloat(int columnIndex, float defaultVal) {
        try {
            return input(columnIndex).getFloat(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("FLOAT", columnIndex, exception);
        }
    }

    public double getDouble(int columnIndex) {
        try {
            return input(columnIndex).getDouble(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("DOUBLE", columnIndex, exception);
        }
    }

    public double getDouble(int columnIndex, double defaultVal) {
        try {
            return input(columnIndex).getDouble(rowIndex, defaultVal);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("DOUBLE", columnIndex, exception);
        }
    }

    public LocalDate getLocalDate(int columnIndex) {
        try {
            return input(columnIndex).getLocalDate(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("DATE", columnIndex, exception);
        }
    }

    public java.sql.Date getDate(int columnIndex) {
        try {
            return input(columnIndex).getDate(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("DATE", columnIndex, exception);
        }
    }

    public LocalDateTime getLocalDateTime(int columnIndex) {
        try {
            return input(columnIndex).getLocalDateTime(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("TIMESTAMP", columnIndex, exception);
        }
    }

    public Timestamp getTimestamp(int columnIndex) {
        try {
            return input(columnIndex).getTimestamp(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("TIMESTAMP", columnIndex, exception);
        }
    }

    public OffsetDateTime getOffsetDateTime(int columnIndex) {
        try {
            return input(columnIndex).getOffsetDateTime(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("TIMESTAMP WITH TIME ZONE", columnIndex, exception);
        }
    }

    public BigDecimal getBigDecimal(int columnIndex) {
        try {
            return input(columnIndex).getBigDecimal(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("DECIMAL", columnIndex, exception);
        }
    }

    public String getString(int columnIndex) {
        try {
            return input(columnIndex).getString(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw readFailure("VARCHAR", columnIndex, exception);
        }
    }

    public void setNull() {
        try {
            context.output().setNull(rowIndex);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("NULL", exception);
        }
    }

    public void setBoolean(boolean value) {
        try {
            context.output().setBoolean(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("BOOLEAN", exception);
        }
    }

    public void setByte(byte value) {
        try {
            context.output().setByte(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TINYINT", exception);
        }
    }

    public void setShort(short value) {
        try {
            context.output().setShort(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("SMALLINT", exception);
        }
    }

    public void setUint8(int value) {
        try {
            context.output().setUint8(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("UTINYINT", exception);
        }
    }

    public void setUint16(int value) {
        try {
            context.output().setUint16(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("USMALLINT", exception);
        }
    }

    public void setInt(int value) {
        try {
            context.output().setInt(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("INTEGER", exception);
        }
    }

    public void setUint32(long value) {
        try {
            context.output().setUint32(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("UINTEGER", exception);
        }
    }

    public void setLong(long value) {
        try {
            context.output().setLong(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("BIGINT", exception);
        }
    }

    public void setHugeInt(BigInteger value) {
        try {
            context.output().setHugeInt(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("HUGEINT", exception);
        }
    }

    public void setUHugeInt(BigInteger value) {
        try {
            context.output().setUHugeInt(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("UHUGEINT", exception);
        }
    }

    public void setUint64(BigInteger value) {
        try {
            context.output().setUint64(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("UBIGINT", exception);
        }
    }

    public void setFloat(float value) {
        try {
            context.output().setFloat(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("FLOAT", exception);
        }
    }

    public void setDouble(double value) {
        try {
            context.output().setDouble(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("DOUBLE", exception);
        }
    }

    public void setDate(LocalDate value) {
        try {
            context.output().setDate(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("DATE", exception);
        }
    }

    public void setDate(java.sql.Date value) {
        try {
            context.output().setDate(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("DATE", exception);
        }
    }

    public void setDate(java.util.Date value) {
        try {
            context.output().setDate(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("DATE", exception);
        }
    }

    public void setTimestamp(LocalDateTime value) {
        try {
            context.output().setTimestamp(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TIMESTAMP", exception);
        }
    }

    public void setTimestamp(Timestamp value) {
        try {
            context.output().setTimestamp(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TIMESTAMP", exception);
        }
    }

    public void setTimestamp(java.util.Date value) {
        try {
            context.output().setTimestamp(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TIMESTAMP", exception);
        }
    }

    public void setTimestamp(LocalDate value) {
        try {
            context.output().setTimestamp(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TIMESTAMP", exception);
        }
    }

    public void setOffsetDateTime(OffsetDateTime value) {
        try {
            context.output().setOffsetDateTime(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("TIMESTAMP WITH TIME ZONE", exception);
        }
    }

    public void setBigDecimal(BigDecimal value) {
        try {
            context.output().setBigDecimal(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("DECIMAL", exception);
        }
    }

    public void setString(String value) {
        try {
            context.output().setString(rowIndex, value);
        } catch (DuckDBFunctionException exception) {
            throw writeFailure("VARCHAR", exception);
        }
    }

    private DuckDBReadableVector input(int columnIndex) {
        return context.inputUnchecked(columnIndex);
    }

    private DuckDBFunctionException readFailure(String type, int columnIndex, DuckDBFunctionException exception) {
        return new DuckDBFunctionException(
            "Failed to read " + type + " from input column " + columnIndex + " at row " + rowIndex, exception);
    }

    private DuckDBFunctionException writeFailure(String type, DuckDBFunctionException exception) {
        return new DuckDBFunctionException("Failed to write " + type + " to output row " + rowIndex, exception);
    }
}

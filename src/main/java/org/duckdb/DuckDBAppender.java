package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import org.duckdb.DuckDBTimestamp;

/**
 * Java interface to DuckDB Appender.
 */
public class DuckDBAppender implements AutoCloseable {

    protected ByteBuffer appender_ref = null;

    public DuckDBAppender(DuckDBConnection con, String schemaName, String tableName) throws SQLException {
        if (con == null) {
            throw new SQLException("Invalid connection");
        }
        appender_ref = DuckDBNative.duckdb_jdbc_create_appender(con.connRef, schemaName.getBytes(UTF_8),
                                                                tableName.getBytes(UTF_8));
    }

    public DuckDBAppender beginRow() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_begin_row(appender_ref);
        return this;
    }

    public DuckDBAppender endRow() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_end_row(appender_ref);
        return this;
    }

    public DuckDBAppender flush() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_flush(appender_ref);
        return this;
    }

    // append primitives

    public DuckDBAppender append(boolean value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_boolean(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(char value) throws SQLException {
        String str = String.valueOf(value);
        append(str);
        return this;
    }

    public DuckDBAppender append(byte value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_byte(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(short value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_short(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(int value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_int(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(long value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_long(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(float value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_float(appender_ref, value);
        return this;
    }

    public DuckDBAppender append(double value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_double(appender_ref, value);
        return this;
    }

    // append primitive wrappers and decimal

    public DuckDBAppender append(Boolean value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.booleanValue());
    }

    public DuckDBAppender append(Byte value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.byteValue());
    }

    public DuckDBAppender append(Character value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.charValue());
    }

    public DuckDBAppender append(Short value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.shortValue());
    }

    public DuckDBAppender append(Integer value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.intValue());
    }

    public DuckDBAppender append(Long value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.longValue());
    }

    public DuckDBAppender append(Float value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.floatValue());
    }

    public DuckDBAppender append(Double value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.doubleValue());
    }

    public DuckDBAppender append(BigDecimal value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        DuckDBNative.duckdb_jdbc_appender_append_decimal(appender_ref, value);
        return this;
    }

    // append arrays

    public DuckDBAppender append(boolean[] values) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(boolean[] values, boolean[] nullMask) throws SQLException {
        // todo
        return this;
    }

    public DuckDBAppender append(byte[] blobBody) throws SQLException {
        return appendBlob(blobBody);
    }

    public DuckDBAppender appendBlob(byte[] blobBody) throws SQLException {
        if (blobBody == null) {
            return appendNull();
        }
        DuckDBNative.duckdb_jdbc_appender_append_bytes(appender_ref, blobBody);
        return this;
    }

    public DuckDBAppender appendByteArray(byte[] values, boolean[] nullMask) throws SQLException {
        // todo
        return this;
    }

    public DuckDBAppender append(char[] characters) throws SQLException {
        return appendCharacters(characters);
    }

    public DuckDBAppender appendCharacters(char[] characters) throws SQLException {
        if (characters == null) {
            return appendNull();
        }
        String str = String.valueOf(characters);
        return append(str);
    }

    public DuckDBAppender appendCharArray(char[] values, boolean[] nullMask) throws SQLException {
        // todo
        return this;
    }

    public DuckDBAppender append(short[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(short[] values, boolean[] nullMask) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(int[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(int[] values, boolean[] nullMask) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(long[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(long[] values, boolean[] nullMask) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(float[] values) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(float[] values, boolean[] nullMask) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(double[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(double[] values, boolean[] nullMask) throws SQLException {
        if (values == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(Object[] value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    // append objects

    public DuckDBAppender append(String value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        byte[] bytes = value.getBytes(UTF_8);
        DuckDBNative.duckdb_jdbc_appender_append_string(appender_ref, bytes);
        return this;
    }

    public DuckDBAppender appendTimestampMicros(long micros) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_timestamp(appender_ref, micros);
        return this;
    }

    public DuckDBAppender append(LocalDateTime value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        long micros = DuckDBTimestamp.localDateTime2Micros(value);
        return appendTimestampMicros(micros);
    }

    public DuckDBAppender appendEpochDays(int days) throws SQLException {
        // todo
        //        DuckDBNative.duckdb_jdbc_appender_append_date(appender_ref, days);
        return this;
    }

    public DuckDBAppender append(LocalDate value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new SQLException("Unsupported number of days: " + days + ", must fit into 'int32_t'");
        }
        return appendEpochDays((int) days);
    }

    public DuckDBAppender append(LocalTime value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(OffsetDateTime value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(OffsetTime value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    public DuckDBAppender append(java.util.Date value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        // todo
        return this;
    }

    // append special

    public DuckDBAppender appendNull() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_null(appender_ref);
        return this;
    }

    public DuckDBAppender appendDefault() throws SQLException {
        // todo
        return this;
    }

    // append deprecated

    @Deprecated // use append(BigDecimal value)
    public DuckDBAppender appendBigDecimal(BigDecimal value) throws SQLException {
        append(value);
        return this;
    }

    @Deprecated // use append(LocalDateTime value)
    public DuckDBAppender appendLocalDateTime(LocalDateTime value) throws SQLException {
        append(value);
        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        close();
    }

    public synchronized void close() throws SQLException {
        if (appender_ref != null) {
            DuckDBNative.duckdb_jdbc_appender_close(appender_ref);
            appender_ref = null;
        }
    }
}

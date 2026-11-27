package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.JdbcUtils.createSQLException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * An appender of the rows that are appended one value at a time. The values are collected into a data chunk that is
 * appended to the table when it is full or the appender is flushed.
 */
@Deprecated
public class DuckDBSingleValueAppender implements AutoCloseable {

    private final DuckDBConnection conn;
    private DuckDBV2Appender appender;
    private ByteBuffer chunk;
    private ByteBuffer[] vectors;
    private long capacity;
    // the index of the row that is being appended, and of the column to append to
    private long row = 0;
    private int column = 0;

    public DuckDBSingleValueAppender(DuckDBConnection con, String schemaName, String tableName) throws SQLException {
        if (con == null) {
            throw createSQLException("Invalid connection", ErrorCode.SINGLE_VALUE_BAD_CONNECTION);
        }
        this.conn = con;
        String schema = schemaName == null || schemaName.isEmpty() ? null : schemaName;
        appender = DuckDBV2Appender.create(con, null, schema, tableName);

        int columnCount = (int) appender.columnCount();
        ByteBuffer[] types = new ByteBuffer[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                types[i] = appender.columnType(i);
            }
            chunk = duckdb_create_data_chunk(types);
        } finally {
            for (ByteBuffer type : types) {
                if (type != null) {
                    duckdb_destroy_logical_type(type);
                }
            }
        }
        vectors = new ByteBuffer[columnCount];
        for (int i = 0; i < columnCount; i++) {
            vectors[i] = duckdb_data_chunk_get_vector(chunk, i);
        }
        capacity = duckdb_vector_size();
        resetChunk();
    }

    private void checkOpen() throws SQLException {
        if (appender == null) {
            throw new SQLException("Invalid Input Error: Invalid appender");
        }
    }

    private void resetChunk() throws SQLException {
        duckdb_data_chunk_reset(chunk);
        for (ByteBuffer vec : vectors) {
            duckdb_v2_vector_set_size(vec, capacity);
        }
        row = 0;
        column = 0;
    }

    // Appends the collected rows to the buffer of the appender
    private void collect() throws SQLException {
        if (row == 0) {
            return;
        }
        for (ByteBuffer vec : vectors) {
            duckdb_v2_vector_set_size(vec, row);
        }
        duckdb_data_chunk_set_size(chunk, row);
        int state = appender.appendChunk(chunk);
        String error = new String(appender.error(), UTF_8);
        resetChunk();
        if (state != 0) {
            throw new SQLException(error);
        }
    }

    private void flushAppender() throws SQLException {
        if (appender.flush() != 0) {
            throw new SQLException(new String(appender.error(), UTF_8));
        }
    }

    public void beginRow() throws SQLException {
        // the values are appended to the columns in order, no matter how many times the row is begun
        checkOpen();
    }

    public void endRow() throws SQLException {
        checkOpen();
        if (column != vectors.length) {
            // the values of the incomplete row are dropped
            column = 0;
            throw new SQLException("Invalid Input Error: Call to EndRow before all columns have been appended to");
        }
        column = 0;
        row++;
        if (row == capacity) {
            collect();
        }
    }

    public void flush() throws SQLException {
        checkOpen();
        collect();
        flushAppender();
    }

    // Appends the value to the column, the value is cast to the type of the column, the value is destroyed
    private void appendValue(ByteBuffer value) throws SQLException {
        try {
            if (column >= vectors.length) {
                throw new SQLException("Invalid Input Error: Too many appends for chunk!");
            }
            duckdb_v2_vector_set_value(vectors[column], row, value);
            column++;
        } finally {
            duckdb_v2_value_destroy(value);
        }
    }

    private ByteBuffer factory() throws SQLException {
        return duckdb_v2_connection_get_factory(duckdb_v2_connection_from_ref(conn.connRef));
    }

    private void appendObject(Object value) throws SQLException {
        checkOpen();
        appendValue(DuckDBCore.toValue(factory(), value));
    }

    public void append(boolean value) throws SQLException {
        appendObject(value);
    }

    public void append(byte value) throws SQLException {
        appendObject(value);
    }

    public void append(short value) throws SQLException {
        appendObject(value);
    }

    public void append(int value) throws SQLException {
        appendObject(value);
    }

    public void append(long value) throws SQLException {
        appendObject(value);
    }

    // New naming schema for object params to keep compatibility with calling "append(null)"
    public void appendLocalDateTime(LocalDateTime value) throws SQLException {
        checkOpen();
        if (value == null) {
            appendValue(DuckDBCore.nullValue(factory()));
        } else {
            long timeInMicros = DuckDBTimestamp.localDateTime2Micros(value);
            appendValue(duckdb_v2_value_create_timestamp(factory(), timeInMicros));
        }
    }

    public void appendBigDecimal(BigDecimal value) throws SQLException {
        appendObject(value);
    }

    public void append(float value) throws SQLException {
        appendObject(value);
    }

    public void append(double value) throws SQLException {
        appendObject(value);
    }

    public void append(String value) throws SQLException {
        appendObject(value);
    }

    public void append(byte[] value) throws SQLException {
        appendObject(value);
    }

    public synchronized void close() throws SQLException {
        if (appender == null) {
            return;
        }
        DuckDBV2Appender closing = appender;
        appender = null;
        try {
            if (row > 0) {
                for (ByteBuffer vec : vectors) {
                    duckdb_v2_vector_set_size(vec, row);
                }
                duckdb_data_chunk_set_size(chunk, row);
                if (closing.appendChunk(chunk) != 0) {
                    throw new SQLException(new String(closing.error(), UTF_8));
                }
            }
            if (closing.close() != 0) {
                throw new SQLException(new String(closing.error(), UTF_8));
            }
        } finally {
            duckdb_destroy_data_chunk(chunk);
            closing.destroy();
        }
    }
}

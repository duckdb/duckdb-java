package org.duckdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DuckDBResultSet implements ResultSet {
    private final DuckDBConnection conn;
    private final DuckDBPreparedStatement stmt;
    private final DuckDBResultSetMetaData meta;

    /**
     * {@code null} if this result set is closed.
     */
    private ByteBuffer resultRef;
    private final Lock resultRefLock = new ReentrantLock();

    private DuckDBVector[] currentChunk = {};
    private int chunkIdx = 0;
    private boolean finished = false;
    private boolean wasNull;

    public DuckDBResultSet(DuckDBConnection conn, DuckDBPreparedStatement stmt, DuckDBResultSetMetaData meta,
                           ByteBuffer resultRef) throws SQLException {
        try {
            this.conn = Objects.requireNonNull(conn);
            this.stmt = Objects.requireNonNull(stmt);
            this.resultRef = Objects.requireNonNull(resultRef);
            this.meta = Objects.requireNonNull(meta);
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    public Statement getStatement() throws SQLException {
        checkOpen();
        return stmt;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return meta;
    }

    public boolean next() throws SQLException {
        checkOpen();
        if (finished) {
            return false;
        }
        chunkIdx++;
        if (currentChunk.length == 0 || chunkIdx > currentChunk[0].length) {
            currentChunk = fetchChunk();
            chunkIdx = 1;
        }
        if (currentChunk.length == 0) {
            finished = true;
            return false;
        }
        return true;
    }

    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        resultRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }
            DuckDBNative.duckdb_jdbc_free_result(resultRef);
            // Nullness is used to determine whether we're closed
            resultRef = null;
        } finally {
            resultRefLock.unlock();
        }

        // isCloseOnCompletion() throws if already closed, and we can't check for isClosed() because it could change
        // between when we check and call isCloseOnCompletion, so access the field directly.
        if (stmt.closeOnCompletion) {
            stmt.close();
        }
    }

    protected void finalize() throws Throwable {
        close();
    }

    public boolean isClosed() throws SQLException {
        return resultRef == null;
    }

    private void check(int columnIndex) throws SQLException {
        checkOpen();
        if (columnIndex < 1 || columnIndex > meta.column_count) {
            throw new SQLException("Column index out of bounds");
        }
    }

    /**
     * Export the result set as an ArrowReader
     *
     * @param arrow_buffer_allocator an instance of {@link org.apache.arrow.memory.BufferAllocator}
     * @param arrow_batch_size batch size of arrow vectors to return
     * @return an instance of {@link org.apache.arrow.vector.ipc.ArrowReader}
     */
    public synchronized Object arrowExportStream(Object arrow_buffer_allocator, long arrow_batch_size)
        throws SQLException {
        checkOpen();

        try {
            Class<?> buffer_allocator_class = Class.forName("org.apache.arrow.memory.BufferAllocator");
            if (!buffer_allocator_class.isInstance(arrow_buffer_allocator)) {
                throw new RuntimeException("Need to pass an Arrow BufferAllocator");
            }
            Long stream_pointer = DuckDBNative.duckdb_jdbc_arrow_stream(resultRef, arrow_batch_size);
            Class<?> arrow_array_stream_class = Class.forName("org.apache.arrow.c.ArrowArrayStream");
            Object arrow_array_stream =
                arrow_array_stream_class.getMethod("wrap", long.class).invoke(null, stream_pointer);

            Class<?> c_data_class = Class.forName("org.apache.arrow.c.Data");

            return c_data_class.getMethod("importArrayStream", buffer_allocator_class, arrow_array_stream_class)
                .invoke(null, arrow_buffer_allocator, arrow_array_stream);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Object getObject(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getObject(chunkIdx - 1);
    }

    public Struct getStruct(int columnIndex) throws SQLException {
        return checkAndNull(columnIndex) ? null : currentChunk[columnIndex - 1].getStruct(chunkIdx - 1);
    }

    public OffsetTime getOffsetTime(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getOffsetTime(chunkIdx - 1);
    }

    public boolean wasNull() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was closed");
        }
        return wasNull;
    }

    private boolean checkAndNull(int columnIndex) throws SQLException {
        check(columnIndex);
        try {
            wasNull = currentChunk[columnIndex - 1].check_and_null(chunkIdx - 1);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new SQLException("No row in context", e);
        }
        return wasNull;
    }

    public JsonNode getJsonObject(int columnIndex) throws SQLException {
        String result = getLazyString(columnIndex);
        return result == null ? null : new JsonNode(result);
    }

    public String getLazyString(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getLazyString(chunkIdx - 1);
    }

    public String getString(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }

        Object res = getObject(columnIndex);
        if (res == null) {
            return null;
        } else if (res instanceof Blob && "GEOMETRY".equalsIgnoreCase(meta.column_types_string[columnIndex - 1])) {
            return DuckDBGeometryDeserializer.deserializeToWKT((Blob) res);
        } else {
            return res.toString();
        }
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return false;
        }
        return currentChunk[columnIndex - 1].getBoolean(chunkIdx - 1);
    }

    public byte getByte(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getByte(chunkIdx - 1);
    }

    public short getShort(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getShort(chunkIdx - 1);
    }

    public int getInt(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getInt(chunkIdx - 1);
    }

    private short getUint8(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getUint8(chunkIdx - 1);
    }

    private int getUint16(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getUint16(chunkIdx - 1);
    }

    private long getUint32(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getUint32(chunkIdx - 1);
    }

    private BigInteger getUint64(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return BigInteger.ZERO;
        }
        return currentChunk[columnIndex - 1].getUint64(chunkIdx - 1);
    }

    public long getLong(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return 0;
        }
        return currentChunk[columnIndex - 1].getLong(chunkIdx - 1);
    }

    public BigInteger getHugeint(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return BigInteger.ZERO;
        }
        return currentChunk[columnIndex - 1].getHugeint(chunkIdx - 1);
    }

    public BigInteger getUhugeint(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return BigInteger.ZERO;
        }
        return currentChunk[columnIndex - 1].getUhugeint(chunkIdx - 1);
    }

    public float getFloat(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return Float.NaN;
        }
        return currentChunk[columnIndex - 1].getFloat(chunkIdx - 1);
    }

    public double getDouble(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return Double.NaN;
        }
        return currentChunk[columnIndex - 1].getDouble(chunkIdx - 1);
    }

    public int findColumn(String columnLabel) throws SQLException {
        checkOpen();
        for (int col_idx = 0; col_idx < meta.column_count; col_idx++) {
            if (meta.column_names[col_idx].equalsIgnoreCase(columnLabel)) {
                return col_idx + 1;
            }
        }
        throw new SQLException("Could not find column with label " + columnLabel);
    }

    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }

        return currentChunk[columnIndex - 1].getBytes(chunkIdx - 1);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return checkAndNull(columnIndex) ? null : currentChunk[columnIndex - 1].getDate(chunkIdx - 1);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getTimestamp(chunkIdx - 1);
    }

    private LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getLocalDateTime(chunkIdx - 1);
    }

    private OffsetDateTime getOffsetDateTime(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getOffsetDateTime(chunkIdx - 1);
    }

    public UUID getUuid(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getUuid(chunkIdx - 1);
    }

    public static class DuckDBBlobResult implements Blob {

        static class ByteBufferBackedInputStream extends InputStream {

            ByteBuffer buf;

            public ByteBufferBackedInputStream(ByteBuffer buf) {
                this.buf = buf;
            }

            public int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get() & 0xFF;
            }

            public int read(byte[] bytes, int off, int len) throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }

                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        }

        public DuckDBBlobResult(ByteBuffer buffer_p) {
            buffer_p.position(0);
            buffer_p.order(ByteOrder.LITTLE_ENDIAN);
            this.buffer = buffer_p;
        }

        public InputStream getBinaryStream() {
            return getBinaryStream(0, length());
        }

        public InputStream getBinaryStream(long pos, long length) {
            return new ByteBufferBackedInputStream(buffer);
        }

        @Override
        public byte[] getBytes(long pos, int length) throws SQLException {
            if (pos < 1 || length < 0) {
                throw new SQLException("Invalid position or length");
            }
            byte[] bytes = new byte[length];
            buffer.position((int) pos - 1);
            buffer.get(bytes, 0, length);
            return bytes;
        }

        public long position(Blob pattern, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("position");
        }

        public long position(byte[] pattern, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("position");
        }

        public long length() {
            return buffer.capacity();
        }

        public void free() {
            // nop
        }

        public OutputStream setBinaryStream(long pos) throws SQLException {
            throw new SQLFeatureNotSupportedException("setBinaryStream");
        }

        public void truncate(long length) throws SQLException {
            throw new SQLFeatureNotSupportedException("truncate");
        }

        public int setBytes(long pos, byte[] bytes) throws SQLException {
            throw new SQLFeatureNotSupportedException("setBytes");
        }

        public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
            throw new SQLFeatureNotSupportedException("setBytes");
        }

        @Override
        public String toString() {
            return "DuckDBBlobResult{"
                + "buffer=" + buffer + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            DuckDBBlobResult that = (DuckDBBlobResult) o;
            return Objects.equals(buffer, that.buffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(buffer);
        }

        private final ByteBuffer buffer;
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getBlob(chunkIdx - 1);
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream");
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream");
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream");
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream");
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("getWarnings");
    }

    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearWarnings");
    }

    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName");
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getBigDecimal(chunkIdx - 1);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("isBeforeFirst");
    }

    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isAfterLast");
    }

    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("isFirst");
    }

    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isLast");
    }

    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("beforeFirst");
    }

    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("afterLast");
    }

    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("first");
    }

    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("last");
    }

    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("getRow");
    }

    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("absolute");
    }

    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("relative");
    }

    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("previous");
    }

    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLFeatureNotSupportedException("setFetchDirection");
        }
    }

    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Fetch size has to be >= 0");
        }
    }

    public int getFetchSize() throws SQLException {
        checkOpen();
        return DuckDBNative.duckdb_jdbc_fetch_size();
    }

    public int getType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowUpdated");
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowInserted");
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowDeleted");
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("insertRow");
    }

    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRow");
    }

    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("deleteRow");
    }

    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates");
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToInsertRow");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow");
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    public Array getArray(int columnIndex) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getArray(chunkIdx - 1);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getTime(chunkIdx - 1, cal);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        if (checkAndNull(columnIndex)) {
            return null;
        }
        return currentChunk[columnIndex - 1].getTimestamp(chunkIdx - 1, cal);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability");
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    private boolean isTimestamp(DuckDBColumnType sqlType) {
        return (sqlType == DuckDBColumnType.TIMESTAMP || sqlType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkOpen();

        if (type == null) {
            throw new SQLException("type is null");
        }

        if (checkAndNull(columnIndex)) {
            return null;
        }

        DuckDBColumnType sqlType = meta.column_types[columnIndex - 1];
        if (type == BigDecimal.class) {
            if (sqlType == DuckDBColumnType.DECIMAL) {
                return type.cast(getBigDecimal(columnIndex));
            } else if (sqlType == DuckDBColumnType.FLOAT) {
                return type.cast(BigDecimal.valueOf(getFloat(columnIndex)));
            } else if (sqlType == DuckDBColumnType.DOUBLE) {
                return type.cast(BigDecimal.valueOf(getDouble(columnIndex)));
            } else if (sqlType == DuckDBColumnType.HUGEINT) {
                return type.cast(new BigDecimal(getHugeint(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UHUGEINT) {
                return type.cast(new BigDecimal(getUhugeint(columnIndex)));
            } else if (sqlType == DuckDBColumnType.BIGINT) {
                return type.cast(BigDecimal.valueOf(getLong(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UBIGINT) {
                return type.cast(new BigDecimal(getUint64(columnIndex)));
            } else if (sqlType == DuckDBColumnType.INTEGER) {
                return type.cast(BigDecimal.valueOf(getInt(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UINTEGER) {
                return type.cast(BigDecimal.valueOf(getUint32(columnIndex)));
            } else if (sqlType == DuckDBColumnType.SMALLINT) {
                return type.cast(BigDecimal.valueOf(getShort(columnIndex)));
            } else if (sqlType == DuckDBColumnType.USMALLINT) {
                return type.cast(BigDecimal.valueOf(getUint16(columnIndex)));
            } else if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast(BigDecimal.valueOf(getByte(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UTINYINT) {
                return type.cast(BigDecimal.valueOf(getUint8(columnIndex)));
            } else {
                throw new SQLException("Can't convert value to BigDecimal, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == String.class) {
            if (sqlType == DuckDBColumnType.VARCHAR || sqlType == DuckDBColumnType.ENUM) {
                return type.cast(getString(columnIndex));
            } else {
                throw new SQLException("Can't convert value to String, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Boolean.class) {
            if (sqlType == DuckDBColumnType.BOOLEAN) {
                return type.cast(getBoolean(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Boolean, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Byte.class) {
            if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast(getByte(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Byte, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Short.class) {
            if (sqlType == DuckDBColumnType.SMALLINT) {
                return type.cast(getShort(columnIndex));
            } else if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast((short) getByte(columnIndex));
            } else if (sqlType == DuckDBColumnType.UTINYINT) {
                return type.cast(getUint8(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Short, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Integer.class) {
            if (sqlType == DuckDBColumnType.INTEGER) {
                return type.cast(getInt(columnIndex));
            } else if (sqlType == DuckDBColumnType.SMALLINT) {
                return type.cast((int) getShort(columnIndex));
            } else if (sqlType == DuckDBColumnType.USMALLINT) {
                return type.cast(getUint16(columnIndex));
            } else if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast((int) getByte(columnIndex));
            } else if (sqlType == DuckDBColumnType.UTINYINT) {
                return type.cast((int) getUint8(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Integer, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Long.class) {
            if (sqlType == DuckDBColumnType.BIGINT || isTimestamp(sqlType)) {
                return type.cast(getLong(columnIndex));
            } else if (sqlType == DuckDBColumnType.INTEGER) {
                return type.cast((long) getInt(columnIndex));
            } else if (sqlType == DuckDBColumnType.UINTEGER) {
                return type.cast(getUint32(columnIndex));
            } else if (sqlType == DuckDBColumnType.SMALLINT) {
                return type.cast((long) getShort(columnIndex));
            } else if (sqlType == DuckDBColumnType.USMALLINT) {
                return type.cast((long) getUint16(columnIndex));
            } else if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast((long) getByte(columnIndex));
            } else if (sqlType == DuckDBColumnType.UTINYINT) {
                return type.cast((long) getUint8(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Long, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Float.class) {
            if (sqlType == DuckDBColumnType.FLOAT) {
                return type.cast(getFloat(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Float, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Double.class) {
            if (sqlType == DuckDBColumnType.DOUBLE) {
                return type.cast(getDouble(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Double, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Date.class) {
            if (sqlType == DuckDBColumnType.DATE) {
                return type.cast(getDate(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Date, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Time.class) {
            if (sqlType == DuckDBColumnType.TIME) {
                return type.cast(getTime(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Time, Java type: " + type + ", SQL type: " + sqlType);
            }
        } else if (type == Timestamp.class) {
            if (isTimestamp(sqlType)) {
                return type.cast(getTimestamp(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Timestamp, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == LocalDate.class) {
            if (sqlType == DuckDBColumnType.DATE) {
                final Date date = getDate(columnIndex);
                if (date == null) {
                    return null;
                }
                return type.cast(date.toLocalDate());
            } else {
                throw new SQLException("Can't convert value to LocalDate, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == LocalDateTime.class) {
            if (isTimestamp(sqlType)) {
                return type.cast(getLocalDateTime(columnIndex));
            } else {
                throw new SQLException("Can't convert value to LocalDateTime, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == BigInteger.class) {
            if (sqlType == DuckDBColumnType.HUGEINT) {
                return type.cast(getHugeint(columnIndex));
            } else if (sqlType == DuckDBColumnType.UHUGEINT) {
                return type.cast(getUhugeint(columnIndex));
            } else if (sqlType == DuckDBColumnType.BIGINT) {
                return type.cast(BigInteger.valueOf(getLong(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UBIGINT) {
                return type.cast(getUint64(columnIndex));
            } else if (sqlType == DuckDBColumnType.INTEGER) {
                return type.cast(BigInteger.valueOf(getInt(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UINTEGER) {
                return type.cast(BigInteger.valueOf(getUint32(columnIndex)));
            } else if (sqlType == DuckDBColumnType.SMALLINT) {
                return type.cast(BigInteger.valueOf(getShort(columnIndex)));
            } else if (sqlType == DuckDBColumnType.USMALLINT) {
                return type.cast(BigInteger.valueOf(getUint16(columnIndex)));
            } else if (sqlType == DuckDBColumnType.TINYINT) {
                return type.cast(BigInteger.valueOf(getByte(columnIndex)));
            } else if (sqlType == DuckDBColumnType.UTINYINT) {
                return type.cast(BigInteger.valueOf(getUint8(columnIndex)));
            } else {
                throw new SQLException("Can't convert value to BigInteger, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == OffsetDateTime.class) {
            if (sqlType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
                return type.cast(getOffsetDateTime(columnIndex));
            } else {
                throw new SQLException("Can't convert value to OffsetDateTime, Java type: " + type +
                                       ", SQL type: " + sqlType);
            }
        } else if (type == Blob.class) {
            if (sqlType == DuckDBColumnType.BLOB) {
                throw new SQLException("Can't convert value to Blob, Java type: " + type + ", SQL type: " + sqlType);
                // return type.cast(getLocalDateTime(columnIndex));
            } else {
                throw new SQLException("Can't convert value to Blob, SQL type: " + sqlType);
            }
        } else if (type == UUID.class) {
            if (sqlType == DuckDBColumnType.UUID || sqlType == DuckDBColumnType.VARCHAR) {
                return type.cast(getUuid(columnIndex));
            } else {
                throw new SQLException("Can't convert value to UUID, SQL type: " + sqlType);
            }
        } else {
            throw new SQLException("Can't convert value to " + type + ", SQL type: " + sqlType);
        }
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type is null");
        }
        if (columnLabel == null || columnLabel.isEmpty()) {
            throw new SQLException("columnLabel is null");
        }

        int index = findColumn(columnLabel);
        return getObject(index, type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    boolean isFinished() {
        return finished;
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was closed");
        }
    }

    private DuckDBVector[] fetchChunk() throws SQLException {
        // Take both result set and connection locks for fetching,
        // connection lock must be taken first because concurrent
        // rs#close() call can be initiated from conn#close()
        // that holds connection lock.
        conn.connRefLock.lock();
        try {
            conn.checkOpen();
            resultRefLock.lock();
            try {
                checkOpen();
                return DuckDBNative.duckdb_jdbc_fetch(resultRef, conn.connRef);
            } finally {
                resultRefLock.unlock();
            }
        } catch (SQLException e) {
            close();
            throw e;
        } finally {
            conn.connRefLock.unlock();
        }
    }
}

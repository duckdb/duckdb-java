package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DuckDBCAPIAppender implements AutoCloseable {

    private static final Set<Integer> supportedTypes = new LinkedHashSet<>();
    static {
        supportedTypes.add(DUCKDB_TYPE_TINYINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_SMALLINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_INTEGER.typeId);
        supportedTypes.add(DUCKDB_TYPE_BIGINT.typeId);

        supportedTypes.add(DUCKDB_TYPE_FLOAT.typeId);
        supportedTypes.add(DUCKDB_TYPE_DOUBLE.typeId);

        supportedTypes.add(DUCKDB_TYPE_VARCHAR.typeId);
        supportedTypes.add(DUCKDB_TYPE_DATE.typeId);
    }

    private final DuckDBConnection conn;

    private final String catalog;
    private final String schema;
    private final String table;

    private final long maxRows;

    private ByteBuffer appenderRef;
    private final Lock appenderRefLock = new ReentrantLock();

    private final ByteBuffer chunkRef;
    private final Column[] columns;

    private long rowIdx = 0;
    private int colIdx = 0;

    DuckDBCAPIAppender(DuckDBConnection conn, String catalog, String schema, String table) throws SQLException {
        this.conn = conn;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;

        this.maxRows = duckdb_vector_size();

        ByteBuffer appenderRef = null;
        ByteBuffer[] colTypes = null;
        ByteBuffer chunkRef = null;
        Column[] vectors = null;
        try {
            appenderRef = createAppender(conn, catalog, schema, table);
            colTypes = readTableTypes(appenderRef);
            chunkRef = createChunk(colTypes);
            vectors = createVectors(chunkRef, colTypes);
        } catch (Exception e) {
            if (null != chunkRef) {
                duckdb_destroy_data_chunk(chunkRef);
            }
            if (null != colTypes) {
                for (ByteBuffer ct : colTypes) {
                    if (null != ct) {
                        duckdb_destroy_logical_type(ct);
                    }
                }
            }
            if (null != appenderRef) {
                duckdb_appender_destroy(appenderRef);
            }
            throw new SQLException(createErrMsg(e.getMessage()));
        }

        this.appenderRef = appenderRef;
        this.chunkRef = chunkRef;
        this.columns = vectors;
    }

    public DuckDBCAPIAppender beginRow() throws SQLException {
        checkOpen();
        if (0 != colIdx) {
            throw new SQLException(createErrMsg("'endRow' must be called before adding next row"));
        }
        return this;
    }

    public DuckDBCAPIAppender endRow() throws SQLException {
        checkOpen();
        if (columns.length != colIdx) {
            throw new SQLException(createErrMsg("'endRow' can be called only after adding all columns"));
        }
        rowIdx++;
        if (rowIdx >= maxRows) {
            try {
                flush();
            } catch (SQLException e) {
                rowIdx--;
                throw e;
            }
        }
        colIdx = 0;
        return this;
    }

    public void flush() throws SQLException {
        checkOpen();

        if (0 == rowIdx) {
            return;
        }

        appenderRefLock.lock();
        try {
            checkOpen();

            duckdb_data_chunk_set_size(chunkRef, rowIdx);

            int appendState = duckdb_append_data_chunk(appenderRef, chunkRef);
            if (0 != appendState) {
                byte[] errorUTF8 = duckdb_appender_error(appenderRef);
                String error = null != errorUTF8 ? strFromUTF8(errorUTF8) : "";
                throw new SQLException(createErrMsg(error));
            }

            int flushState = duckdb_appender_flush(appenderRef);
            if (0 != flushState) {
                byte[] errorUTF8 = duckdb_appender_error(appenderRef);
                String error = null != errorUTF8 ? strFromUTF8(errorUTF8) : "";
                throw new SQLException(createErrMsg(error));
            }

            duckdb_data_chunk_reset(chunkRef);
            try {
                for (Column col : columns) {
                    col.reset();
                }
            } catch (SQLException e) {
                throw new SQLException(createErrMsg(e.getMessage()));
            }

            rowIdx = 0;
        } finally {
            appenderRefLock.unlock();
        }
    }

    @Override
    // todo: tracking in conn
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        appenderRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }
            if (rowIdx > 0) {
                try {
                    flush();
                } catch (SQLException e) {
                    // suppress
                }
            }
            for (Column col : columns) {
                col.destroy();
            }
            duckdb_destroy_data_chunk(chunkRef);
            duckdb_appender_close(appenderRef);
            duckdb_appender_destroy(appenderRef);

            // Untrack the appender from parent connection,
            // if 'closing' flag is set it means that the parent connection itself
            // is being closed and we don't need to untrack this instance from the connection.
            if (!conn.closing) {
                conn.connRefLock.lock();
                try {
                    conn.appenders.remove(this);
                } finally {
                    conn.connRefLock.unlock();
                }
            }

            appenderRef = null;
        } finally {
            appenderRefLock.unlock();
        }
    }

    public boolean isClosed() throws SQLException {
        return appenderRef == null;
    }

    // append primitives

    public DuckDBCAPIAppender append(char value) throws SQLException {
        String str = String.valueOf(value);
        append(str);
        return this;
    }

    public DuckDBCAPIAppender append(byte value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_TINYINT);
        col.data.put(value);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(short value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_SMALLINT);
        col.data.putShort(value);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(int value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_INTEGER);
        col.data.putInt(value);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(long value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_BIGINT);
        col.data.putLong(value);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(float value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_FLOAT);
        col.data.putFloat(value);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(double value) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_DOUBLE);
        col.data.putDouble(value);
        colIdx++;
        return this;
    }

    // append primitive wrappers and decimal

    public DuckDBCAPIAppender append(Character value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.charValue());
    }

    public DuckDBCAPIAppender append(Byte value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.byteValue());
    }

    public DuckDBCAPIAppender append(Short value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.shortValue());
    }

    public DuckDBCAPIAppender append(Integer value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.intValue());
    }

    public DuckDBCAPIAppender append(Long value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.longValue());
    }

    public DuckDBCAPIAppender append(Float value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.floatValue());
    }

    public DuckDBCAPIAppender append(Double value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        return append(value.doubleValue());
    }

    // append arrays

    public DuckDBCAPIAppender append(char[] characters) throws SQLException {
        return appendCharacters(characters);
    }

    public DuckDBCAPIAppender appendCharacters(char[] characters) throws SQLException {
        if (characters == null) {
            return appendNull();
        }
        String str = String.valueOf(characters);
        return append(str);
    }

    // append objects

    public DuckDBCAPIAppender append(String value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        Column col = prepareColumnForWriting(DUCKDB_TYPE_VARCHAR);
        appenderRefLock.lock();
        try {
            checkOpen();
            byte[] bytes = value.getBytes(UTF_8);
            duckdb_vector_assign_string_element_len(col.vectorRef, rowIdx, bytes);
        } finally {
            appenderRefLock.unlock();
        }
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender appendEpochDays(int days) throws SQLException {
        Column col = prepareColumnForWriting(DUCKDB_TYPE_DATE);
        col.data.putInt(days);
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender append(LocalDate value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new SQLException(createErrMsg("unsupported number of days: " + days + ", must fit into 'int32_t'"));
        }
        return appendEpochDays((int) days);
    }

    // append special

    public DuckDBCAPIAppender appendNull() throws SQLException {
        Column col = prepareColumnForWriting(null);
        appenderRefLock.lock();
        try {
            checkOpen();
            col.setNull(rowIdx);
        } finally {
            appenderRefLock.unlock();
        }
        colIdx++;
        return this;
    }

    public DuckDBCAPIAppender appendDefault() throws SQLException {
        prepareColumnForWriting(null);
        appenderRefLock.lock();
        try {
            checkOpen();
            duckdb_append_default_to_chunk(appenderRef, chunkRef, colIdx, rowIdx);
        } finally {
            appenderRefLock.unlock();
        }
        colIdx++;
        return this;
    }

    private String createErrMsg(String error) {
        return "Appender error"
            + ", catalog: '" + catalog + "'"
            + ", schema: '" + schema + "'"
            + ", table: '" + table + "'"
            + ", message: " + (null != error ? error : "N/A");
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException(createErrMsg("appender was closed"));
        }
    }

    private static byte[] utf8(String str) {
        if (null == str) {
            return null;
        }
        return str.getBytes(UTF_8);
    }

    private static String strFromUTF8(byte[] utf8) {
        if (null == utf8) {
            return null;
        }
        return new String(utf8, UTF_8);
    }

    private static ByteBuffer createAppender(DuckDBConnection conn, String catalog, String schema, String table)
        throws SQLException {
        conn.checkOpen();
        Lock connRefLock = conn.connRefLock;
        connRefLock.lock();
        try {
            ByteBuffer[] out = new ByteBuffer[1];
            int state = duckdb_appender_create_ext(conn.connRef, utf8(catalog), utf8(schema), utf8(table), out);
            if (0 != state) {
                throw new SQLException("duckdb_appender_create_ext error");
            }
            return out[0];
        } finally {
            connRefLock.unlock();
        }
    }

    private static ByteBuffer[] readTableTypes(ByteBuffer appenderRef) throws SQLException {
        long colCountLong = duckdb_appender_column_count(appenderRef);
        if (colCountLong > Integer.MAX_VALUE || colCountLong < 0) {
            throw new SQLException("invalid columns count: " + colCountLong);
        }
        int colCount = (int) colCountLong;

        ByteBuffer[] res = new ByteBuffer[colCount];

        for (int i = 0; i < colCount; i++) {
            ByteBuffer colType = duckdb_appender_column_type(appenderRef, i);
            if (null == colType) {
                throw new SQLException("cannot get logical type for column: " + i);
            }
            int typeId = duckdb_get_type_id(colType);
            if (!supportedTypes.contains(typeId)) {
                for (ByteBuffer lt : res) {
                    duckdb_destroy_logical_type(lt);
                }
                throw new SQLException("unsupported C API type: " + typeId);
            }
            res[i] = colType;
        }

        return res;
    }

    private static ByteBuffer createChunk(ByteBuffer[] colTypes) throws SQLException {
        ByteBuffer chunkRef = duckdb_create_data_chunk(colTypes);
        if (null == chunkRef) {
            throw new SQLException("cannot create data chunk");
        }
        return chunkRef;
    }

    @SuppressWarnings("fallthrough")
    private static void initVecChildren(Column parent) throws SQLException {
        List<ByteBuffer> children = new ArrayList<>();
        switch (parent.colType) {
        case DUCKDB_TYPE_LIST:
        case DUCKDB_TYPE_MAP: {
            ByteBuffer vec = duckdb_list_vector_get_child(parent.vectorRef);
            children.add(vec);
        }
        case DUCKDB_TYPE_STRUCT:
        case DUCKDB_TYPE_UNION: {
            long count = duckdb_struct_type_child_count(parent.colTypeRef);
            for (int i = 0; i < count; i++) {
                ByteBuffer vec = duckdb_struct_vector_get_child(parent.vectorRef, i);
                children.add(vec);
            }
        }
        case DUCKDB_TYPE_ARRAY: {
            ByteBuffer vec = duckdb_array_vector_get_child(parent.vectorRef);
            children.add(vec);
        }

            for (ByteBuffer child : children) {
                if (null == child) {
                    throw new SQLException("cannot initialize data chunk child list vector");
                }
                ByteBuffer lt = duckdb_vector_get_column_type(child);
                if (null == lt) {
                    throw new SQLException("cannot initialize data chunk child list vector type");
                }
                Column cvec = new Column(lt, child);
                parent.children.add(cvec);
            }
        }
    }

    private static Column[] createVectors(ByteBuffer chunkRef, ByteBuffer[] colTypes) throws SQLException {
        Column[] vectors = new Column[colTypes.length];
        try {
            for (int i = 0; i < colTypes.length; i++) {
                ByteBuffer vector = duckdb_data_chunk_get_vector(chunkRef, i);
                vectors[i] = new Column(colTypes[i], vector);
                colTypes[i] = null;
            }
        } catch (Exception e) {
            for (Column col : vectors) {
                if (null != col) {
                    col.destroy();
                }
            }
            throw e;
        }
        return vectors;
    }

    private Column prepareColumnForWriting(CAPIType ctype) throws SQLException {
        checkOpen();
        if (colIdx >= columns.length) {
            throw new SQLException(createErrMsg("invalid columns count, expected: " + columns.length));
        }
        Column col = columns[colIdx];
        if (null != ctype) {
            if (col.colType.typeId != ctype.typeId) {
                throw new SQLException(
                    createErrMsg("invalid columns type, expected: '" + ctype + "', actual: '" + col.colType + "'"));
            }
            long pos = rowIdx * ctype.widthBytes;
            if (pos >= col.data.capacity()) {
                throw new SQLException(createErrMsg("invalid calculated position: " + pos + ", type: " + ctype));
            }
            col.data.position((int) (pos));
        }
        return col;
    }

    private static class Column {
        private ByteBuffer colTypeRef;
        private final CAPIType colType;
        private final ByteBuffer vectorRef;
        private ByteBuffer data;
        private ByteBuffer validityRef;
        private final List<Column> children = new ArrayList<>();

        private Column(ByteBuffer colTypeRef, ByteBuffer vector) throws SQLException {
            this.colTypeRef = colTypeRef;
            int colTypeId = duckdb_get_type_id(this.colTypeRef);
            this.colType = capiTypeFromTypeId(colTypeId);

            this.vectorRef = vector;
            if (null == this.vectorRef) {
                throw new SQLException("cannot initialize data chunk vector");
            }

            this.data = duckdb_vector_get_data(this.vectorRef, colType.widthBytes);
            if (null == this.data) {
                throw new SQLException("cannot initialize data chunk vector data");
            }

            duckdb_vector_ensure_validity_writable(this.vectorRef);
            this.validityRef = duckdb_vector_get_validity(this.vectorRef);
            if (null == this.validityRef) {
                throw new SQLException("cannot initialize data chunk vector validity");
            }

            // last call in constructor
            initVecChildren(this);
        }

        void reset() throws SQLException {
            this.data = duckdb_vector_get_data(vectorRef, colType.widthBytes);
            if (null == this.data) {
                throw new SQLException("cannot reset data chunk vector data");
            }

            duckdb_vector_ensure_validity_writable(this.vectorRef);
            this.validityRef = duckdb_vector_get_validity(this.vectorRef);
            if (null == this.validityRef) {
                throw new SQLException("cannot reset data chunk vector validity");
            }

            for (Column col : children) {
                col.reset();
            }
        }

        void destroy() {
            for (Column cvec : children) {
                cvec.destroy();
            }
            children.clear();
            if (null != colTypeRef) {
                duckdb_destroy_logical_type(colTypeRef);
                colTypeRef = null;
            }
        }

        void setNull(long rowIdx) {
            duckdb_validity_set_row_validity(validityRef, rowIdx, false);
            for (Column col : children) {
                col.setNull(rowIdx);
            }
        }
    }
}

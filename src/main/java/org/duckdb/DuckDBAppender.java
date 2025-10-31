package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.*;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.DuckDBHugeInt.HUGE_INT_MAX;
import static org.duckdb.DuckDBHugeInt.HUGE_INT_MIN;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.*;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DuckDBAppender implements AutoCloseable {

    private static final Set<Integer> supportedTypes = new LinkedHashSet<>();
    static {
        supportedTypes.add(DUCKDB_TYPE_BOOLEAN.typeId);
        supportedTypes.add(DUCKDB_TYPE_TINYINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_UTINYINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_SMALLINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_USMALLINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_INTEGER.typeId);
        supportedTypes.add(DUCKDB_TYPE_UINTEGER.typeId);
        supportedTypes.add(DUCKDB_TYPE_BIGINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_UBIGINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_HUGEINT.typeId);
        supportedTypes.add(DUCKDB_TYPE_UHUGEINT.typeId);

        supportedTypes.add(DUCKDB_TYPE_FLOAT.typeId);
        supportedTypes.add(DUCKDB_TYPE_DOUBLE.typeId);

        supportedTypes.add(DUCKDB_TYPE_DECIMAL.typeId);

        supportedTypes.add(DUCKDB_TYPE_VARCHAR.typeId);
        supportedTypes.add(DUCKDB_TYPE_BLOB.typeId);

        supportedTypes.add(DUCKDB_TYPE_DATE.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIME.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIME_TZ.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIMESTAMP_S.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIMESTAMP_MS.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIMESTAMP.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIMESTAMP_TZ.typeId);
        supportedTypes.add(DUCKDB_TYPE_TIMESTAMP_NS.typeId);

        supportedTypes.add(DUCKDB_TYPE_UUID.typeId);

        supportedTypes.add(DUCKDB_TYPE_ARRAY.typeId);
        supportedTypes.add(DUCKDB_TYPE_LIST.typeId);
        supportedTypes.add(DUCKDB_TYPE_MAP.typeId);

        supportedTypes.add(DUCKDB_TYPE_STRUCT.typeId);
        supportedTypes.add(DUCKDB_TYPE_UNION.typeId);
    }
    private static final CAPIType[] int8Types = new CAPIType[] {DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_UTINYINT};
    private static final CAPIType[] int16Types = new CAPIType[] {DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_USMALLINT};
    private static final CAPIType[] int32Types = new CAPIType[] {DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UINTEGER};
    private static final CAPIType[] int64Types = new CAPIType[] {DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_UBIGINT};
    private static final CAPIType[] int128Types = new CAPIType[] {DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_UHUGEINT};
    private static final CAPIType[] timestampLocalTypes = new CAPIType[] {
        DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_TIMESTAMP_NS};
    private static final CAPIType[] timestampMicrosTypes =
        new CAPIType[] {DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_TIMESTAMP_TZ};
    private static final CAPIType[] collectionTypes = new CAPIType[] {DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_LIST};

    private static final int STRING_MAX_INLINE_BYTES = 12;

    private static final LocalDateTime EPOCH_DATE_TIME = LocalDateTime.ofEpochSecond(0, 0, UTC);

    private static final long MAX_TOP_LEVEL_ROWS = duckdb_vector_size();

    private final DuckDBConnection conn;

    private final String catalog;
    private final String schema;
    private final String table;

    private ByteBuffer appenderRef;
    private final Lock appenderRefLock = new ReentrantLock();

    private final ByteBuffer chunkRef;
    private final List<Column> columns;

    private long rowIdx = 0;

    private Column currentColumn = null;
    private Column prevColumn = null;

    private boolean writeInlinedStrings = true;

    DuckDBAppender(DuckDBConnection conn, String catalog, String schema, String table) throws SQLException {
        this.conn = conn;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;

        ByteBuffer appenderRef = null;
        ByteBuffer[] colTypes = null;
        ByteBuffer chunkRef = null;
        List<Column> cols = null;
        try {
            appenderRef = createAppender(conn, catalog, schema, table);
            colTypes = readTableTypes(appenderRef);
            chunkRef = createChunk(colTypes);
            cols = createTopLevelColumns(chunkRef, colTypes);
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
            throw new SQLException(createErrMsg(e.getMessage()), e);
        }

        this.appenderRef = appenderRef;
        this.chunkRef = chunkRef;
        this.columns = cols;
    }

    public DuckDBAppender beginRow() throws SQLException {
        checkOpen();
        if (!readyForANewRowInvariant()) {
            throw new SQLException(createErrMsg("'endRow' must be called before calling 'beginRow' again"));
        }
        if (null == columns || 0 == columns.size()) {
            throw new SQLException(createErrMsg("no columns found to append to"));
        }
        this.currentColumn = columns.get(0);
        return this;
    }

    public DuckDBAppender endRow() throws SQLException {
        checkOpen();
        if (!rowCompletedInvariant()) {
            Column topCol = currentTopLevelColumn();
            if (null != topCol) {
                throw new SQLException(
                    createErrMsg("all columns must be appended to before calling 'endRow', expected columns count: " +
                                 columns.size() + ", actual: " + (topCol.idx + 1)));
            } else {
                throw new SQLException(createErrMsg(
                    "calls to 'beginRow' and 'endRow' must be paired and cannot be interleaved with other 'begin*' and 'end*' calls"));
            }
        }

        rowIdx++;
        Column prev = prevColumn;
        this.prevColumn = null;
        if (rowIdx >= MAX_TOP_LEVEL_ROWS) {
            try {
                flush();
            } catch (SQLException e) {
                this.prevColumn = prev;
                rowIdx--;
                throw e;
            }
        }

        return this;
    }

    public DuckDBAppender beginStruct() throws SQLException {
        checkOpen();
        if (!rowBegunInvariant()) {
            throw new SQLException(createErrMsg("'beginRow' must be called before calling 'beginStruct'"));
        }
        checkCurrentColumnType(DUCKDB_TYPE_STRUCT);
        //        if (structBegunInvariant()) {
        //            throw new SQLException(createErrMsg("'endStruct' must be called before calling 'beginStruct'
        //            again"));
        //        }
        if (0 == currentColumn.children.size()) {
            throw new SQLException(createErrMsg("invalid empty struct"));
        }
        this.currentColumn = currentColumn.children.get(0);
        return this;
    }

    public DuckDBAppender endStruct() throws SQLException {
        checkOpen();
        if (!structCompletedInvariant()) {
            if (structBegunInvariant()) {
                throw new SQLException(createErrMsg(
                    "all struct fields must be appended to before calling 'endStruct', expected fields count: " +
                    currentColumn.parent.children.size() + ", actual: " + (currentColumn.idx + 1)));
            }
            throw new SQLException(createErrMsg("all struct fields must be appended to before calling 'endStruct'"));
        }
        this.prevColumn = this.prevColumn.parent;
        return this;
    }

    public DuckDBAppender beginUnion(String tag) throws SQLException {
        checkOpen();
        if (!rowBegunInvariant()) {
            throw new SQLException(createErrMsg("'beginRow' must be called before calling 'beginUnion'"));
        }
        Column col = currentColumn(DUCKDB_TYPE_UNION);
        this.currentColumn = putUnionTag(col, rowIdx, tag);
        return this;
    }

    public DuckDBAppender endUnion() throws SQLException {
        checkOpen();
        if (!unionCompletedInvariant()) {
            throw new SQLException(createErrMsg("union column must be appended to before calling 'endUnion'"));
        }
        this.prevColumn = this.prevColumn.parent;
        return this;
    }

    public long flush() throws SQLException {
        checkOpen();
        if (!readyForANewRowInvariant()) {
            throw new SQLException(createErrMsg("'endRow' must be called before calling 'flush'"));
        }

        if (0 == rowIdx) {
            return rowIdx;
        }

        appenderRefLock.lock();
        try {
            checkOpen();

            duckdb_data_chunk_set_size(chunkRef, rowIdx);

            int appendState = duckdb_append_data_chunk(appenderRef, chunkRef);
            if (0 != appendState) {
                byte[] errorUTF8 = duckdb_appender_error(appenderRef);
                String error = strFromUTF8(errorUTF8);
                throw new SQLException(createErrMsg(error));
            }

            int flushState = duckdb_appender_flush(appenderRef);
            if (0 != flushState) {
                byte[] errorUTF8 = duckdb_appender_error(appenderRef);
                String error = strFromUTF8(errorUTF8);
                throw new SQLException(createErrMsg(error));
            }

            duckdb_data_chunk_reset(chunkRef);
            try {
                for (Column col : columns) {
                    col.reset();
                }
            } catch (SQLException e) {
                throw new SQLException(createErrMsg(e.getMessage()), e);
            }

            long ret = rowIdx;
            rowIdx = 0;
            return ret;
        } finally {
            appenderRefLock.unlock();
        }
    }

    @Override
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

    public DuckDBAppender append(boolean value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_BOOLEAN);
        byte val = (byte) (value ? 1 : 0);
        putByte(col, rowIdx, val);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(char value) throws SQLException {
        String str = String.valueOf(value);
        return append(str);
    }

    public DuckDBAppender append(byte value) throws SQLException {
        Column col = currentColumn(int8Types);
        putByte(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(short value) throws SQLException {
        Column col = currentColumn(int16Types);
        putShort(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(int value) throws SQLException {
        Column col = currentColumn(int32Types);
        putInt(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(long value) throws SQLException {
        Column col = currentColumn(int64Types);
        putLong(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(float value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_FLOAT);
        putFloat(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(double value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DOUBLE);
        putDouble(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    // append primitive wrappers, int128 and decimal

    public DuckDBAppender append(Boolean value) throws SQLException {
        currentColumn(DUCKDB_TYPE_BOOLEAN);
        if (value == null) {
            return appendNull();
        }
        return append(value.booleanValue());
    }

    public DuckDBAppender append(Character value) throws SQLException {
        currentColumn(DUCKDB_TYPE_VARCHAR);
        if (value == null) {
            return appendNull();
        }
        return append(value.charValue());
    }

    public DuckDBAppender append(Byte value) throws SQLException {
        currentColumn(int8Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.byteValue());
    }

    public DuckDBAppender append(Short value) throws SQLException {
        currentColumn(int16Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.shortValue());
    }

    public DuckDBAppender append(Integer value) throws SQLException {
        currentColumn(int32Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.intValue());
    }

    public DuckDBAppender append(Long value) throws SQLException {
        currentColumn(int64Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.longValue());
    }

    public DuckDBAppender append(Float value) throws SQLException {
        currentColumn(DUCKDB_TYPE_FLOAT);
        if (value == null) {
            return appendNull();
        }
        return append(value.floatValue());
    }

    public DuckDBAppender append(Double value) throws SQLException {
        currentColumn(DUCKDB_TYPE_DOUBLE);
        if (value == null) {
            return appendNull();
        }
        return append(value.doubleValue());
    }

    public DuckDBAppender appendHugeInt(long lower, long upper) throws SQLException {
        Column col = currentColumn(int128Types);
        putHugeInt(col, rowIdx, lower, upper);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(BigInteger value) throws SQLException {
        Column col = currentColumn(int128Types);
        if (value == null) {
            return appendNull();
        }
        putBigInteger(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDecimal(short value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DECIMAL);
        checkDecimalType(col, DUCKDB_TYPE_SMALLINT);
        putDecimal(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDecimal(int value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DECIMAL);
        checkDecimalType(col, DUCKDB_TYPE_INTEGER);
        putDecimal(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDecimal(long value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DECIMAL);
        checkDecimalType(col, DUCKDB_TYPE_BIGINT);
        putDecimal(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDecimal(long lower, long upper) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DECIMAL);
        checkDecimalType(col, DUCKDB_TYPE_HUGEINT);
        putDecimal(col, rowIdx, lower, upper);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(BigDecimal value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DECIMAL);
        if (value == null) {
            return appendNull();
        }
        putDecimal(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    // append arrays

    public DuckDBAppender append(boolean[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(boolean[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, DUCKDB_TYPE_BOOLEAN);
        if (values == null) {
            return appendNull();
        }
        putBoolArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(boolean[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(boolean[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, DUCKDB_TYPE_BOOLEAN);
        if (values == null) {
            return appendNull();
        }
        putBoolArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendByteArray(byte[] values) throws SQLException {
        return appendByteArray(values, null);
    }

    public DuckDBAppender appendByteArray(byte[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, int8Types);
        if (values == null) {
            return appendNull();
        }
        putByteArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendByteArray(byte[][] values) throws SQLException {
        return appendByteArray(values, null);
    }

    public DuckDBAppender appendByteArray(byte[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, int8Types);
        if (values == null) {
            return appendNull();
        }
        putByteArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(byte[] values) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_BLOB);
        if (values == null) {
            return appendNull();
        }
        putStringOrBlob(col, rowIdx, values);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(char[] characters) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_VARCHAR);
        if (characters == null) {
            return appendNull();
        }
        String str = String.valueOf(characters);
        return append(str);
    }

    public DuckDBAppender append(short[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(short[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, int16Types);
        if (values == null) {
            return appendNull();
        }
        putShortArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(short[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(short[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, int16Types);
        if (values == null) {
            return appendNull();
        }
        putShortArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(int[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(int[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, int32Types);
        if (values == null) {
            return appendNull();
        }
        putIntArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(int[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(int[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, int32Types);
        if (values == null) {
            return appendNull();
        }
        putIntArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(long[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(long[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, int64Types);
        if (values == null) {
            return appendNull();
        }
        putLongArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(long[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(long[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, int64Types);
        if (values == null) {
            return appendNull();
        }
        putLongArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(float[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(float[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, DUCKDB_TYPE_FLOAT);
        if (values == null) {
            return appendNull();
        }
        putFloatArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(float[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(float[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, DUCKDB_TYPE_FLOAT);
        if (values == null) {
            return appendNull();
        }
        putFloatArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(double[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(double[] values, boolean[] nullMask) throws SQLException {
        Column col = currentColumn(collectionTypes);
        arrayInnerColumn(col, DUCKDB_TYPE_DOUBLE);
        if (values == null) {
            return appendNull();
        }
        putDoubleArray(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(double[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(double[][] values, boolean[][] nullMask) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_ARRAY);
        Column inner = arrayInnerColumn(col, DUCKDB_TYPE_ARRAY);
        arrayInnerColumn(inner, DUCKDB_TYPE_DOUBLE);
        if (values == null) {
            return appendNull();
        }
        putDoubleArray2D(col, rowIdx, values, nullMask);
        moveToNextColumn();
        return this;
    }

    // append objects

    public DuckDBAppender append(String value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_VARCHAR);
        if (value == null) {
            return appendNull();
        }

        byte[] bytes = value.getBytes(UTF_8);
        putStringOrBlob(col, rowIdx, bytes);

        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendUUID(long mostSigBits, long leastSigBits) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_UUID);
        putUUID(col, rowIdx, mostSigBits, leastSigBits);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(UUID value) throws SQLException {
        currentColumn(DUCKDB_TYPE_UUID);
        if (value == null) {
            return appendNull();
        }

        long mostSigBits = value.getMostSignificantBits();
        long leastSigBits = value.getLeastSignificantBits();
        return appendUUID(mostSigBits, leastSigBits);
    }

    public DuckDBAppender appendEpochDays(int days) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DATE);
        putEpochDays(col, rowIdx, days);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(LocalDate value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_DATE);
        if (value == null) {
            return appendNull();
        }
        putLocalDate(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDayMicros(long micros) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIME);
        putDayMicros(col, rowIdx, micros);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(LocalTime value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIME);
        if (value == null) {
            return appendNull();
        }
        putLocalTime(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDayMicros(long micros, int offset) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIME_TZ);
        putDayMicros(col, rowIdx, micros, offset);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(OffsetTime value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIME_TZ);
        if (value == null) {
            return appendNull();
        }
        putOffsetTime(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendEpochSeconds(long seconds) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIMESTAMP_S);
        putEpochMoment(col, rowIdx, seconds);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendEpochMillis(long millis) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIMESTAMP_MS);
        putEpochMoment(col, rowIdx, millis);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendEpochMicros(long micros) throws SQLException {
        Column col = currentColumn(timestampMicrosTypes);
        putEpochMoment(col, rowIdx, micros);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendEpochNanos(long nanos) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIMESTAMP_NS);
        putEpochMoment(col, rowIdx, nanos);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(LocalDateTime value) throws SQLException {
        Column col = currentColumn(timestampLocalTypes);
        if (value == null) {
            return appendNull();
        }
        putLocalDateTime(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(java.util.Date value) throws SQLException {
        Column col = currentColumn(timestampLocalTypes);
        if (value == null) {
            return appendNull();
        }
        putDate(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(OffsetDateTime value) throws SQLException {
        Column col = currentColumn(DUCKDB_TYPE_TIMESTAMP_TZ);
        if (value == null) {
            return appendNull();
        }
        putOffsetDateTime(col, rowIdx, value);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(Collection<?> collection) throws SQLException {
        currentColumn(collectionTypes);
        if (collection == null) {
            return appendNull();
        }
        return append(collection, collection.size());
    }

    public DuckDBAppender append(Iterable<?> iter, int count) throws SQLException {
        currentColumn(collectionTypes);
        if (iter == null) {
            return appendNull();
        }
        return append(iter.iterator(), count);
    }

    public DuckDBAppender append(Iterator<?> iter, int count) throws SQLException {
        Column parentCol = currentColumn(collectionTypes);
        if (parentCol.colType != DUCKDB_TYPE_ARRAY && parentCol.colType != DUCKDB_TYPE_LIST) {
            throw new SQLException(createErrMsg("invalid array/list column type: '" + parentCol.colType + "'"));
        }
        if (iter == null) {
            return appendNull();
        }
        Column col = parentCol.children.get(0);
        putObjectArrayOrList(col, rowIdx, iter, count);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender append(Map<?, ?> map) throws SQLException {
        Column parentCol = currentColumn(DUCKDB_TYPE_MAP);
        if (map == null) {
            return appendNull();
        }
        Column col = parentCol.children.get(0);
        putMap(col, rowIdx, map);
        moveToNextColumn();
        return this;
    }

    // append special

    public DuckDBAppender appendNull() throws SQLException {
        Column col = currentColumn();
        col.setNull(rowIdx);
        moveToNextColumn();
        return this;
    }

    public DuckDBAppender appendDefault() throws SQLException {
        Column col = currentColumn();
        appenderRefLock.lock();
        try {
            checkOpen();
            duckdb_append_default_to_chunk(appenderRef, chunkRef, col.idx, rowIdx);
        } finally {
            appenderRefLock.unlock();
        }
        moveToNextColumn();
        return this;
    }

    // options

    public boolean getWriteInlinedStrings() {
        return writeInlinedStrings;
    }

    public DuckDBAppender setWriteInlinedStrings(boolean writeInlinedStrings) {
        this.writeInlinedStrings = writeInlinedStrings;
        return this;
    }

    private String createErrMsg(String error) {
        return "Appender error"
            + ", catalog: '" + catalog + "'"
            + ", schema: '" + schema + "'"
            + ", table: '" + table + "'"
            + ", message: " + (null != error ? error : "N/A");
    }

    // next column

    private Column nextColumn(Column curCol) {
        if (null == curCol) {
            return null;
        }
        final List<Column> cols;
        if (null == curCol.parent) {
            cols = columns;
        } else {
            cols = curCol.parent.children;
        }
        int nextColIdx = curCol.idx + 1;
        if (nextColIdx < cols.size()) {
            return cols.get(nextColIdx);
        } else {
            if (null != curCol.parent) {
                curCol = curCol.parent;
                // recurse up the tree
                return nextColumn(curCol);
            } else {
                return null;
            }
        }
    }

    private void moveToNextColumn() throws SQLException {
        Column col = currentColumn();
        this.prevColumn = currentColumn;
        if (unionBegunInvariant()) {
            this.currentColumn = nextColumn(col.parent);
        } else {
            this.currentColumn = nextColumn(col);
        }
    }

    // checks

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException(createErrMsg("appender was closed"));
        }
    }

    private void checkCurrentColumnType(CAPIType ctype) throws SQLException {
        checkCurrentColumnType(ctype.typeArray);
    }

    private void checkCurrentColumnType(CAPIType[] ctypes) throws SQLException {
        Column col = currentColumn();
        checkColumnType(col, ctypes);
    }

    private void checkColumnType(Column col, CAPIType ctype) throws SQLException {
        checkColumnType(col, ctype.typeArray);
    }

    private void checkColumnType(Column col, CAPIType[] ctypes) throws SQLException {
        for (CAPIType ct : ctypes) {
            if (col.colType == ct) {
                return;
            }
        }
        throw new SQLException(createErrMsg("invalid column type, expected one of: '" + Arrays.toString(ctypes) +
                                            "', actual: '" + col.colType + "'"));
    }

    private void checkArrayLength(Column col, long length) throws SQLException {
        if (null == col.parent) {
            throw new SQLException(createErrMsg("invalid array/list column specified"));
        }
        switch (col.parent.colType) {
        case DUCKDB_TYPE_LIST:
            return;
        case DUCKDB_TYPE_ARRAY:
            break;
        default:
            throw new SQLException(createErrMsg("invalid array/list column type: " + col.colType));
        }
        if (col.arraySize != length) {
            throw new SQLException(
                createErrMsg("invalid array size, expected: " + col.arraySize + ", actual: " + length));
        }
    }

    private void checkDecimalType(Column col, CAPIType decimalInternalType) throws SQLException {
        if (col.decimalInternalType != decimalInternalType) {
            throw new SQLException(createErrMsg("invalid decimal internal type, expected: '" + col.decimalInternalType +
                                                "', actual: '" + decimalInternalType + "'"));
        }
    }

    private void checkDecimalPrecision(BigDecimal value, CAPIType decimalInternalType, int maxPrecision)
        throws SQLException {
        if (value.precision() > maxPrecision) {
            throw new SQLException(createErrMsg("invalid decimal precision, value: " + value.precision() +
                                                ", max value: " + maxPrecision +
                                                ", decimal internal type: " + decimalInternalType));
        }
    }

    // column helpers

    private Column currentColumn() throws SQLException {
        checkOpen();

        if (null == currentColumn) {
            throw new SQLException(createErrMsg("current column not found, columns count: " + columns.size()));
        }

        return currentColumn;
    }

    private Column currentColumn(CAPIType ctype) throws SQLException {
        return currentColumn(ctype.typeArray);
    }

    private Column currentColumn(CAPIType[] ctypes) throws SQLException {
        Column col = currentColumn();
        checkColumnType(col, ctypes);
        return col;
    }

    private Column arrayInnerColumn(Column arrayCol, CAPIType ctype) throws SQLException {
        return arrayInnerColumn(arrayCol, ctype.typeArray);
    }

    private Column arrayInnerColumn(Column arrayCol, CAPIType[] ctypes) throws SQLException {
        if (arrayCol.colType != DUCKDB_TYPE_ARRAY && arrayCol.colType != DUCKDB_TYPE_LIST) {
            throw new SQLException(createErrMsg("invalid array/list column type: '" + arrayCol.colType + "'"));
        }

        Column col = arrayCol.children.get(0);
        for (CAPIType ct : ctypes) {
            if (col.colType == ct) {
                return col;
            }
        }
        throw new SQLException(createErrMsg("invalid array/list inner column type, expected one of: '" +
                                            Arrays.toString(ctypes) + "', actual: '" + col.colType + "'"));
    }

    private Column currentTopLevelColumn() {
        if (null == currentColumn) {
            return null;
        }
        Column col = currentColumn;
        while (null != col.parent) {
            col = col.parent;
        }
        return col;
    }

    // null mask

    private void setNullMask(Column col, long vectorIdx, boolean[] nullMask, int elementsCount) throws SQLException {
        if (null == col.parent) {
            throw new SQLException(createErrMsg("invalid array/list column specified"));
        }
        switch (col.parent.colType) {
        case DUCKDB_TYPE_ARRAY:
            // While this should work for arrays nested inside lists, currently array null
            // masks are only used for top-level arrays
            setArrayNullMask(col, vectorIdx, nullMask, elementsCount, 0);
            return;
        case DUCKDB_TYPE_LIST:
            setListNullMask(col, nullMask, elementsCount);
            return;
        default:
            throw new SQLException(createErrMsg("invalid array/list column type: " + col.colType));
        }
    }

    private void setArrayNullMask(Column col, long vectorIdx, boolean[] nullMask, int elementsCount, int parentArrayIdx)
        throws SQLException {
        if (null == nullMask) {
            return;
        }
        if (nullMask.length != elementsCount) {
            throw new SQLException(
                createErrMsg("invalid null mask size, expected: " + elementsCount + ", actual: " + nullMask.length));
        }
        for (int i = 0; i < nullMask.length; i++) {
            if (nullMask[i]) {
                col.setNullOnArrayIdx(vectorIdx, (int) (i + col.arraySize * parentArrayIdx));
            }
        }
    }

    private void setListNullMask(Column col, boolean[] nullMask, int elementsCount) throws SQLException {
        if (null == nullMask) {
            return;
        }
        if (nullMask.length != elementsCount) {
            throw new SQLException(
                createErrMsg("invalid null mask size, expected: " + elementsCount + ", actual: " + nullMask.length));
        }
        if (col.listSize < elementsCount) {
            throw new SQLException(
                createErrMsg("invalid list state, list size: " + col.listSize + ", elements count: " + elementsCount));
        }
        for (int i = 0; i < nullMask.length; i++) {
            if (nullMask[i]) {
                long vectorIdx = col.listSize - elementsCount + i;
                col.setNull(vectorIdx);
            }
        }
    }

    // put implementation

    private void putByte(Column col, long vectorIdx, byte value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.put(value);
    }

    private void putShort(Column col, long vectorIdx, short value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putShort(value);
    }

    private void putInt(Column col, long vectorIdx, int value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putInt(value);
    }

    private void putLong(Column col, long vectorIdx, long value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(value);
    }

    private void putFloat(Column col, long vectorIdx, float value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putFloat(value);
    }

    private void putDouble(Column col, long vectorIdx, double value) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putDouble(value);
    }

    private void putHugeInt(Column col, long vectorIdx, long lower, long upper) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(lower);
        col.data.putLong(upper);
    }

    private void putBigInteger(Column col, long vectorIdx, BigInteger value) throws SQLException {
        if (value.compareTo(HUGE_INT_MIN) < 0 || value.compareTo(HUGE_INT_MAX) > 0) {
            throw new SQLException("Specified BigInteger value is out of range for HUGEINT field");
        }
        long lower = value.longValue();
        long upper = value.shiftRight(64).longValue();
        putHugeInt(col, vectorIdx, lower, upper);
    }

    private void putDecimal(Column col, long vectorIdx, short value) throws SQLException {
        int pos = (int) (vectorIdx * col.decimalInternalType.widthBytes);
        col.data.position(pos);
        col.data.putShort(value);
    }

    private void putDecimal(Column col, long vectorIdx, int value) throws SQLException {
        int pos = (int) (vectorIdx * col.decimalInternalType.widthBytes);
        col.data.position(pos);
        col.data.putInt(value);
    }

    private void putDecimal(Column col, long vectorIdx, long value) throws SQLException {
        int pos = (int) (vectorIdx * col.decimalInternalType.widthBytes);
        col.data.position(pos);
        col.data.putLong(value);
    }

    private void putDecimal(Column col, long vectorIdx, long lower, long upper) throws SQLException {
        int pos = (int) (vectorIdx * col.decimalInternalType.widthBytes);
        col.data.position(pos);
        col.data.putLong(lower);
        col.data.putLong(upper);
    }

    private void putDecimal(Column col, long vectorIdx, BigDecimal value) throws SQLException {
        if (value.precision() > col.decimalPrecision) {
            throw new SQLException(createErrMsg("invalid decimal precision, max expected: " + col.decimalPrecision +
                                                ", actual: " + value.precision()));
        }
        if (col.decimalScale != value.scale()) {
            throw new SQLException(
                createErrMsg("invalid decimal scale, expected: " + col.decimalScale + ", actual: " + value.scale()));
        }

        switch (col.decimalInternalType) {
        case DUCKDB_TYPE_SMALLINT: {
            checkDecimalPrecision(value, DUCKDB_TYPE_SMALLINT, 4);
            short shortValue = value.unscaledValue().shortValueExact();
            putDecimal(col, vectorIdx, shortValue);
            break;
        }
        case DUCKDB_TYPE_INTEGER: {
            checkDecimalPrecision(value, DUCKDB_TYPE_INTEGER, 9);
            int intValue = value.unscaledValue().intValueExact();
            putDecimal(col, vectorIdx, intValue);
            break;
        }
        case DUCKDB_TYPE_BIGINT: {
            checkDecimalPrecision(value, DUCKDB_TYPE_BIGINT, 18);
            long longValue = value.unscaledValue().longValueExact();
            putDecimal(col, vectorIdx, longValue);
            break;
        }
        case DUCKDB_TYPE_HUGEINT: {
            checkDecimalPrecision(value, DUCKDB_TYPE_HUGEINT, 38);
            BigInteger unscaledValue = value.unscaledValue();
            long lower = unscaledValue.longValue();
            long upper = unscaledValue.shiftRight(64).longValue();
            putDecimal(col, vectorIdx, lower, upper);
            break;
        }
        default:
            throw new SQLException(createErrMsg("invalid decimal internal type: '" + col.decimalInternalType + "'"));
        }
    }

    private void putStringOrBlob(Column col, long vectorIdx, byte[] bytes) throws SQLException {
        if (writeInlinedStrings && bytes.length < STRING_MAX_INLINE_BYTES) {
            int pos = (int) (vectorIdx * col.colType.widthBytes);
            col.data.position(pos);
            col.data.putInt(bytes.length);
            if (bytes.length > 0) {
                col.data.put(bytes);
            }
        } else {
            appenderRefLock.lock();
            try {
                checkOpen();
                duckdb_vector_assign_string_element_len(col.vectorRef, vectorIdx, bytes);
            } finally {
                appenderRefLock.unlock();
            }
        }
    }

    private void putUUID(Column col, long vectorIdx, long mostSigBits, long leastSigBits) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(leastSigBits);
        mostSigBits ^= Long.MIN_VALUE;
        col.data.putLong(mostSigBits);
    }

    private void putEpochDays(Column col, long vectorIdx, int days) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putInt(days);
    }

    private void putLocalDate(Column col, long vectorIdx, LocalDate date) throws SQLException {
        long days = date.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new SQLException(createErrMsg("unsupported number of days: " + days + ", must fit into 'int32_t'"));
        }
        putEpochDays(col, vectorIdx, (int) days);
    }

    public void putDayMicros(Column col, long vectorIdx, long micros) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(micros);
    }

    public void putLocalTime(Column col, long vectorIdx, LocalTime value) throws SQLException {
        long micros = value.toNanoOfDay() / 1000;
        putDayMicros(col, vectorIdx, micros);
    }

    public void putDayMicros(Column col, long vectorIdx, long micros, int offset) throws SQLException {
        int maxOffset = 16 * 60 * 60 - 1;
        long packed = ((micros & 0xFFFFFFFFFFL) << 24) | (long) ((maxOffset - offset) & 0xFFFFFF);
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(packed);
    }

    public void putOffsetTime(Column col, long vectorIdx, OffsetTime value) throws SQLException {
        int offset = value.getOffset().getTotalSeconds();
        long micros = value.toLocalTime().toNanoOfDay() / 1000;
        putDayMicros(col, vectorIdx, micros, offset);
    }

    private void putEpochMoment(Column col, long vectorIdx, long moment) throws SQLException {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(moment);
    }

    private void putLocalDateTime(Column col, long vectorIdx, LocalDateTime value) throws SQLException {
        final long moment;
        switch (col.colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            moment = EPOCH_DATE_TIME.until(value, SECONDS);
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            moment = EPOCH_DATE_TIME.until(value, MILLIS);
            break;
        case DUCKDB_TYPE_TIMESTAMP:
            moment = EPOCH_DATE_TIME.until(value, MICROS);
            break;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            moment = EPOCH_DATE_TIME.until(value, NANOS);
            break;
        default:
            throw new SQLException(createErrMsg("invalid column type: " + col.colType));
        }
        putEpochMoment(col, vectorIdx, moment);
    }

    private void putDate(Column col, long vectorIdx, java.util.Date value) throws SQLException {
        final long moment;
        switch (col.colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            moment = value.getTime() / 1000;
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS: {
            moment = value.getTime();
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            moment = Math.multiplyExact(value.getTime(), 1000L);
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP_NS: {
            moment = Math.multiplyExact(value.getTime(), 1000000L);
            break;
        }
        default:
            throw new SQLException(createErrMsg("invalid column type: " + col.colType));
        }
        putEpochMoment(col, vectorIdx, moment);
    }

    private void putOffsetDateTime(Column col, long vectorIdx, OffsetDateTime value) throws SQLException {
        ZonedDateTime zdt = value.atZoneSameInstant(ZoneOffset.UTC);
        LocalDateTime ldt = zdt.toLocalDateTime();
        long micros = EPOCH_DATE_TIME.until(ldt, MICROS);
        putEpochMoment(col, vectorIdx, micros);
    }

    private void putBoolArray(Column arrayCol, long vectorIdx, boolean[] values) throws SQLException {
        putBoolArray(arrayCol, vectorIdx, values, null);
    }

    private void putBoolArray(Column arrayCol, long vectorIdx, boolean[] values, boolean[] nullMask)
        throws SQLException {
        Column col = arrayInnerColumn(arrayCol, DUCKDB_TYPE_BOOLEAN);

        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) (values[i] ? 1 : 0);
        }

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        col.data.position(pos);
        col.data.put(bytes);
    }

    private void putByteArray(Column arrayCol, long vectorIdx, byte[] values) throws SQLException {
        putByteArray(arrayCol, vectorIdx, values, null);
    }

    private void putByteArray(Column arrayCol, long vectorIdx, byte[] values, boolean[] nullMask) throws SQLException {
        Column col = arrayInnerColumn(arrayCol, int8Types);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        col.data.position(pos);
        col.data.put(values);
    }

    private void putShortArray(Column arrayCol, long vectorIdx, short[] values) throws SQLException {
        putShortArray(arrayCol, vectorIdx, values, null);
    }

    private void putShortArray(Column arrayCol, long vectorIdx, short[] values, boolean[] nullMask)
        throws SQLException {
        Column col = arrayInnerColumn(arrayCol, int16Types);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        ShortBuffer shortData = col.data.asShortBuffer();
        shortData.position(pos);
        shortData.put(values);
    }

    private void putIntArray(Column arrayCol, long vectorIdx, int[] values) throws SQLException {
        putIntArray(arrayCol, vectorIdx, values, null);
    }

    private void putIntArray(Column arrayCol, long vectorIdx, int[] values, boolean[] nullMask) throws SQLException {
        Column col = arrayInnerColumn(arrayCol, int32Types);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        IntBuffer intData = col.data.asIntBuffer();
        intData.position(pos);
        intData.put(values);
    }

    private void putLongArray(Column arrayCol, long vectorIdx, long[] values) throws SQLException {
        putLongArray(arrayCol, vectorIdx, values, null);
    }

    private void putLongArray(Column arrayCol, long vectorIdx, long[] values, boolean[] nullMask) throws SQLException {
        Column col = arrayInnerColumn(arrayCol, int64Types);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        LongBuffer longData = col.data.asLongBuffer();
        longData.position(pos);
        longData.put(values);
    }

    private void putFloatArray(Column arrayCol, long vectorIdx, float[] values) throws SQLException {
        putFloatArray(arrayCol, vectorIdx, values, null);
    }

    private void putFloatArray(Column arrayCol, long vectorIdx, float[] values, boolean[] nullMask)
        throws SQLException {
        Column col = arrayInnerColumn(arrayCol, DUCKDB_TYPE_FLOAT);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        FloatBuffer floatData = col.data.asFloatBuffer();
        floatData.position(pos);
        floatData.put(values);
    }

    private void putDoubleArray(Column arrayCol, long vectorIdx, double[] values) throws SQLException {
        putDoubleArray(arrayCol, vectorIdx, values, null);
    }

    private void putDoubleArray(Column arrayCol, long vectorIdx, double[] values, boolean[] nullMask)
        throws SQLException {
        Column col = arrayInnerColumn(arrayCol, DUCKDB_TYPE_DOUBLE);

        checkArrayLength(col, values.length);
        int pos = prepareListColumn(col, vectorIdx, values.length);
        setNullMask(col, vectorIdx, nullMask, values.length);

        DoubleBuffer doubleData = col.data.asDoubleBuffer();
        doubleData.position(pos);
        doubleData.put(values);
    }

    private void putBoolArray2D(Column col, long vectorIdx, boolean[][] values) throws SQLException {
        putBoolArray2D(col, vectorIdx, values, null);
    }

    private void putBoolArray2D(Column outerCol, long vectorIdx, boolean[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, DUCKDB_TYPE_BOOLEAN);

        byte[] buf = new byte[(int) col.arraySize];

        for (int i = 0; i < values.length; i++) {
            boolean[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            for (int j = 0; j < childValues.length; j++) {
                buf[j] = (byte) (childValues[j] ? 1 : 0);
            }

            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            col.data.position(pos);
            col.data.put(buf);
        }
    }

    private void putByteArray2D(Column outerCol, long vectorIdx, byte[][] values) throws SQLException {
        putByteArray2D(outerCol, vectorIdx, values, null);
    }

    private void putByteArray2D(Column outerCol, long vectorIdx, byte[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, int8Types);

        for (int i = 0; i < values.length; i++) {
            byte[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            col.data.position(pos);
            col.data.put(childValues);
        }
    }

    private void putShortArray2D(Column outerCol, long vectorIdx, short[][] values) throws SQLException {
        putShortArray2D(outerCol, vectorIdx, values, null);
    }

    private void putShortArray2D(Column outerCol, long vectorIdx, short[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, int16Types);

        for (int i = 0; i < values.length; i++) {
            short[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            ShortBuffer shortBuffer = col.data.asShortBuffer();
            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            shortBuffer.position(pos);
            shortBuffer.put(childValues);
        }
    }

    private void putIntArray2D(Column col, long vectorIdx, int[][] values) throws SQLException {
        putIntArray2D(col, vectorIdx, values, null);
    }

    private void putIntArray2D(Column outerCol, long vectorIdx, int[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, int32Types);

        for (int i = 0; i < values.length; i++) {
            int[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            IntBuffer intData = col.data.asIntBuffer();
            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            intData.position(pos);
            intData.put(childValues);
        }
    }

    private void putLongArray2D(Column outerCol, long vectorIdx, long[][] values) throws SQLException {
        putLongArray2D(outerCol, vectorIdx, values, null);
    }

    private void putLongArray2D(Column outerCol, long vectorIdx, long[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, int64Types);

        for (int i = 0; i < values.length; i++) {
            long[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            LongBuffer longData = col.data.asLongBuffer();
            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            longData.position(pos);
            longData.put(childValues);
        }
    }

    private void putFloatArray2D(Column outerCol, long vectorIdx, float[][] values) throws SQLException {
        putFloatArray2D(outerCol, vectorIdx, values, null);
    }

    private void putFloatArray2D(Column outerCol, long vectorIdx, float[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, DUCKDB_TYPE_FLOAT);

        for (int i = 0; i < values.length; i++) {
            float[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            FloatBuffer floatData = col.data.asFloatBuffer();
            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            floatData.position(pos);
            floatData.put(childValues);
        }
    }

    private void putDoubleArray2D(Column outerCol, long vectorIdx, double[][] values) throws SQLException {
        putDoubleArray2D(outerCol, vectorIdx, values, null);
    }

    private void putDoubleArray2D(Column outerCol, long vectorIdx, double[][] values, boolean[][] nullMask)
        throws SQLException {
        Column innerCol = arrayInnerColumn(outerCol, DUCKDB_TYPE_ARRAY);
        checkArrayLength(innerCol, values.length);
        Column col = arrayInnerColumn(innerCol, DUCKDB_TYPE_DOUBLE);

        for (int i = 0; i < values.length; i++) {
            double[] childValues = values[i];

            if (childValues == null) {
                innerCol.setNullOnArrayIdx(vectorIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, vectorIdx, nullMask[i], childValues.length, i);
            }

            DoubleBuffer doubleBuffer = col.data.asDoubleBuffer();
            int pos = (int) ((vectorIdx * innerCol.arraySize + i) * col.arraySize);
            doubleBuffer.position(pos);
            doubleBuffer.put(childValues);
        }
    }

    private void putObjectArrayOrList(Column col, long vectorIdx, Iterator<?> iter, int count) throws SQLException {
        checkArrayLength(col, count);
        prepareListColumn(col, vectorIdx, count);

        final long offset;
        if (col.parent.colType == DUCKDB_TYPE_LIST) {
            offset = col.listSize - count;
        } else {
            offset = vectorIdx * col.arraySize;
        }

        for (long i = 0; i < count; i++) {
            if (!iter.hasNext()) {
                throw new SQLException(
                    createErrMsg("invalid iterator elements count, expected: " + count + ", actual" + i));
            }

            Object value = iter.next();
            long innerVectorIdx = offset + i;

            putCompositeElement(col, innerVectorIdx, value);
        }
    }

    private void putMap(Column col, long vectorIdx, Map<?, ?> map) throws SQLException {
        prepareListColumn(col, vectorIdx, map.size());
        long offset = col.listSize - map.size();

        long i = 0;
        List<Object> values = new ArrayList<>(2);
        values.add(null);
        values.add(null);
        for (Map.Entry<?, ?> en : map.entrySet()) {
            values.set(0, en.getKey());
            values.set(1, en.getValue());
            long innerVectorIdx = offset + i;
            putCompositeElementStruct(col, innerVectorIdx, values);
            i += 1;
        }
    }

    private void putCompositeElement(Column col, long vectorIdx, Object value) throws SQLException {
        if (null == value) {
            col.setNull(vectorIdx);
            return;
        }
        switch (col.colType) {
        // primitive wrappers are intended to be used only
        // for struct/union fields
        case DUCKDB_TYPE_BOOLEAN: {
            Boolean bool = (Boolean) value;
            byte num = (byte) (bool ? 1 : 0);
            putByte(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_TINYINT: {
            Byte num = (Byte) value;
            putByte(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_SMALLINT: {
            Short num = (Short) value;
            putShort(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_INTEGER: {
            Integer num = (Integer) value;
            putInt(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_BIGINT: {
            Long num = (Long) value;
            putLong(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_HUGEINT: {
            BigInteger num = (BigInteger) value;
            putBigInteger(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_FLOAT: {
            Float num = (Float) value;
            putFloat(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_DOUBLE: {
            Double num = (Double) value;
            putDouble(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_DECIMAL: {
            BigDecimal num = (BigDecimal) value;
            putDecimal(col, vectorIdx, num);
            break;
        }
        case DUCKDB_TYPE_VARCHAR: {
            String st = (String) value;
            byte[] bytes = utf8(st);
            putStringOrBlob(col, vectorIdx, bytes);
            break;
        }
        case DUCKDB_TYPE_UUID: {
            UUID uid = (UUID) value;
            long mostSigBits = uid.getMostSignificantBits();
            long leastSigBits = uid.getLeastSignificantBits();
            putUUID(col, vectorIdx, mostSigBits, leastSigBits);
            break;
        }
        case DUCKDB_TYPE_DATE: {
            LocalDate ld = (LocalDate) value;
            putLocalDate(col, vectorIdx, ld);
            break;
        }
        case DUCKDB_TYPE_TIME: {
            LocalTime lt = (LocalTime) value;
            putLocalTime(col, vectorIdx, lt);
            break;
        }
        case DUCKDB_TYPE_TIME_TZ: {
            OffsetTime ot = (OffsetTime) value;
            putOffsetTime(col, vectorIdx, ot);
            break;
        }
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP_NS:
        case DUCKDB_TYPE_TIMESTAMP_S:
            if (value instanceof LocalDateTime) {
                LocalDateTime ldt = (LocalDateTime) value;
                putLocalDateTime(col, vectorIdx, ldt);
            } else if (value instanceof java.util.Date) {
                Date dt = (Date) value;
                putDate(col, vectorIdx, dt);
            } else {
                throw new SQLException(createErrMsg("invalid object type for timestamp column, expected one of: [" +
                                                    LocalDateTime.class.getName() + ", " + Date.class.getName() +
                                                    "], actual: [" + value.getClass().getName() + "]"));
            }
            break;
        case DUCKDB_TYPE_TIMESTAMP_TZ: {
            OffsetDateTime odt = (OffsetDateTime) value;
            putOffsetDateTime(col, vectorIdx, odt);
            break;
        }
        case DUCKDB_TYPE_ARRAY:
            putCompositeElementArray(col, vectorIdx, value);
            break;
        case DUCKDB_TYPE_LIST: {
            Collection<?> collection = (Collection<?>) value;
            if (col.children.size() != 1) {
                throw new SQLException(createErrMsg("invalid list column"));
            }
            Column innerCol = col.children.get(0);
            putObjectArrayOrList(innerCol, vectorIdx, collection.iterator(), collection.size());
            break;
        }
        case DUCKDB_TYPE_MAP: {
            Map<?, ?> map = (Map<?, ?>) value;
            if (col.children.size() != 1) {
                throw new SQLException(createErrMsg("invalid map column"));
            }
            Column innerCol = col.children.get(0);
            putMap(innerCol, vectorIdx, map);
            break;
        }
        case DUCKDB_TYPE_STRUCT: {
            putCompositeElementStruct(col, vectorIdx, value);
            break;
        }
        case DUCKDB_TYPE_UNION: {
            putCompositeElementUnion(col, vectorIdx, value);
            break;
        }
        default:
            throw new SQLException(createErrMsg("unsupported composite column, inner type: " + col.colType));
        }
    }

    private void putCompositeElementArray(Column col, long vectorIdx, Object value) throws SQLException {
        if (col.children.size() != 1) {
            throw new SQLException(createErrMsg("invalid array column"));
        }
        Column innerCol = col.children.get(0);
        switch (innerCol.colType) {
        case DUCKDB_TYPE_BOOLEAN: {
            boolean[] arr = (boolean[]) value;
            putBoolArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_TINYINT: {
            byte[] arr = (byte[]) value;
            putByteArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_SMALLINT: {
            short[] arr = (short[]) value;
            putShortArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_INTEGER: {
            int[] arr = (int[]) value;
            putIntArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_BIGINT: {
            long[] arr = (long[]) value;
            putLongArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_FLOAT: {
            float[] arr = (float[]) value;
            putFloatArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_DOUBLE: {
            double[] arr = (double[]) value;
            putDoubleArray(col, vectorIdx, arr);
            break;
        }
        case DUCKDB_TYPE_ARRAY: {
            if (value instanceof boolean[][]) {
                boolean[][] arr = (boolean[][]) value;
                putBoolArray2D(col, vectorIdx, arr);
            } else if (value instanceof byte[][]) {
                byte[][] arr = (byte[][]) value;
                putByteArray2D(col, vectorIdx, arr);
            } else if (value instanceof short[][]) {
                short[][] arr = (short[][]) value;
                putShortArray2D(col, vectorIdx, arr);
            } else if (value instanceof int[][]) {
                int[][] arr = (int[][]) value;
                putIntArray2D(col, vectorIdx, arr);
            } else if (value instanceof long[][]) {
                long[][] arr = (long[][]) value;
                putLongArray2D(col, vectorIdx, arr);
            } else if (value instanceof float[][]) {
                float[][] arr = (float[][]) value;
                putFloatArray2D(col, vectorIdx, arr);
            } else if (value instanceof double[][]) {
                double[][] arr = (double[][]) value;
                putDoubleArray2D(col, vectorIdx, arr);
            } else {
                throw new SQLException(createErrMsg("unsupported 2D array type: " + value.getClass().getName()));
            }
            break;
        }
        default:
            throw new SQLException(createErrMsg("unsupported array type: " + innerCol.colType));
        }
    }

    private void putCompositeElementStruct(Column structCol, long vectorIdx, Object structValue) throws SQLException {
        final Collection<?> collection;
        if (structValue instanceof Map) {
            if (structValue instanceof LinkedHashMap) {
                LinkedHashMap<?, ?> map = (LinkedHashMap<?, ?>) structValue;
                collection = map.values();
            } else {
                throw new SQLException(createErrMsg(
                    "struct values must be specified as an instance of a 'java.util.LinkedHashMap' or as a collection of objects, actual class: " +
                    structValue.getClass().getName()));
            }
        } else {
            collection = (Collection<?>) structValue;
        }

        if (structCol.children.size() != collection.size()) {
            throw new SQLException(createErrMsg("invalid struct object specified, expected fields count: " +
                                                structCol.children.size() + ", actual: " + collection.size()));
        }

        int i = 0;
        for (Object value : collection) {
            Column col = structCol.children.get(i);
            putCompositeElement(col, vectorIdx, value);
            i += 1;
        }
    }

    private void putCompositeElementUnion(Column unionCol, long vectorIdx, Object unionValue) throws SQLException {
        if (!(unionValue instanceof AbstractMap.SimpleEntry)) {
            throw new SQLException(createErrMsg(
                "union values must be specified as an instance of 'java.util.AbstractMap.SimpleEntry<String, Object>', actual type: " +
                unionValue.getClass().getName()));
        }
        AbstractMap.SimpleEntry<?, ?> entry = (AbstractMap.SimpleEntry<?, ?>) unionValue;
        String tag = String.valueOf(entry.getKey());
        Column col = putUnionTag(unionCol, vectorIdx, tag);
        putCompositeElement(col, vectorIdx, entry.getValue());
    }

    private Column putUnionTag(Column col, long vectorIdx, String tag) throws SQLException {
        int fieldWithTag = 0;
        for (int i = 1; i < col.children.size(); i++) {
            Column childCol = col.children.get(i);
            if (childCol.structFieldName.equals(tag)) {
                fieldWithTag = i;
            }
        }
        if (0 == fieldWithTag) {
            throw new SQLException(createErrMsg("specified union field not found, value: '" + tag + "'"));
        }

        // set tag
        Column tagCol = col.children.get(0);
        putByte(tagCol, vectorIdx, (byte) (fieldWithTag - 1));
        // set other fields to NULL
        for (int i = 1; i < col.children.size(); i++) {
            if (i == fieldWithTag) {
                continue;
            }
            Column childCol = col.children.get(i);
            childCol.setNull(vectorIdx);
        }
        return col.children.get(fieldWithTag);
    }

    // state invariants

    private boolean rowBegunInvariant() {
        return null != currentColumn;
    }

    private boolean rowCompletedInvariant() {
        return null == currentColumn && null != prevColumn && prevColumn.idx == columns.size() - 1;
    }

    private boolean structBegunInvariant() {
        return null != currentColumn && null != currentColumn.parent &&
            currentColumn.parent.colType == DUCKDB_TYPE_STRUCT;
    }

    private boolean structCompletedInvariant() {
        return null != prevColumn && null != prevColumn.parent && prevColumn.parent.colType == DUCKDB_TYPE_STRUCT &&
            prevColumn.idx == prevColumn.parent.children.size() - 1;
    }

    private boolean unionBegunInvariant() {
        return null != currentColumn && null != currentColumn.parent &&
            currentColumn.parent.colType == DUCKDB_TYPE_UNION;
    }

    private boolean unionCompletedInvariant() {
        return null != prevColumn && null != prevColumn.parent && prevColumn.parent.colType == DUCKDB_TYPE_UNION;
    }

    private boolean readyForANewRowInvariant() {
        return null == currentColumn && null == prevColumn;
    }

    // list helpers

    private int prepareListColumn(Column innerCol, long vectorIdx, long listElementsCount) throws SQLException {
        if (null == innerCol.parent) {
            throw new SQLException(createErrMsg("invalid array/list column specified"));
        }
        Column col = innerCol.parent;
        switch (col.colType) {
        case DUCKDB_TYPE_ARRAY:
            return (int) (vectorIdx * innerCol.arraySize);
        case DUCKDB_TYPE_LIST:
        case DUCKDB_TYPE_MAP:
            break;
        default:
            throw new SQLException(createErrMsg("invalid array/list column type: " + col.colType));
        }
        appenderRefLock.lock();
        try {
            checkOpen();
            long offset = duckdb_list_vector_get_size(col.vectorRef);
            LongBuffer longBuffer = col.data.asLongBuffer();
            int pos = (int) (vectorIdx * DUCKDB_TYPE_LIST.widthBytes / Long.BYTES);
            longBuffer.position(pos);
            longBuffer.put(offset);
            longBuffer.put(listElementsCount);
            long listSize = offset + listElementsCount;
            int reserveStatus = duckdb_list_vector_reserve(col.vectorRef, listSize);
            if (0 != reserveStatus) {
                throw new SQLException(
                    createErrMsg("'duckdb_list_vector_reserve' call failed, list size: " + listSize));
            }
            innerCol.reset(listSize);
            int setStatus = duckdb_list_vector_set_size(col.vectorRef, listSize);
            if (0 != setStatus) {
                throw new SQLException(
                    createErrMsg("'duckdb_list_vector_set_size' call failed, list size: " + listSize));
            }
            return (int) offset;
        } finally {
            appenderRefLock.unlock();
        }
    }

    // string helpers

    private static byte[] utf8(String str) {
        if (null == str) {
            return null;
        }
        return str.getBytes(UTF_8);
    }

    private static String strFromUTF8(byte[] utf8) {
        if (null == utf8) {
            return "";
        }
        return new String(utf8, UTF_8);
    }

    // static methods

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
                    if (null != lt) {
                        duckdb_destroy_logical_type(lt);
                    }
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

    private static void initVecChildren(Column parent) throws SQLException {
        switch (parent.colType) {
        case DUCKDB_TYPE_LIST:
        case DUCKDB_TYPE_MAP: {
            ByteBuffer vec = duckdb_list_vector_get_child(parent.vectorRef);
            Column col = new Column(parent, 0, null, vec);
            parent.children.add(col);
            break;
        }
        case DUCKDB_TYPE_STRUCT:
        case DUCKDB_TYPE_UNION: {
            long count = duckdb_struct_type_child_count(parent.colTypeRef);
            for (int i = 0; i < count; i++) {
                ByteBuffer vec = duckdb_struct_vector_get_child(parent.vectorRef, i);
                Column col = new Column(parent, i, null, vec, i);
                parent.children.add(col);
            }
            break;
        }
        case DUCKDB_TYPE_ARRAY: {
            ByteBuffer vec = duckdb_array_vector_get_child(parent.vectorRef);
            Column col = new Column(parent, 0, null, vec);
            parent.children.add(col);
            break;
        }
        }
    }

    private static List<Column> createTopLevelColumns(ByteBuffer chunkRef, ByteBuffer[] colTypes) throws SQLException {
        List<Column> columns = new ArrayList<>(colTypes.length);
        try {
            for (int i = 0; i < colTypes.length; i++) {
                ByteBuffer vector = duckdb_data_chunk_get_vector(chunkRef, i);
                Column col = new Column(null, i, colTypes[i], vector);
                columns.add(col);
                colTypes[i] = null;
            }
        } catch (Exception e) {
            for (Column col : columns) {
                if (null != col) {
                    col.destroy();
                }
            }
            throw e;
        }
        return columns;
    }

    private static class Column {
        private final Column parent;
        private final int idx;
        private /* final */ ByteBuffer colTypeRef;
        private final CAPIType colType;
        private final CAPIType decimalInternalType;
        private final int decimalPrecision;
        private final int decimalScale;
        private final long arraySize;
        private final String structFieldName;

        private final ByteBuffer vectorRef;
        private final List<Column> children = new ArrayList<>();

        private long listSize = 0;
        private ByteBuffer data = null;
        private ByteBuffer validity = null;

        private Column(Column parent, int idx, ByteBuffer colTypeRef, ByteBuffer vector) throws SQLException {
            this(parent, idx, colTypeRef, vector, -1);
        }

        private Column(Column parent, int idx, ByteBuffer colTypeRef, ByteBuffer vector, int structFieldIdx)
            throws SQLException {
            this.parent = parent;
            this.idx = idx;

            if (null == vector) {
                throw new SQLException("cannot initialize data chunk vector");
            }

            if (null == colTypeRef) {
                this.colTypeRef = duckdb_vector_get_column_type(vector);
                if (null == this.colTypeRef) {
                    throw new SQLException("cannot initialize data chunk vector type");
                }
            } else {
                this.colTypeRef = colTypeRef;
            }

            int colTypeId = duckdb_get_type_id(this.colTypeRef);
            this.colType = capiTypeFromTypeId(colTypeId);

            if (colType == DUCKDB_TYPE_DECIMAL) {
                int decimalInternalTypeId = duckdb_decimal_internal_type(this.colTypeRef);
                this.decimalInternalType = capiTypeFromTypeId(decimalInternalTypeId);
                this.decimalPrecision = duckdb_decimal_width(this.colTypeRef);
                this.decimalScale = duckdb_decimal_scale(this.colTypeRef);
            } else {
                this.decimalInternalType = DUCKDB_TYPE_INVALID;
                this.decimalPrecision = -1;
                this.decimalScale = -1;
            }

            if (structFieldIdx >= 0) {
                byte[] nameUTF8 = duckdb_struct_type_child_name(parent.colTypeRef, structFieldIdx);
                this.structFieldName = strFromUTF8(nameUTF8);
            } else {
                this.structFieldName = null;
            }

            this.vectorRef = vector;

            if (null == parent || parent.colType != DUCKDB_TYPE_ARRAY) {
                this.arraySize = 1;
            } else {
                this.arraySize = duckdb_array_type_array_size(parent.colTypeRef);
            }

            long maxElems = maxElementsCount();
            if (colType.widthBytes > 0 || colType == DUCKDB_TYPE_DECIMAL) {
                long vectorSizeBytes = maxElems * widthBytes();
                this.data = duckdb_vector_get_data(vectorRef, vectorSizeBytes);
                if (null == this.data) {
                    throw new SQLException("cannot initialize data chunk vector data");
                }
            } else {
                this.data = null;
            }

            duckdb_vector_ensure_validity_writable(vectorRef);
            this.validity = duckdb_vector_get_validity(vectorRef, maxElems);
            if (null == this.validity) {
                throw new SQLException("cannot initialize data chunk vector validity");
            }

            // last call in constructor
            initVecChildren(this);
        }

        void reset(long listSize) throws SQLException {
            if (null == parent || !(parent.colType == DUCKDB_TYPE_LIST || parent.colType == DUCKDB_TYPE_MAP)) {
                throw new SQLException("invalid list column");
            }
            this.listSize = listSize;
            reset();
        }

        void reset() throws SQLException {
            long maxElems = maxElementsCount();

            if (null != this.data) {
                long vectorSizeBytes = maxElems * widthBytes();
                this.data = duckdb_vector_get_data(vectorRef, vectorSizeBytes);
                if (null == this.data) {
                    throw new SQLException("cannot reset data chunk vector data");
                }
            }

            duckdb_vector_ensure_validity_writable(vectorRef);
            this.validity = duckdb_vector_get_validity(vectorRef, maxElems);
            if (null == this.validity) {
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

        void setNull(long vectorIdx) throws SQLException {
            if (colType == DUCKDB_TYPE_ARRAY) {
                setNullOnArrayIdx(vectorIdx, 0);
                for (Column col : children) {
                    for (int i = 0; i < col.arraySize; i++) {
                        col.setNullOnArrayIdx(vectorIdx, i);
                    }
                }
            } else {
                setNullOnVectorIdx(vectorIdx);
                if (colType == DUCKDB_TYPE_LIST || colType == DUCKDB_TYPE_MAP) {
                    return;
                }
                for (Column col : children) {
                    col.setNull(vectorIdx);
                }
            }
        }

        void setNullOnArrayIdx(long rowIdx, int arrayIdx) {
            long vectorIdx = rowIdx * arraySize * parentArraySize() + arrayIdx;
            setNullOnVectorIdx(vectorIdx);
        }

        void setNullOnVectorIdx(long vectorIdx) {
            long validityPos = vectorIdx / 64;
            LongBuffer entries = this.validity.asLongBuffer();
            entries.position((int) validityPos);
            long mask = entries.get();
            long idxInEntry = vectorIdx % 64;
            mask &= ~(1L << idxInEntry);
            entries.position((int) validityPos);
            entries.put(mask);
        }

        long widthBytes() {
            if (colType == DUCKDB_TYPE_DECIMAL) {
                return decimalInternalType.widthBytes;
            } else {
                return colType.widthBytes;
            }
        }

        long parentArraySize() {
            if (null == parent) {
                return 1;
            }
            return parent.arraySize;
        }

        long maxElementsCount() {
            Column ancestor = this;
            while (null != ancestor) {
                if (null != ancestor.parent &&
                    (ancestor.parent.colType == DUCKDB_TYPE_LIST || ancestor.parent.colType == DUCKDB_TYPE_MAP)) {
                    break;
                }
                ancestor = ancestor.parent;
            }
            long maxEntries = null != ancestor ? ancestor.listSize : DuckDBAppender.MAX_TOP_LEVEL_ROWS;
            return maxEntries * arraySize * parentArraySize();
        }
    }
}

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

    private static final int STRING_MAX_INLINE_BYTES = 12;

    private static final LocalDateTime EPOCH_DATE_TIME = LocalDateTime.ofEpochSecond(0, 0, UTC);

    private final DuckDBConnection conn;

    private final String catalog;
    private final String schema;
    private final String table;

    private final long maxRows;

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

        this.maxRows = duckdb_vector_size();

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
        if (rowIdx >= maxRows) {
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
        checkCurrentColumnType(DUCKDB_TYPE_UNION);

        int fieldWithTag = 0;
        for (int i = 1; i < currentColumn.children.size(); i++) {
            Column childCol = currentColumn.children.get(i);
            if (childCol.structFieldName.equals(tag)) {
                fieldWithTag = i;
            }
        }
        if (0 == fieldWithTag) {
            throw new SQLException(createErrMsg("specified union field not found, value: '" + tag + "'"));
        }

        // set tag
        Column structCol = currentColumn;
        this.currentColumn = currentColumn.children.get(0);
        append((byte) (fieldWithTag - 1));
        // set other fields to NULL
        for (int i = 1; i < structCol.children.size(); i++) {
            if (i == fieldWithTag) {
                continue;
            }
            Column childCol = structCol.children.get(i);
            childCol.setNull(rowIdx);
        }
        this.currentColumn = structCol.children.get(fieldWithTag);
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
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_BOOLEAN);
        byte val = (byte) (value ? 1 : 0);
        col.data.put(val);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(char value) throws SQLException {
        String str = String.valueOf(value);
        return append(str);
    }

    public DuckDBAppender append(byte value) throws SQLException {
        Column col = currentColumnWithRowPos(int8Types);
        col.data.put(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(short value) throws SQLException {
        Column col = currentColumnWithRowPos(int16Types);
        col.data.putShort(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(int value) throws SQLException {
        Column col = currentColumnWithRowPos(int32Types);
        col.data.putInt(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(long value) throws SQLException {
        Column col = currentColumnWithRowPos(int64Types);
        col.data.putLong(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(float value) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_FLOAT);
        col.data.putFloat(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(double value) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_DOUBLE);
        col.data.putDouble(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    // append primitive wrappers, int128 and decimal

    public DuckDBAppender append(Boolean value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_BOOLEAN);
        if (value == null) {
            return appendNull();
        }
        return append(value.booleanValue());
    }

    public DuckDBAppender append(Character value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_VARCHAR);
        if (value == null) {
            return appendNull();
        }
        return append(value.charValue());
    }

    public DuckDBAppender append(Byte value) throws SQLException {
        checkCurrentColumnType(int8Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.byteValue());
    }

    public DuckDBAppender append(Short value) throws SQLException {
        checkCurrentColumnType(int16Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.shortValue());
    }

    public DuckDBAppender append(Integer value) throws SQLException {
        checkCurrentColumnType(int32Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.intValue());
    }

    public DuckDBAppender append(Long value) throws SQLException {
        checkCurrentColumnType(int64Types);
        if (value == null) {
            return appendNull();
        }
        return append(value.longValue());
    }

    public DuckDBAppender append(Float value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_FLOAT);
        if (value == null) {
            return appendNull();
        }
        return append(value.floatValue());
    }

    public DuckDBAppender append(Double value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_DOUBLE);
        if (value == null) {
            return appendNull();
        }
        return append(value.doubleValue());
    }

    public DuckDBAppender appendHugeInt(long lower, long upper) throws SQLException {
        Column col = currentColumnWithRowPos(int128Types);
        col.data.putLong(lower);
        col.data.putLong(upper);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(BigInteger value) throws SQLException {
        checkCurrentColumnType(int128Types);
        if (value == null) {
            return appendNull();
        }
        if (value.compareTo(HUGE_INT_MIN) < 0 || value.compareTo(HUGE_INT_MAX) > 0) {
            throw new SQLException("Specified BigInteger value is out of range for HUGEINT field");
        }
        long lower = value.longValue();
        long upper = value.shiftRight(64).longValue();
        return appendHugeInt(lower, upper);
    }

    public DuckDBAppender appendDecimal(short value) throws SQLException {
        Column col = currentDecimalColumnWithRowPos(DUCKDB_TYPE_SMALLINT);
        col.data.putShort(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendDecimal(int value) throws SQLException {
        Column col = currentDecimalColumnWithRowPos(DUCKDB_TYPE_INTEGER);
        col.data.putInt(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendDecimal(long value) throws SQLException {
        Column col = currentDecimalColumnWithRowPos(DUCKDB_TYPE_BIGINT);
        col.data.putLong(value);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendDecimal(long lower, long upper) throws SQLException {
        Column col = currentDecimalColumnWithRowPos(DUCKDB_TYPE_HUGEINT);
        col.data.putLong(lower);
        col.data.putLong(upper);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(BigDecimal value) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_DECIMAL);
        if (value == null) {
            return appendNull();
        }
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
            return appendDecimal(shortValue);
        }
        case DUCKDB_TYPE_INTEGER: {
            checkDecimalPrecision(value, DUCKDB_TYPE_INTEGER, 9);
            int intValue = value.unscaledValue().intValueExact();
            return appendDecimal(intValue);
        }
        case DUCKDB_TYPE_BIGINT: {
            checkDecimalPrecision(value, DUCKDB_TYPE_BIGINT, 18);
            long longValue = value.unscaledValue().longValueExact();
            return appendDecimal(longValue);
        }
        case DUCKDB_TYPE_HUGEINT: {
            checkDecimalPrecision(value, DUCKDB_TYPE_HUGEINT, 38);
            BigInteger unscaledValue = value.unscaledValue();
            long lower = unscaledValue.longValue();
            long upper = unscaledValue.shiftRight(64).longValue();
            return appendDecimal(lower, upper);
        }
        default:
            throw new SQLException(createErrMsg("invalid decimal internal type: '" + col.decimalInternalType + "'"));
        }
    }

    // append arrays

    public DuckDBAppender append(boolean[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(boolean[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(DUCKDB_TYPE_BOOLEAN);
        if (values == null) {
            return appendNull();
        }

        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) (values[i] ? 1 : 0);
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        int pos = (int) (rowIdx * col.arraySize);
        col.data.position(pos);
        col.data.put(bytes);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(boolean[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(boolean[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(DUCKDB_TYPE_BOOLEAN);
        byte[] buf = new byte[(int) col.arraySize];

        for (int i = 0; i < values.length; i++) {
            boolean[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            for (int j = 0; j < childValues.length; j++) {
                buf[j] = (byte) (childValues[j] ? 1 : 0);
            }

            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            col.data.position(pos);
            col.data.put(buf);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendByteArray(byte[] values) throws SQLException {
        return appendByteArray(values, null);
    }

    public DuckDBAppender appendByteArray(byte[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(int8Types);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        int pos = (int) (rowIdx * col.arraySize);
        col.data.position(pos);
        col.data.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendByteArray(byte[][] values) throws SQLException {
        return appendByteArray(values, null);
    }

    public DuckDBAppender appendByteArray(byte[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(int8Types);

        for (int i = 0; i < values.length; i++) {
            byte[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            col.data.position(pos);
            col.data.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(byte[] values) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_BLOB);
        if (values == null) {
            return appendNull();
        }
        return appendStringOrBlobInternal(DUCKDB_TYPE_BLOB, values);
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
        Column col = currentArrayInnerColumn(int16Types);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        ShortBuffer shortData = col.data.asShortBuffer();
        int pos = (int) (rowIdx * col.arraySize);
        shortData.position(pos);
        shortData.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(short[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(short[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(int16Types);

        for (int i = 0; i < values.length; i++) {
            short[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            ShortBuffer shortBuffer = col.data.asShortBuffer();
            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            shortBuffer.position(pos);
            shortBuffer.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(int[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(int[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(int32Types);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        IntBuffer intData = col.data.asIntBuffer();
        int pos = (int) (rowIdx * col.arraySize);
        intData.position(pos);
        intData.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(int[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(int[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(int32Types);

        for (int i = 0; i < values.length; i++) {
            int[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            IntBuffer intData = col.data.asIntBuffer();
            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            intData.position(pos);
            intData.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(long[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(long[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(int64Types);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        LongBuffer longData = col.data.asLongBuffer();
        int pos = (int) (rowIdx * col.arraySize);
        longData.position(pos);
        longData.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(long[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(long[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(int64Types);

        for (int i = 0; i < values.length; i++) {
            long[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            LongBuffer longData = col.data.asLongBuffer();
            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            longData.position(pos);
            longData.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(float[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(float[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(DUCKDB_TYPE_FLOAT);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        FloatBuffer floatData = col.data.asFloatBuffer();
        int pos = (int) (rowIdx * col.arraySize);
        floatData.position(pos);
        floatData.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(float[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(float[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(DUCKDB_TYPE_FLOAT);

        for (int i = 0; i < values.length; i++) {
            float[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            FloatBuffer floatData = col.data.asFloatBuffer();
            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            floatData.position(pos);
            floatData.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(double[] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(double[] values, boolean[] nullMask) throws SQLException {
        Column col = currentArrayInnerColumn(DUCKDB_TYPE_DOUBLE);
        if (values == null) {
            return appendNull();
        }

        checkArrayLength(col, values.length);
        setArrayNullMask(col, nullMask);

        DoubleBuffer doubleData = col.data.asDoubleBuffer();
        int pos = (int) (rowIdx * col.arraySize);
        doubleData.position(pos);
        doubleData.put(values);

        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(double[][] values) throws SQLException {
        return append(values, null);
    }

    public DuckDBAppender append(double[][] values, boolean[][] nullMask) throws SQLException {
        Column arrayCol = currentArrayInnerColumn(DUCKDB_TYPE_ARRAY);
        if (values == null) {
            return appendNull();
        }
        checkArrayLength(arrayCol, values.length);

        Column col = currentNestedArrayInnerColumn(DUCKDB_TYPE_DOUBLE);

        for (int i = 0; i < values.length; i++) {
            double[] childValues = values[i];

            if (childValues == null) {
                arrayCol.setNull(rowIdx, i);
                continue;
            }
            checkArrayLength(col, childValues.length);
            if (nullMask != null) {
                setArrayNullMask(col, nullMask[i], i);
            }

            DoubleBuffer doubleBuffer = col.data.asDoubleBuffer();
            int pos = (int) ((rowIdx * arrayCol.arraySize + i) * col.arraySize);
            doubleBuffer.position(pos);
            doubleBuffer.put(childValues);
        }

        incrementColOrStructFieldIdx();
        return this;
    }

    // append objects

    public DuckDBAppender append(String value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_VARCHAR);
        if (value == null) {
            return appendNull();
        }

        byte[] bytes = value.getBytes(UTF_8);
        return appendStringOrBlobInternal(DUCKDB_TYPE_VARCHAR, bytes);
    }

    public DuckDBAppender appendUUID(long mostSigBits, long leastSigBits) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_UUID);
        col.data.putLong(leastSigBits);
        mostSigBits ^= Long.MIN_VALUE;
        col.data.putLong(mostSigBits);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(UUID value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_UUID);
        if (value == null) {
            return appendNull();
        }

        long mostSigBits = value.getMostSignificantBits();
        long leastSigBits = value.getLeastSignificantBits();
        return appendUUID(mostSigBits, leastSigBits);
    }

    public DuckDBAppender appendEpochDays(int days) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_DATE);
        col.data.putInt(days);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(LocalDate value) throws SQLException {
        if (value == null) {
            return appendNull();
        }
        long days = value.toEpochDay();
        if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
            throw new SQLException(createErrMsg("unsupported number of days: " + days + ", must fit into 'int32_t'"));
        }
        return appendEpochDays((int) days);
    }

    public DuckDBAppender appendDayMicros(long micros) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_TIME);
        col.data.putLong(micros);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(LocalTime value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_TIME);
        if (value == null) {
            return appendNull();
        }
        long micros = value.toNanoOfDay() / 1000;
        return appendDayMicros(micros);
    }

    public DuckDBAppender appendDayMicros(long micros, int offset) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_TIME_TZ);
        long packed = ((micros & 0xFFFFFFFFFFL) << 24) | (long) (offset & 0xFFFFFF);
        col.data.putLong(packed);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(OffsetTime value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_TIME_TZ);
        if (value == null) {
            return appendNull();
        }
        int offset = value.getOffset().getTotalSeconds();
        long micros = value.toLocalTime().toNanoOfDay() / 1000;
        return appendDayMicros(micros, offset);
    }

    public DuckDBAppender appendEpochSeconds(long seconds) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_TIMESTAMP_S);
        col.data.putLong(seconds);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendEpochMillis(long millis) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_TIMESTAMP_MS);
        col.data.putLong(millis);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendEpochMicros(long micros) throws SQLException {
        Column col = currentColumnWithRowPos(timestampMicrosTypes);
        col.data.putLong(micros);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender appendEpochNanos(long nanos) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_TIMESTAMP_NS);
        col.data.putLong(nanos);
        incrementColOrStructFieldIdx();
        return this;
    }

    public DuckDBAppender append(LocalDateTime value) throws SQLException {
        Column col = currentColumn();
        checkCurrentColumnType(timestampLocalTypes);
        if (value == null) {
            return appendNull();
        }
        switch (col.colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            long seconds = EPOCH_DATE_TIME.until(value, SECONDS);
            return appendEpochSeconds(seconds);
        case DUCKDB_TYPE_TIMESTAMP_MS: {
            long millis = EPOCH_DATE_TIME.until(value, MILLIS);
            return appendEpochMillis(millis);
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            long micros = EPOCH_DATE_TIME.until(value, MICROS);
            return appendEpochMicros(micros);
        }
        case DUCKDB_TYPE_TIMESTAMP_NS: {
            long nanos = EPOCH_DATE_TIME.until(value, NANOS);
            return appendEpochNanos(nanos);
        }
        default:
            throw new SQLException(createErrMsg("invalid column type: " + col.colType));
        }
    }

    public DuckDBAppender append(java.util.Date value) throws SQLException {
        Column col = currentColumn();
        checkCurrentColumnType(timestampLocalTypes);
        if (value == null) {
            return appendNull();
        }
        switch (col.colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            long seconds = value.getTime() / 1000;
            return appendEpochSeconds(seconds);
        case DUCKDB_TYPE_TIMESTAMP_MS: {
            long millis = value.getTime();
            return appendEpochMillis(millis);
        }
        case DUCKDB_TYPE_TIMESTAMP: {
            long micros = Math.multiplyExact(value.getTime(), 1000);
            return appendEpochMicros(micros);
        }
        case DUCKDB_TYPE_TIMESTAMP_NS: {
            long nanos = Math.multiplyExact(value.getTime(), 1000000);
            return appendEpochNanos(nanos);
        }
        default:
            throw new SQLException(createErrMsg("invalid column type: " + col.colType));
        }
    }

    public DuckDBAppender append(OffsetDateTime value) throws SQLException {
        checkCurrentColumnType(DUCKDB_TYPE_TIMESTAMP_TZ);
        if (value == null) {
            return appendNull();
        }
        ZonedDateTime zdt = value.atZoneSameInstant(ZoneOffset.UTC);
        LocalDateTime ldt = zdt.toLocalDateTime();
        long micros = EPOCH_DATE_TIME.until(ldt, MICROS);
        return appendEpochMicros(micros);
    }

    // append special

    public DuckDBAppender appendNull() throws SQLException {
        Column col = currentColumn();
        col.setNull(rowIdx);
        incrementColOrStructFieldIdx();
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
        incrementColOrStructFieldIdx();
        return this;
    }

    // options

    public boolean getWriteInlinedStrings() {
        return writeInlinedStrings;
    }

    public void setWriteInlinedStrings(boolean writeInlinedStrings) {
        this.writeInlinedStrings = writeInlinedStrings;
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

    private void incrementColOrStructFieldIdx() throws SQLException {
        Column col = currentColumn();
        this.prevColumn = currentColumn;
        if (unionBegunInvariant()) {
            this.currentColumn = nextColumn(col.parent);
        } else {
            this.currentColumn = nextColumn(col);
        }
    }

    private Column currentColumn() throws SQLException {
        if (null == currentColumn) {
            throw new SQLException(createErrMsg("current column not found, columns count: " + columns.size()));
        }

        return currentColumn;
    }

    private Column currentArrayInnerColumn(CAPIType ctype) throws SQLException {
        return currentArrayInnerColumn(ctype.typeArray);
    }

    private Column currentArrayInnerColumn(CAPIType[] ctypes) throws SQLException {
        Column parentCol = currentColumn();
        if (parentCol.colType != DUCKDB_TYPE_ARRAY) {
            throw new SQLException(createErrMsg("invalid array column type: '" + parentCol.colType + "'"));
        }

        Column col = parentCol.children.get(0);
        for (CAPIType ct : ctypes) {
            if (col.colType == ct) {
                return col;
            }
        }
        throw new SQLException(createErrMsg("invalid array inner column type, expected one of: '" +
                                            Arrays.toString(ctypes) + "', actual: '" + col.colType + "'"));
    }

    private Column currentNestedArrayInnerColumn(CAPIType ctype) throws SQLException {
        return currentNestedArrayInnerColumn(ctype.typeArray);
    }

    private Column currentNestedArrayInnerColumn(CAPIType[] ctypes) throws SQLException {
        Column parentCol = currentColumn();
        if (parentCol.colType != DUCKDB_TYPE_ARRAY) {
            throw new SQLException(createErrMsg("invalid array column type: '" + parentCol.colType + "'"));
        }

        Column arrayCol = parentCol.children.get(0);
        if (arrayCol.colType != DUCKDB_TYPE_ARRAY) {
            throw new SQLException(createErrMsg("invalid nested array column type: '" + arrayCol.colType + "'"));
        }

        Column col = arrayCol.children.get(0);
        for (CAPIType ct : ctypes) {
            if (col.colType == ct) {
                return col;
            }
        }
        throw new SQLException(createErrMsg("invalid  nested array inner column type, expected one of: '" +
                                            Arrays.toString(ctypes) + "', actual: '" + col.colType + "'"));
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

    private void checkArrayLength(Column col, int length) throws SQLException {
        if (col.arraySize != length) {
            throw new SQLException(
                createErrMsg("invalid array size, expected: " + col.arraySize + ", actual: " + length));
        }
    }

    private void setArrayNullMask(Column col, boolean[] nullMask) throws SQLException {
        setArrayNullMask(col, nullMask, 0);
    }

    private void setArrayNullMask(Column col, boolean[] nullMask, int parentArrayIdx) throws SQLException {
        if (null == nullMask) {
            return;
        }
        //        if (nullMask.length != col.arraySize) {
        //            throw new SQLException(createErrMsg("invalid null mask size, expected: " + col.arraySize +
        //                                                ", actual: " + nullMask.length));
        //        }
        for (int i = 0; i < nullMask.length; i++) {
            if (nullMask[i]) {
                col.setNull(rowIdx, (int) (i + col.arraySize * parentArrayIdx));
            }
        }
    }

    private Column currentDecimalColumnWithRowPos(CAPIType decimalInternalType) throws SQLException {
        Column col = currentColumnWithRowPos(DUCKDB_TYPE_DECIMAL);
        if (col.decimalInternalType != decimalInternalType) {
            throw new SQLException(createErrMsg("invalid decimal internal type, expected: '" + col.decimalInternalType +
                                                "', actual: '" + decimalInternalType + "'"));
        }
        setRowPos(col, col.decimalInternalType.widthBytes);
        return col;
    }

    private Column currentColumnWithRowPos(CAPIType ctype) throws SQLException {
        return currentColumnWithRowPos(ctype.typeArray);
    }

    private void setRowPos(Column col, long widthBytes) throws SQLException {
        long pos = rowIdx * widthBytes;
        if (pos >= col.data.capacity()) {
            throw new SQLException(
                createErrMsg("invalid calculated position: " + pos + ", type: '" + col.colType + "'"));
        }
        col.data.position((int) (pos));
    }

    private Column currentColumnWithRowPos(CAPIType[] ctypes) throws SQLException {
        Column col = currentColumn();

        boolean typeMatches = false;
        for (CAPIType ct : ctypes) {
            if (col.colType.typeId == ct.typeId) {
                typeMatches = true;
            }
            if (col.colType.widthBytes != ct.widthBytes) {
                throw new SQLException(
                    createErrMsg("invalid columns type width, expected: '" + ct + "', actual: '" + col.colType + "'"));
            }
        }
        if (!typeMatches) {
            String[] typeStrs = new String[ctypes.length];
            for (int i = 0; i < ctypes.length; i++) {
                typeStrs[i] = String.valueOf(ctypes[i]);
            }
            throw new SQLException(createErrMsg("invalid columns type, expected one of: '" + Arrays.toString(typeStrs) +
                                                "', actual: '" + col.colType + "'"));
        }

        if (col.colType.widthBytes > 0) {
            setRowPos(col, col.colType.widthBytes);
        }

        return col;
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

    private void checkDecimalPrecision(BigDecimal value, CAPIType decimalInternalType, int maxPrecision)
        throws SQLException {
        if (value.precision() > maxPrecision) {
            throw new SQLException(createErrMsg("invalid decimal precision, value: " + value.precision() +
                                                ", max value: " + maxPrecision +
                                                ", decimal internal type: " + decimalInternalType));
        }
    }

    private DuckDBAppender appendStringOrBlobInternal(CAPIType ctype, byte[] bytes) throws SQLException {
        if (writeInlinedStrings && bytes.length < STRING_MAX_INLINE_BYTES) {
            Column col = currentColumnWithRowPos(ctype);
            col.data.putInt(bytes.length);
            if (bytes.length > 0) {
                col.data.put(bytes);
            }
        } else {
            Column col = currentColumn();
            checkColumnType(col, ctype);
            appenderRefLock.lock();
            try {
                checkOpen();
                duckdb_vector_assign_string_element_len(col.vectorRef, rowIdx, bytes);
            } finally {
                appenderRefLock.unlock();
            }
        }

        incrementColOrStructFieldIdx();
        return this;
    }

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
        private ByteBuffer data;
        private ByteBuffer validity;
        private final List<Column> children = new ArrayList<>();

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

            if (null == parent || parent.colType != DUCKDB_TYPE_ARRAY) {
                this.arraySize = 1;
            } else {
                this.arraySize = duckdb_array_type_array_size(parent.colTypeRef);
            }

            if (structFieldIdx >= 0) {
                byte[] nameUTF8 = duckdb_struct_type_child_name(parent.colTypeRef, structFieldIdx);
                this.structFieldName = strFromUTF8(nameUTF8);
            } else {
                this.structFieldName = null;
            }

            this.vectorRef = vector;

            if (colType.widthBytes > 0 || colType == DUCKDB_TYPE_DECIMAL) {
                this.data = duckdb_vector_get_data(vectorRef, widthBytes() * arraySize * parentArraySize());
                if (null == this.data) {
                    throw new SQLException("cannot initialize data chunk vector data");
                }
            } else {
                this.data = null;
            }

            duckdb_vector_ensure_validity_writable(vectorRef);
            this.validity = duckdb_vector_get_validity(vectorRef, arraySize * parentArraySize());
            if (null == this.validity) {
                throw new SQLException("cannot initialize data chunk vector validity");
            }

            // last call in constructor
            initVecChildren(this);
        }

        void reset() throws SQLException {
            if (null != this.data) {
                this.data = duckdb_vector_get_data(vectorRef, widthBytes() * arraySize * parentArraySize());
                if (null == this.data) {
                    throw new SQLException("cannot reset data chunk vector data");
                }
            }

            duckdb_vector_ensure_validity_writable(vectorRef);
            this.validity = duckdb_vector_get_validity(vectorRef, arraySize * parentArraySize());
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

        void setNull(long rowIdx) throws SQLException {
            if (1 != arraySize) {
                throw new SQLException("Invalid API usage for array, size: " + arraySize);
            }
            setNull(rowIdx, 0);
            for (Column col : children) {
                for (int i = 0; i < col.arraySize; i++) {
                    col.setNull(rowIdx, i);
                }
            }
        }

        void setNull(long rowIdx, int arrayIdx) {
            LongBuffer entries = this.validity.asLongBuffer();

            long vectorPos = rowIdx * arraySize * parentArraySize() + arrayIdx;
            long validityPos = vectorPos / 64;
            entries.position((int) validityPos);
            long mask = entries.get();

            long idxInEntry = vectorPos % 64;
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
    }
}

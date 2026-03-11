package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_ARRAY;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_BLOB;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_BOOLEAN;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_DATE;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_DECIMAL;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_DOUBLE;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_ENUM;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_FLOAT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_HUGEINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_INTEGER;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_LIST;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_MAP;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_SMALLINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_STRUCT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_MS;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_NS;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_S;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_TZ;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME_NS;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TIME_TZ;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_TINYINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UBIGINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UHUGEINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UINTEGER;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UNION;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_USMALLINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UTINYINT;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_UUID;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_VARCHAR;
import static org.duckdb.DuckDBBindings.duckdb_data_chunk_get_column_count;
import static org.duckdb.DuckDBBindings.duckdb_data_chunk_get_vector;
import static org.duckdb.DuckDBBindings.duckdb_destroy_logical_type;
import static org.duckdb.DuckDBBindings.duckdb_list_vector_get_size;
import static org.duckdb.DuckDBBindings.duckdb_list_vector_reserve;
import static org.duckdb.DuckDBBindings.duckdb_list_vector_set_size;
import static org.duckdb.DuckDBBindings.duckdb_vector_assign_string_element_len;
import static org.duckdb.DuckDBBindings.duckdb_vector_get_column_type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.duckdb.DuckDBVectorWriteCore.Column;

public final class UdfOutputAppender implements AutoCloseable {
    private static final long UNSIGNED_INT_MAX = 0xFFFF_FFFFL;
    private static final LocalDateTime EPOCH_DATE_TIME = LocalDateTime.ofEpochSecond(0, 0, UTC);
    private static final int MAX_TZ_SECONDS = 16 * 60 * 60 - 1;

    private final int rowCapacity;
    private final List<Column> columns;
    private final UdfScalarWriter[] scalarWriters;

    private int rowCount;
    private int pendingColumnIndex = -1;
    private boolean closed;

    public UdfOutputAppender(ByteBuffer chunkRef) throws SQLException {
        Objects.requireNonNull(chunkRef, "chunkRef");

        this.rowCapacity = Math.toIntExact(DuckDBVectorWriteCore.MAX_TOP_LEVEL_ROWS);

        int columnCount = Math.toIntExact(duckdb_data_chunk_get_column_count(chunkRef));
        ByteBuffer[] colTypes = new ByteBuffer[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ByteBuffer vector = duckdb_data_chunk_get_vector(chunkRef, i);
            if (vector == null) {
                throw new SQLException("cannot initialize output chunk vector");
            }
            colTypes[i] = duckdb_vector_get_column_type(vector);
            if (colTypes[i] == null) {
                throw new SQLException("cannot initialize output chunk vector type");
            }
        }

        List<Column> createdColumns;
        try {
            createdColumns = DuckDBVectorWriteCore.createTopLevelColumns(chunkRef, colTypes);
        } catch (Exception e) {
            for (ByteBuffer colType : colTypes) {
                if (colType != null) {
                    duckdb_destroy_logical_type(colType);
                }
            }
            throw e;
        }
        this.columns = createdColumns;
        this.scalarWriters = new UdfScalarWriter[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                Column column = columns.get(i);
                DuckDBColumnType type;
                try {
                    type = UdfTypeCatalog.fromCapiTypeId(column.colType.typeId);
                } catch (Exception unsupportedType) {
                    continue;
                }
                ByteBuffer data = UdfTypeCatalog.requiresVectorRef(type) ? null : column.data;
                ByteBuffer vectorRef = UdfTypeCatalog.requiresVectorRef(type) ? column.vectorRef : null;
                scalarWriters[i] =
                    new UdfScalarWriter(column.colType.typeId, data, vectorRef, column.validity, rowCapacity);
            }
        } catch (Exception e) {
            destroyColumns();
            throw e;
        }
    }

    public int getColumnCount() {
        checkOpen();
        return columns.size();
    }

    public int getRowCapacity() {
        return rowCapacity;
    }

    public int getSize() {
        return rowCount;
    }

    public void setNull(int columnIndex, int row) {
        checkOpen();
        checkColumnIndex(columnIndex);
        checkRowIndex(row);
        try {
            columns.get(columnIndex).setNull(row);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set NULL value: " + e.getMessage(), e);
        }
    }

    public void setInt(int columnIndex, int row, int value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_INT, "setInt").setInt(row, value);
    }

    public void setLong(int columnIndex, int row, long value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_LONG, "setLong").setLong(row, value);
    }

    public void setFloat(int columnIndex, int row, float value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_FLOAT, "setFloat").setFloat(row, value);
    }

    public void setDouble(int columnIndex, int row, double value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_DOUBLE, "setDouble").setDouble(row, value);
    }

    public void setBoolean(int columnIndex, int row, boolean value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_BOOLEAN, "setBoolean").setBoolean(row, value);
    }

    public void setString(int columnIndex, int row, String value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_STRING, "setString").setString(row, value);
    }

    public void setBytes(int columnIndex, int row, byte[] value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_BYTES, "setBytes").setBytes(row, value);
    }

    public void setObject(int columnIndex, int row, Object value) {
        checkOpen();
        checkColumnIndex(columnIndex);
        checkRowIndex(row);
        try {
            writeValue(columns.get(columnIndex), row, value);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set value in output column: " + e.getMessage(), e);
        }
    }

    public void setBigDecimal(int columnIndex, int row, BigDecimal value) {
        checkOpen();
        checkRowIndex(row);
        scalarWriter(columnIndex, UdfTypeCatalog.Accessor.SET_DECIMAL, "setBigDecimal").setBigDecimal(row, value);
    }

    public void setLocalDate(int columnIndex, int row, LocalDate value) {
        setObject(columnIndex, row, value);
    }

    public void setLocalTime(int columnIndex, int row, LocalTime value) {
        setObject(columnIndex, row, value);
    }

    public void setOffsetTime(int columnIndex, int row, OffsetTime value) {
        setObject(columnIndex, row, value);
    }

    public void setLocalDateTime(int columnIndex, int row, LocalDateTime value) {
        setObject(columnIndex, row, value);
    }

    public void setDate(int columnIndex, int row, java.util.Date value) {
        setObject(columnIndex, row, value);
    }

    public void setOffsetDateTime(int columnIndex, int row, OffsetDateTime value) {
        setObject(columnIndex, row, value);
    }

    public void setUUID(int columnIndex, int row, UUID value) {
        setObject(columnIndex, row, value);
    }

    public UdfOutputAppender beginRow() {
        checkOpen();
        if (pendingColumnIndex >= 0) {
            throw new IllegalStateException("endRow must be called before beginRow");
        }
        if (rowCount >= rowCapacity) {
            throw new IllegalStateException("output row capacity exceeded");
        }
        pendingColumnIndex = 0;
        return this;
    }

    public UdfOutputAppender appendNull() {
        return appendNextValue(null);
    }

    public UdfOutputAppender append(boolean value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(int value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(long value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(float value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(double value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(BigDecimal value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(String value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(byte[] value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(LocalDate value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(LocalTime value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(OffsetTime value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(LocalDateTime value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(java.util.Date value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(OffsetDateTime value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(UUID value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender append(Object value) {
        return appendNextValue(value);
    }

    public UdfOutputAppender endRow() {
        checkOpen();
        if (pendingColumnIndex < 0) {
            throw new IllegalStateException("beginRow must be called before endRow");
        }
        if (pendingColumnIndex != columns.size()) {
            throw new IllegalStateException("all columns must be appended before endRow");
        }
        pendingColumnIndex = -1;
        rowCount++;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        destroyColumns();
        closed = true;
    }

    private UdfOutputAppender appendNextValue(Object value) {
        Column column = nextColumnForAppendColumn();
        try {
            writeValue(column, rowCount, value);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to append row value: " + e.getMessage(), e);
        }
        pendingColumnIndex++;
        return this;
    }

    private void destroyColumns() {
        for (Column column : columns) {
            column.destroy();
        }
    }

    private Column nextColumnForAppendColumn() {
        checkOpen();
        if (pendingColumnIndex < 0) {
            throw new IllegalStateException("beginRow must be called before append");
        }
        if (pendingColumnIndex >= columns.size()) {
            throw new IllegalStateException("too many values appended in current row");
        }
        return columns.get(pendingColumnIndex);
    }

    private UdfScalarWriter scalarWriter(int columnIndex, UdfTypeCatalog.Accessor accessor, String method) {
        checkColumnIndex(columnIndex);
        UdfScalarWriter writer = scalarWriters[columnIndex];
        if (writer == null) {
            throw new UnsupportedOperationException(method + " is not available for non-scalar output type " +
                                                    columns.get(columnIndex).colType + " at column " + columnIndex +
                                                    "; use setObject/append(Object) for nested types");
        }
        if (!UdfTypeCatalog.supportsAccessor(writer.getType(), accessor)) {
            throw new UnsupportedOperationException(method + " is not supported for output type " + writer.getType() +
                                                    " at column " + columnIndex);
        }
        return writer;
    }

    private void checkColumnIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            throw new IndexOutOfBoundsException("column=" + columnIndex + ", columnCount=" + columns.size());
        }
    }

    private void checkRowIndex(int row) {
        if (row < 0 || row >= rowCapacity) {
            throw new IndexOutOfBoundsException("row=" + row + ", rowCapacity=" + rowCapacity);
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("UdfOutputAppender is closed");
        }
    }

    private void writeValue(Column col, long vectorIdx, Object value) throws SQLException {
        if (value == null) {
            col.setNull(vectorIdx);
            return;
        }
        switch (col.colType) {
        case DUCKDB_TYPE_BOOLEAN:
            putByte(col, vectorIdx, (byte) (requireBoolean(value) ? 1 : 0));
            return;
        case DUCKDB_TYPE_TINYINT:
            putByte(col, vectorIdx, (byte) requireSignedLongInRange(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT"));
            return;
        case DUCKDB_TYPE_UTINYINT:
            putByte(col, vectorIdx, (byte) requireSignedLongInRange(value, 0, 0xFFL, "UTINYINT"));
            return;
        case DUCKDB_TYPE_SMALLINT:
            putShort(col, vectorIdx,
                     (short) requireSignedLongInRange(value, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT"));
            return;
        case DUCKDB_TYPE_USMALLINT:
            putShort(col, vectorIdx, (short) requireSignedLongInRange(value, 0, 0xFFFFL, "USMALLINT"));
            return;
        case DUCKDB_TYPE_INTEGER:
            putInt(col, vectorIdx,
                   (int) requireSignedLongInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER"));
            return;
        case DUCKDB_TYPE_UINTEGER:
            putInt(col, vectorIdx, (int) requireSignedLongInRange(value, 0, UNSIGNED_INT_MAX, "UINTEGER"));
            return;
        case DUCKDB_TYPE_BIGINT:
        case DUCKDB_TYPE_UBIGINT:
        case DUCKDB_TYPE_TIME:
        case DUCKDB_TYPE_TIME_NS:
        case DUCKDB_TYPE_TIME_TZ:
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_S:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP_NS:
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            putLong(col, vectorIdx, requireLongOrTemporal(value, col.colType));
            return;
        case DUCKDB_TYPE_FLOAT:
            putFloat(col, vectorIdx, (float) requireDouble(value));
            return;
        case DUCKDB_TYPE_DOUBLE:
            putDouble(col, vectorIdx, requireDouble(value));
            return;
        case DUCKDB_TYPE_DECIMAL:
            UdfNative.setDecimal(col.vectorRef, Math.toIntExact(vectorIdx), requireBigDecimal(value));
            return;
        case DUCKDB_TYPE_DATE:
            putInt(col, vectorIdx, requireDateEpochDays(value));
            return;
        case DUCKDB_TYPE_HUGEINT:
        case DUCKDB_TYPE_UHUGEINT:
            putFixedWidthBytes(col, vectorIdx, requireBytes(value), 16, col.colType.toString());
            return;
        case DUCKDB_TYPE_UUID:
            if (value instanceof UUID) {
                UUID uuid = (UUID) value;
                putUUID(col, vectorIdx, uuid);
            } else {
                putFixedWidthBytes(col, vectorIdx, requireBytes(value), 16, "UUID");
            }
            return;
        case DUCKDB_TYPE_VARCHAR:
            putStringOrBlob(col, vectorIdx, requireString(value).getBytes(UTF_8));
            return;
        case DUCKDB_TYPE_BLOB:
            putStringOrBlob(col, vectorIdx, requireBytes(value));
            return;
        case DUCKDB_TYPE_ENUM:
            putEnum(col, vectorIdx, requireString(value));
            return;
        case DUCKDB_TYPE_ARRAY:
        case DUCKDB_TYPE_LIST:
            writeCollection(col, vectorIdx, value);
            return;
        case DUCKDB_TYPE_MAP:
            writeMap(col, vectorIdx, value);
            return;
        case DUCKDB_TYPE_STRUCT:
            writeStruct(col, vectorIdx, value);
            return;
        case DUCKDB_TYPE_UNION:
            writeUnion(col, vectorIdx, value);
            return;
        default:
            throw new IllegalArgumentException("Unsupported output type for UdfOutputAppender: " + col.colType);
        }
    }

    private void writeCollection(Column parentCol, long vectorIdx, Object value) throws SQLException {
        Column innerCol = requireSingleChild(parentCol, "collection");
        List<?> values = asListValues(value);
        int count = values.size();
        int offset = prepareListColumn(innerCol, vectorIdx, count);
        for (int i = 0; i < count; i++) {
            writeValue(innerCol, offset + i, values.get(i));
        }
    }

    private void writeMap(Column mapCol, long vectorIdx, Object value) throws SQLException {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected java.util.Map for MAP column but got " +
                                               value.getClass().getName());
        }
        Column entryStructCol = requireSingleChild(mapCol, "map");
        Map<?, ?> map = (Map<?, ?>) value;
        int offset = prepareListColumn(entryStructCol, vectorIdx, map.size());
        int index = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            writeStructByPosition(entryStructCol, offset + index, Arrays.asList(entry.getKey(), entry.getValue()));
            index++;
        }
    }

    private void writeStruct(Column structCol, long vectorIdx, Object value) throws SQLException {
        if (value instanceof Map) {
            writeStructByName(structCol, vectorIdx, (Map<?, ?>) value);
            return;
        }
        writeStructByPosition(structCol, vectorIdx, asListValues(value));
    }

    private void writeStructByName(Column structCol, long vectorIdx, Map<?, ?> values) throws SQLException {
        if (structCol.children.size() != values.size()) {
            throw new IllegalArgumentException("Struct field count mismatch, expected " + structCol.children.size() +
                                               " values but got " + values.size());
        }
        for (Column child : structCol.children) {
            String fieldName = child.structFieldName;
            if (!values.containsKey(fieldName)) {
                throw new IllegalArgumentException("Struct value map does not contain field '" + fieldName + "'");
            }
            writeValue(child, vectorIdx, values.get(fieldName));
        }
    }

    private void writeStructByPosition(Column structCol, long vectorIdx, List<?> values) throws SQLException {
        if (structCol.children.size() != values.size()) {
            throw new IllegalArgumentException("Struct field count mismatch, expected " + structCol.children.size() +
                                               " values but got " + values.size());
        }
        for (int i = 0; i < values.size(); i++) {
            writeValue(structCol.children.get(i), vectorIdx, values.get(i));
        }
    }

    private void writeUnion(Column unionCol, long vectorIdx, Object value) throws SQLException {
        if (!(value instanceof AbstractMap.SimpleEntry)) {
            throw new IllegalArgumentException(
                "Union values must be java.util.AbstractMap.SimpleEntry<String, Object>");
        }
        AbstractMap.SimpleEntry<?, ?> entry = (AbstractMap.SimpleEntry<?, ?>) value;
        String tag = String.valueOf(entry.getKey());
        Column selected = selectUnionField(unionCol, vectorIdx, tag);
        writeValue(selected, vectorIdx, entry.getValue());
    }

    private Column selectUnionField(Column unionCol, long vectorIdx, String tag) throws SQLException {
        if (unionCol.children.isEmpty()) {
            throw new IllegalArgumentException("Invalid UNION column without children");
        }
        int selectedIndex = -1;
        for (int i = 1; i < unionCol.children.size(); i++) {
            Column child = unionCol.children.get(i);
            if (Objects.equals(child.structFieldName, tag)) {
                selectedIndex = i;
                break;
            }
        }
        if (selectedIndex < 0) {
            throw new IllegalArgumentException("Unknown UNION tag '" + tag + "'");
        }

        Column tagCol = unionCol.children.get(0);
        putByte(tagCol, vectorIdx, (byte) (selectedIndex - 1));
        for (int i = 1; i < unionCol.children.size(); i++) {
            if (i != selectedIndex) {
                unionCol.children.get(i).setNull(vectorIdx);
            }
        }
        return unionCol.children.get(selectedIndex);
    }

    private void putEnum(Column col, long vectorIdx, String value) {
        Integer dictValue = col.enumDict.get(value);
        if (dictValue == null) {
            throw new IllegalArgumentException("Invalid enum value '" + value + "', expected one of " +
                                               col.enumDict.keySet());
        }

        int pos = (int) (vectorIdx * col.enumInternalType.widthBytes);
        col.data.position(pos);
        switch (col.enumInternalType) {
        case DUCKDB_TYPE_UTINYINT:
            col.data.put(dictValue.byteValue());
            return;
        case DUCKDB_TYPE_USMALLINT:
            col.data.putShort(dictValue.shortValue());
            return;
        case DUCKDB_TYPE_UINTEGER:
            col.data.putInt(dictValue);
            return;
        default:
            throw new IllegalArgumentException("Unsupported enum storage type " + col.enumInternalType);
        }
    }

    private int prepareListColumn(Column innerCol, long vectorIdx, long listElementsCount) throws SQLException {
        if (innerCol.parent == null) {
            throw new IllegalArgumentException("Invalid collection inner column");
        }
        Column parentCol = innerCol.parent;
        switch (parentCol.colType) {
        case DUCKDB_TYPE_ARRAY:
            if (innerCol.arraySize != listElementsCount) {
                throw new IllegalArgumentException("Fixed ARRAY size mismatch, expected " + innerCol.arraySize +
                                                   " values but got " + listElementsCount);
            }
            return (int) (vectorIdx * innerCol.arraySize);
        case DUCKDB_TYPE_LIST:
        case DUCKDB_TYPE_MAP:
            long offset = duckdb_list_vector_get_size(parentCol.vectorRef);
            LongBuffer listEntries = parentCol.data.asLongBuffer();
            int entryPos = (int) (vectorIdx * DUCKDB_TYPE_LIST.widthBytes / Long.BYTES);
            listEntries.position(entryPos);
            listEntries.put(offset);
            listEntries.put(listElementsCount);
            long listSize = offset + listElementsCount;
            int reserveStatus = duckdb_list_vector_reserve(parentCol.vectorRef, listSize);
            if (reserveStatus != 0) {
                throw new SQLException("'duckdb_list_vector_reserve' failed for list size " + listSize);
            }
            innerCol.reset(listSize);
            int setSizeStatus = duckdb_list_vector_set_size(parentCol.vectorRef, listSize);
            if (setSizeStatus != 0) {
                throw new SQLException("'duckdb_list_vector_set_size' failed for list size " + listSize);
            }
            return (int) offset;
        default:
            throw new IllegalArgumentException("Invalid collection parent type " + parentCol.colType);
        }
    }

    private static Column requireSingleChild(Column parentCol, String kind) {
        if (parentCol.children.size() != 1) {
            throw new IllegalArgumentException("Invalid " + kind + " type layout, expected single child");
        }
        return parentCol.children.get(0);
    }

    private static List<?> asListValues(Object value) {
        if (value instanceof List) {
            return (List<?>) value;
        }
        if (value instanceof Collection) {
            return new ArrayList<>((Collection<?>) value);
        }
        if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        if (value instanceof boolean[]) {
            boolean[] arr = (boolean[]) value;
            List<Boolean> out = new ArrayList<>(arr.length);
            for (boolean v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof byte[]) {
            byte[] arr = (byte[]) value;
            List<Byte> out = new ArrayList<>(arr.length);
            for (byte v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof short[]) {
            short[] arr = (short[]) value;
            List<Short> out = new ArrayList<>(arr.length);
            for (short v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof int[]) {
            int[] arr = (int[]) value;
            List<Integer> out = new ArrayList<>(arr.length);
            for (int v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof long[]) {
            long[] arr = (long[]) value;
            List<Long> out = new ArrayList<>(arr.length);
            for (long v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof float[]) {
            float[] arr = (float[]) value;
            List<Float> out = new ArrayList<>(arr.length);
            for (float v : arr) {
                out.add(v);
            }
            return out;
        }
        if (value instanceof double[]) {
            double[] arr = (double[]) value;
            List<Double> out = new ArrayList<>(arr.length);
            for (double v : arr) {
                out.add(v);
            }
            return out;
        }
        throw new IllegalArgumentException("Expected collection/array value but got " + value.getClass().getName());
    }

    private static boolean requireBoolean(Object value) {
        if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("Expected Boolean value but got " + value.getClass().getName());
        }
        return (Boolean) value;
    }

    private static BigDecimal requireBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        throw new IllegalArgumentException("Expected BigDecimal/Number value but got " + value.getClass().getName());
    }

    private static int requireDateEpochDays(Object value) {
        if (value instanceof LocalDate) {
            long days = ((LocalDate) value).toEpochDay();
            if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Expected LocalDate epoch day to fit int32 but got " + days);
            }
            return (int) days;
        }
        return (int) requireSignedLongInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, "DATE");
    }

    private static long requireLongOrTemporal(Object value, DuckDBBindings.CAPIType colType) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        switch (colType) {
        case DUCKDB_TYPE_TIME:
            if (value instanceof LocalTime) {
                return ((LocalTime) value).toNanoOfDay() / 1000L;
            }
            break;
        case DUCKDB_TYPE_TIME_NS:
            if (value instanceof LocalTime) {
                return ((LocalTime) value).toNanoOfDay();
            }
            break;
        case DUCKDB_TYPE_TIME_TZ:
            if (value instanceof OffsetTime) {
                OffsetTime time = (OffsetTime) value;
                return packTimeTzMicros(time.toLocalTime().toNanoOfDay() / 1000L, time.getOffset().getTotalSeconds());
            }
            break;
        case DUCKDB_TYPE_TIMESTAMP_S:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_NS:
            if (value instanceof LocalDateTime) {
                return localDateTimeToMoment((LocalDateTime) value, colType);
            }
            if (value instanceof java.util.Date) {
                return dateToMoment((java.util.Date) value, colType);
            }
            break;
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            if (value instanceof OffsetDateTime) {
                ZonedDateTime zdt = ((OffsetDateTime) value).atZoneSameInstant(UTC);
                return EPOCH_DATE_TIME.until(zdt.toLocalDateTime(), MICROS);
            }
            if (value instanceof java.util.Date) {
                return Math.multiplyExact(((java.util.Date) value).getTime(), 1000L);
            }
            break;
        default:
            break;
        }
        throw new IllegalArgumentException("Expected numeric/temporal value compatible with " + colType + " but got " +
                                           value.getClass().getName());
    }

    private static long localDateTimeToMoment(LocalDateTime value, DuckDBBindings.CAPIType colType) {
        switch (colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            return EPOCH_DATE_TIME.until(value, SECONDS);
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return EPOCH_DATE_TIME.until(value, MILLIS);
        case DUCKDB_TYPE_TIMESTAMP:
            return EPOCH_DATE_TIME.until(value, MICROS);
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return EPOCH_DATE_TIME.until(value, NANOS);
        default:
            throw new IllegalArgumentException("Unsupported LocalDateTime conversion for " + colType);
        }
    }

    private static long dateToMoment(java.util.Date value, DuckDBBindings.CAPIType colType) {
        long millis = value.getTime();
        switch (colType) {
        case DUCKDB_TYPE_TIMESTAMP_S:
            return millis / 1000L;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return millis;
        case DUCKDB_TYPE_TIMESTAMP:
            return Math.multiplyExact(millis, 1000L);
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return Math.multiplyExact(millis, 1000000L);
        default:
            throw new IllegalArgumentException("Unsupported java.util.Date conversion for " + colType);
        }
    }

    private static long packTimeTzMicros(long micros, int offsetSeconds) {
        long normalizedOffset = MAX_TZ_SECONDS - offsetSeconds;
        return ((micros & 0xFFFFFFFFFFL) << 24) | (normalizedOffset & 0xFFFFFFL);
    }

    private static long requireLong(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Expected numeric value but got " + value.getClass().getName());
        }
        return ((Number) value).longValue();
    }

    private static double requireDouble(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Expected numeric value but got " + value.getClass().getName());
        }
        return ((Number) value).doubleValue();
    }

    private static long requireSignedLongInRange(Object value, long min, long max, String typeName) {
        long num = requireLong(value);
        if (num < min || num > max) {
            throw new IllegalArgumentException("Value out of range for " + typeName + ": " + num);
        }
        return num;
    }

    private static String requireString(Object value) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Expected String value but got " + value.getClass().getName());
        }
        return (String) value;
    }

    private static byte[] requireBytes(Object value) {
        if (!(value instanceof byte[])) {
            throw new IllegalArgumentException("Expected byte[] value but got " + value.getClass().getName());
        }
        return (byte[]) value;
    }

    private static void putByte(Column col, long vectorIdx, byte value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.put(value);
    }

    private static void putShort(Column col, long vectorIdx, short value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putShort(value);
    }

    private static void putInt(Column col, long vectorIdx, int value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putInt(value);
    }

    private static void putLong(Column col, long vectorIdx, long value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putLong(value);
    }

    private static void putFloat(Column col, long vectorIdx, float value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putFloat(value);
    }

    private static void putDouble(Column col, long vectorIdx, double value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.putDouble(value);
    }

    private static void putUUID(Column col, long vectorIdx, UUID value) {
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        long leastSigBits = value.getLeastSignificantBits();
        long mostSigBits = value.getMostSignificantBits();
        col.data.putLong(leastSigBits);
        col.data.putLong(mostSigBits ^ Long.MIN_VALUE);
    }

    private static void putFixedWidthBytes(Column col, long vectorIdx, byte[] value, int width, String typeName) {
        if (value.length != width) {
            throw new IllegalArgumentException("Expected " + width + " bytes for " + typeName + " value, got " +
                                               value.length);
        }
        int pos = (int) (vectorIdx * col.colType.widthBytes);
        col.data.position(pos);
        col.data.put(value);
    }

    private static void putStringOrBlob(Column col, long vectorIdx, byte[] value) {
        duckdb_vector_assign_string_element_len(col.vectorRef, vectorIdx, value);
    }
}

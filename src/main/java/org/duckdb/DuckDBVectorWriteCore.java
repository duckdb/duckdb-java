package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class DuckDBVectorWriteCore {
    static final long MAX_TOP_LEVEL_ROWS = duckdb_vector_size();

    private DuckDBVectorWriteCore() {
    }

    static List<Column> createTopLevelColumns(ByteBuffer chunkRef, ByteBuffer[] colTypes) throws SQLException {
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

    private static Map<String, Integer> readEnumDict(ByteBuffer colTypeRef) {
        Map<String, Integer> dict = new LinkedHashMap<>();
        long size = duckdb_enum_dictionary_size(colTypeRef);
        for (long i = 0; i < size; i++) {
            byte[] nameUtf8 = duckdb_enum_dictionary_value(colTypeRef, i);
            String name = new String(nameUtf8, UTF_8);
            dict.put(name, (int) i);
        }
        return dict;
    }

    static final class Column {
        final Column parent;
        final int idx;
        ByteBuffer colTypeRef;
        final CAPIType colType;
        final CAPIType decimalInternalType;
        final int decimalPrecision;
        final int decimalScale;
        final long arraySize;
        final String structFieldName;
        final Map<String, Integer> enumDict;
        final CAPIType enumInternalType;

        final ByteBuffer vectorRef;
        final List<Column> children = new ArrayList<>();

        long listSize = 0;
        ByteBuffer data = null;
        ByteBuffer validity = null;

        Column(Column parent, int idx, ByteBuffer colTypeRef, ByteBuffer vector) throws SQLException {
            this(parent, idx, colTypeRef, vector, -1);
        }

        Column(Column parent, int idx, ByteBuffer colTypeRef, ByteBuffer vector, int structFieldIdx)
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
                this.structFieldName = new String(nameUTF8, UTF_8);
            } else {
                this.structFieldName = null;
            }

            this.vectorRef = vector;

            if (null == parent || parent.colType != DUCKDB_TYPE_ARRAY) {
                this.arraySize = 1;
            } else {
                this.arraySize = duckdb_array_type_array_size(parent.colTypeRef);
            }

            if (colType == DUCKDB_TYPE_ENUM) {
                this.enumDict = readEnumDict(this.colTypeRef);
                int enumInternalTypeId = duckdb_enum_internal_type(this.colTypeRef);
                this.enumInternalType = capiTypeFromTypeId(enumInternalTypeId);
            } else {
                this.enumDict = null;
                this.enumInternalType = null;
            }

            long maxElems = maxElementsCount();
            if (colType.widthBytes > 0 || colType == DUCKDB_TYPE_DECIMAL || colType == DUCKDB_TYPE_ENUM) {
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

            // Last call in constructor, after the current column is fully initialized.
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

        private void setNullOnVectorIdx(long vectorIdx) {
            long validityPos = vectorIdx / 64;
            LongBuffer entries = validity.asLongBuffer();
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
            } else if (colType == DUCKDB_TYPE_ENUM) {
                return enumInternalType.widthBytes;
            } else {
                return colType.widthBytes;
            }
        }

        private long parentArraySize() {
            if (null == parent) {
                return 1;
            }
            return parent.arraySize;
        }

        private long maxElementsCount() {
            Column ancestor = this;
            while (null != ancestor) {
                if (null != ancestor.parent &&
                    (ancestor.parent.colType == DUCKDB_TYPE_LIST || ancestor.parent.colType == DUCKDB_TYPE_MAP)) {
                    break;
                }
                ancestor = ancestor.parent;
            }
            long maxEntries = null != ancestor ? ancestor.listSize : MAX_TOP_LEVEL_ROWS;
            return maxEntries * arraySize * parentArraySize();
        }
    }
}

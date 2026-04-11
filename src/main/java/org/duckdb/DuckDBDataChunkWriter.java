package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;

public class DuckDBDataChunkWriter {
    private final ByteBuffer chunkRef;
    private final long rowCount;
    private final long columnCount;
    private final DuckDBWritableVector[] vectors;

    DuckDBDataChunkWriter(ByteBuffer chunkRef) {
        if (chunkRef == null) {
            throw new DuckDBFunctions.FunctionException("Invalid data chunk reference");
        }
        this.chunkRef = chunkRef;
        this.rowCount = duckdb_vector_size();
        this.columnCount = duckdb_data_chunk_get_column_count(chunkRef);
        this.vectors = new DuckDBWritableVector[Math.toIntExact(columnCount)];

        for (long columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            ByteBuffer vectorRef = duckdb_data_chunk_get_vector(chunkRef, columnIndex);
            int arrayIndex = Math.toIntExact(columnIndex);
            vectors[arrayIndex] = new DuckDBWritableVector(vectorRef, rowCount);
        }
    }

    public long capacity() {
        return rowCount;
    }

    void setSize(long size) {
        duckdb_data_chunk_set_size(chunkRef, size);
    }

    public long columnCount() {
        return columnCount;
    }

    public DuckDBWritableVector vector(long columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
        }
        int arrayIndex = Math.toIntExact(columnIndex);
        return vectors[arrayIndex];
    }
}

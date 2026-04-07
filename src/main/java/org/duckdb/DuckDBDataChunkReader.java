package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public final class DuckDBDataChunkReader {
    private final ByteBuffer chunkRef;
    private final long rowCount;
    private final long columnCount;
    private final DuckDBReadableVector[] vectors;

    DuckDBDataChunkReader(ByteBuffer chunkRef) throws SQLException {
        if (chunkRef == null) {
            throw new SQLException("Invalid data chunk reference");
        }
        this.chunkRef = chunkRef;
        this.rowCount = duckdb_data_chunk_get_size(chunkRef);
        this.columnCount = duckdb_data_chunk_get_column_count(chunkRef);
        this.vectors = new DuckDBReadableVector[Math.toIntExact(columnCount)];
    }

    public long rowCount() {
        return rowCount;
    }

    public long columnCount() {
        return columnCount;
    }

    public DuckDBReadableVector vector(long columnIndex) throws SQLException {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
        }
        int arrayIndex = Math.toIntExact(columnIndex);
        DuckDBReadableVector vector = vectors[arrayIndex];
        if (vector == null) {
            ByteBuffer vectorRef = duckdb_data_chunk_get_vector(chunkRef, columnIndex);
            vector = new DuckDBReadableVectorImpl(vectorRef, rowCount);
            vectors[arrayIndex] = vector;
        }
        return vector;
    }
}

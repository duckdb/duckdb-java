package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public final class DuckDBDataChunkReader {
    private final ByteBuffer chunkRef;
    private final int rowCount;
    private final int columnCount;
    private final DuckDBReadableVector[] vectors;

    DuckDBDataChunkReader(ByteBuffer chunkRef) throws SQLException {
        if (chunkRef == null) {
            throw new SQLException("Invalid data chunk reference");
        }
        this.chunkRef = chunkRef;
        this.rowCount = (int) duckdb_data_chunk_get_size(chunkRef);
        this.columnCount = (int) duckdb_data_chunk_get_column_count(chunkRef);
        this.vectors = new DuckDBReadableVector[columnCount];
    }

    public int rowCount() {
        return rowCount;
    }

    public int columnCount() {
        return columnCount;
    }

    public DuckDBReadableVector vector(int columnIndex) throws SQLException {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
        }
        DuckDBReadableVector vector = vectors[columnIndex];
        if (vector == null) {
            ByteBuffer vectorRef = duckdb_data_chunk_get_vector(chunkRef, columnIndex);
            vector = new DuckDBReadableVector(vectorRef, rowCount);
            vectors[columnIndex] = vector;
        }
        return vector;
    }
}

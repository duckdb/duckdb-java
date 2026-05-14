package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.LongStream;
import org.duckdb.DuckDBFunctions.FunctionException;

/**
 * Reader over callback input data chunks.
 *
 * <p>Column index violations throw {@link IndexOutOfBoundsException}.
 */
public final class DuckDBDataChunkReader {
    private ByteBuffer chunkRef;
    final ReentrantLock chunkRefLock = new ReentrantLock();
    private final long rowCount;
    private final long columnCount;
    private final DuckDBReadableVector[] vectors;

    DuckDBDataChunkReader(ByteBuffer chunkRef) {
        if (chunkRef == null) {
            throw new FunctionException("Invalid data chunk reference");
        }
        this.chunkRef = chunkRef;
        this.rowCount = duckdb_data_chunk_get_size(chunkRef);
        this.columnCount = duckdb_data_chunk_get_column_count(chunkRef);
        this.vectors = new DuckDBReadableVector[Math.toIntExact(columnCount)];

        for (long columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            ByteBuffer vectorRef = duckdb_data_chunk_get_vector(chunkRef, columnIndex);
            int arrayIndex = Math.toIntExact(columnIndex);
            vectors[arrayIndex] = new DuckDBReadableVector(vectorRef, this, rowCount);
        }
    }

    public long rowCount() {
        return rowCount;
    }

    public long columnCount() {
        return columnCount;
    }

    public LongStream stream() {
        return LongStream.range(0, rowCount);
    }

    public DuckDBReadableVector vector(long columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
        }
        int arrayIndex = Math.toIntExact(columnIndex);
        return vectors[arrayIndex];
    }

    void close() {
        closeInternal(false);
    }

    void closeAndDestroy() {
        closeInternal(true);
    }

    private void closeInternal(boolean destroy) {
        if (isClosed()) {
            return;
        }
        chunkRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }
            if (destroy) {
                duckdb_destroy_data_chunk(chunkRef);
            }
            chunkRef = null;
        } finally {
            chunkRefLock.unlock();
        }
    }

    boolean isClosed() {
        return chunkRef == null;
    }

    void checkOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Chunk was closed");
        }
    }
}

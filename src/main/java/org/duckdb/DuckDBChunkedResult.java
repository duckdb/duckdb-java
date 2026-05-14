package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DuckDBChunkedResult implements AutoCloseable {
    private final DuckDBPreparedStatement stmt;
    private ByteBuffer resultRef;
    private final Lock resultRefLock = new ReentrantLock();
    private DuckDBDataChunkReader currentChunk = null;

    public DuckDBChunkedResult(DuckDBPreparedStatement stmt, ByteBuffer resultRef) {
        this.stmt = stmt;
        this.resultRef = resultRef;
    }

    public boolean nextChunk() {
        checkOpen();
        resultRefLock.lock();
        try {
            checkOpen();
            clearCurrentChunk();
            ByteBuffer chunkRef = duckdb_fetch_chunk(resultRef);
            if (chunkRef == null) {
                return false;
            }
            currentChunk = new DuckDBDataChunkReader(chunkRef);
            return true;
        } finally {
            resultRefLock.unlock();
        }
    }

    public DuckDBDataChunkReader chunk() {
        return currentChunk;
    }

    public String columnName(long columnIndex) {
        checkOpen();
        resultRefLock.lock();
        try {
            checkOpen();
            byte[] bytes = duckdb_column_name(resultRef, columnIndex);
            if (null == bytes) {
                return null;
            }
            return new String(bytes, UTF_8);
        } finally {
            resultRefLock.unlock();
        }
    }

    public int columnTypeId(long columnIndex) {
        checkOpen();
        resultRefLock.lock();
        try {
            checkOpen();
            return duckdb_column_type(resultRef, columnIndex);
        } finally {
            resultRefLock.unlock();
        }
    }

    public long columnCount() {
        checkOpen();
        resultRefLock.lock();
        try {
            checkOpen();
            return duckdb_column_count(resultRef);
        } finally {
            resultRefLock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        resultRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }
            clearCurrentChunk();
            duckdb_destroy_result(resultRef);
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

    public boolean isClosed() {
        return resultRef == null;
    }

    void checkError() throws SQLException {
        resultRefLock.lock();
        try {
            byte[] error = duckdb_result_error(resultRef);
            if (error != null) {
                String errorStr = new String(error, UTF_8);
                throw new SQLException("Query failed: " + errorStr);
            }
        } finally {
            resultRefLock.unlock();
        }
    }

    private void checkOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Result was closed");
        }
    }

    private void clearCurrentChunk() {
        if (currentChunk != null) {
            currentChunk.closeAndDestroy();
            currentChunk = null;
        }
    }
}

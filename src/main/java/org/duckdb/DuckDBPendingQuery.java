package org.duckdb;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

class DuckDBPendingQuery {
    private DuckDBConnection conn;
    ByteBuffer pendingRef = null;
    final ReentrantLock pendingRefLock = new ReentrantLock();

    DuckDBPendingQuery(DuckDBConnection conn, ByteBuffer pendingRef) {
        this.conn = conn;
        this.pendingRef = pendingRef;
        this.conn.connRefLock.lock();
        try {
            this.conn.pendingQueries.add(this);
        } finally {
            this.conn.connRefLock.unlock();
        }
    }

    void close() throws SQLException {
        if (pendingRef == null) {
            return;
        }
        pendingRefLock.lock();
        try {
            if (pendingRef == null) {
                return;
            }
            DuckDBNative.duckdb_jdbc_release_pending(pendingRef);
            pendingRef = null;
        } finally {
            pendingRefLock.unlock();
        }

        // Untrack pending query from parent connection,
        // if 'closing' flag is set it means that the parent connection itself
        // is being closed and we don't need to untrack this instance
        if (!conn.closing) {
            conn.connRefLock.lock();
            try {
                conn.pendingQueries.remove(this);
            } finally {
                conn.connRefLock.unlock();
            }
        }
    }
}

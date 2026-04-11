package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;

public final class DuckDBTableFunctionInitInfo {
    private final ByteBuffer initInfoRef;

    public DuckDBTableFunctionInitInfo(ByteBuffer initInfoRef) {
        this.initInfoRef = initInfoRef;
    }

    public void setMaxThreads(long maxThreads) {
        duckdb_init_set_max_threads(initInfoRef, maxThreads);
    }

    @SuppressWarnings("unchecked")
    public <T> T getBindData() {
        return (T) duckdb_init_get_bind_data(initInfoRef);
    }

    public long getColumnCount() {
        return duckdb_init_get_column_count(initInfoRef);
    }

    public long getColumnIndex(long columnIndex) {
        return duckdb_init_get_column_index(initInfoRef, columnIndex);
    }
}

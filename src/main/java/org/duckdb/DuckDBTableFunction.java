package org.duckdb;

public interface DuckDBTableFunction<B, G, L> {

    B bind(DuckDBTableFunctionBindInfo info) throws Exception;

    G init(DuckDBTableFunctionInitInfo info) throws Exception;

    default L localInit(DuckDBTableFunctionInitInfo info) throws Exception {
        return null;
    }

    long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception;
}

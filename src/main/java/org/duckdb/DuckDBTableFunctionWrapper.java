package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.JdbcUtils.collectStackTrace;

import java.nio.ByteBuffer;

class DuckDBTableFunctionWrapper {
    private final DuckDBTableFunction<?, ?, ?> function;

    DuckDBTableFunctionWrapper(DuckDBTableFunction<?, ?, ?> function) {
        this.function = function;
    }

    public void executeBind(ByteBuffer bindInfoRef) {
        try {
            DuckDBTableFunctionBindInfo bindInfo = new DuckDBTableFunctionBindInfo(bindInfoRef);
            try {
                Object bindData = function.bind(bindInfo);
                if (null != bindData) {
                    duckdb_bind_set_bind_data(bindInfoRef, bindData);
                }
            } finally {
                bindInfo.clearAccessedParameters();
            }
        } catch (Throwable throwable) {
            String trace = collectStackTrace(throwable);
            duckdb_bind_set_error(bindInfoRef, trace.getBytes(UTF_8));
        }
    }

    public void executeGlobalInit(ByteBuffer initInfoRef) {
        try {
            DuckDBTableFunctionInitInfo initInfo = new DuckDBTableFunctionInitInfo(initInfoRef);
            Object globalData = function.init(initInfo);
            if (null != globalData) {
                duckdb_init_set_init_data(initInfoRef, globalData);
            }
        } catch (Throwable throwable) {
            String trace = collectStackTrace(throwable);
            duckdb_init_set_error(initInfoRef, trace.getBytes(UTF_8));
        }
    }

    public void executeLocalInit(ByteBuffer initInfoRef) {
        try {
            DuckDBTableFunctionInitInfo initInfo = new DuckDBTableFunctionInitInfo(initInfoRef);
            Object localData = function.localInit(initInfo);
            if (null != localData) {
                duckdb_init_set_init_data(initInfoRef, localData);
            }
        } catch (Throwable throwable) {
            String trace = collectStackTrace(throwable);
            duckdb_init_set_error(initInfoRef, trace.getBytes(UTF_8));
        }
    }

    public void executeFunction(ByteBuffer tableFunctionInfoRef, ByteBuffer outputChunkRef) {
        try {
            Object bindData = DuckDBBindings.duckdb_function_get_bind_data(tableFunctionInfoRef);
            Object globalData = DuckDBBindings.duckdb_function_get_init_data(tableFunctionInfoRef);
            Object localData = DuckDBBindings.duckdb_function_get_local_init_data(tableFunctionInfoRef);
            DuckDBTableFunctionCallInfo info = new DuckDBTableFunctionCallInfo(bindData, globalData, localData);
            DuckDBDataChunkWriter writer = new DuckDBDataChunkWriter(outputChunkRef);
            long recordsWritten = function.apply(info, writer);
            writer.setSize(recordsWritten);
        } catch (Throwable throwable) {
            String trace = collectStackTrace(throwable);
            duckdb_function_set_error(tableFunctionInfoRef, trace.getBytes(UTF_8));
        }
    }
}

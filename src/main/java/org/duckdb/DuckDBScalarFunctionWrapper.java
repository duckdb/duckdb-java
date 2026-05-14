package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.duckdb_scalar_function_set_error;
import static org.duckdb.JdbcUtils.collectStackTrace;

import java.nio.ByteBuffer;

class DuckDBScalarFunctionWrapper {
    private final DuckDBScalarFunction function;

    DuckDBScalarFunctionWrapper(DuckDBScalarFunction function) {
        this.function = function;
    }

    public void execute(ByteBuffer functionInfo, ByteBuffer inputChunk, ByteBuffer outputVector) {
        DuckDBDataChunkReader inputReader = null;
        DuckDBWritableVector outputWriter = null;
        try {
            inputReader = new DuckDBDataChunkReader(inputChunk);
            outputWriter = new DuckDBWritableVector(outputVector, inputReader.rowCount());
            function.apply(inputReader, outputWriter);
        } catch (Throwable throwable) {
            String trace = collectStackTrace(throwable);
            duckdb_scalar_function_set_error(functionInfo, trace.getBytes(UTF_8));
        } finally {
            if (null != inputReader) {
                inputReader.close();
            }
            if (null != outputWriter) {
                outputWriter.close();
            }
        }
    }
}

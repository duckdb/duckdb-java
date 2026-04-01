package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

final class DuckDBScalarFunctionWrapper {
    private final DuckDBVectorizedScalarFunction function;

    DuckDBScalarFunctionWrapper(DuckDBVectorizedScalarFunction function) {
        this.function = function;
    }

    public void execute(ByteBuffer functionInfo, ByteBuffer inputChunk, int rowCount, ByteBuffer outputVector) {
        try {
            DuckDBDataChunkReader inputReader = new DuckDBDataChunkReader(inputChunk, rowCount);
            DuckDBWritableVector outputWriter = new DuckDBWritableVector(outputVector, rowCount);
            function.apply(inputReader, rowCount, outputWriter);
        } catch (Throwable throwable) {
            reportError(functionInfo, throwable);
        }
    }

    private static void reportError(ByteBuffer functionInfo, Throwable throwable) {
        String message = throwable.getMessage();
        String className = throwable.getClass().getName();
        String formatted =
            message == null || message.isEmpty() ? className : String.format("%s: %s", className, message);
        String error = "Java scalar function threw exception: " + formatted;
        DuckDBBindings.duckdb_scalar_function_set_error(functionInfo, error.getBytes(UTF_8));
    }
}

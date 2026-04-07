package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

final class DuckDBScalarFunctionWrapper {
    private final DuckDBScalarFunction function;
    private final boolean propagateNulls;

    DuckDBScalarFunctionWrapper(DuckDBScalarFunction function, boolean propagateNulls) {
        this.function = function;
        this.propagateNulls = propagateNulls;
    }

    public void execute(ByteBuffer functionInfo, ByteBuffer inputChunk, ByteBuffer outputVector) {
        try {
            DuckDBDataChunkReader inputReader = new DuckDBDataChunkReader(inputChunk);
            DuckDBWritableVector outputWriter = new DuckDBWritableVectorImpl(outputVector, inputReader.rowCount());
            DuckDBScalarContext context = new DuckDBScalarContext(inputReader, outputWriter, propagateNulls);
            function.apply(context);
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

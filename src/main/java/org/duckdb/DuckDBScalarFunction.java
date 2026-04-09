package org.duckdb;

@FunctionalInterface
public interface DuckDBScalarFunction {
    /**
     * Processes a full input chunk and writes one output value per row directly into the DuckDB output vector.
     *
     * <p>The context and all wrappers returned from it are valid only for the duration of the callback and must not
     * be retained.
     *
     * @param input data chunk with function arguments
     * @param output vector to write function results into
     * @throws Exception when function execution fails
     */
    void apply(DuckDBDataChunkReader input, DuckDBWritableVector output) throws Exception;
}

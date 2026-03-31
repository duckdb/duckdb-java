package org.duckdb;

@FunctionalInterface
public interface DuckDBVectorizedScalarFunction {
    /**
     * Processes a full input chunk and writes one output value per row directly into the DuckDB output vector.
     *
     * <p>The input and output wrappers are valid only for the duration of the callback and must not be retained.
     *
     * @param input input vectors for the current chunk
     * @param rowCount number of rows in the current chunk
     * @param out output vector for the current chunk
     * @throws Exception when function execution fails
     */
    void apply(DuckDBDataChunkReader input, int rowCount, DuckDBWritableVector out) throws Exception;
}

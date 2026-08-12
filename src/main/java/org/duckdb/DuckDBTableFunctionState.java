package org.duckdb;

/**
 * Opt-in lifecycle contract for state returned by a {@link DuckDBTableFunction}.
 *
 * <p>DuckDB takes ownership of an object implementing this interface after the
 * object has been successfully installed as bind, global-init, or local-init
 * data. DuckDB then makes a best-effort call to {@link #close()} when the
 * corresponding native state is destroyed. A {@code null} state and an object
 * that does not implement this interface are not closed by this lifecycle.
 * Implementations must not rely on garbage collection for releasing their
 * resources.</p>
 *
 * <p>Teardown may run on a native execution or worker thread rather than the
 * thread that created the state. Local states can be created and closed
 * concurrently with one another. There is no global ordering guarantee among
 * bind, global-init, and local-init states, or among local states.</p>
 *
 * <p>A holder is closed at most once, but the same Java object may be installed
 * in multiple holders and consequently have {@code close()} called once per
 * holder. Implementations should therefore be idempotent and thread-safe when
 * they manage shared resources or may be aliased. {@code close()} must not
 * re-enter the same connection or execute SQL during teardown.</p>
 *
 * <p>Exceptions and other {@link Throwable}s from teardown are handled on a
 * best-effort basis and are not propagated to the query caller. Implementations
 * that need diagnostics must record them themselves. A close failure must not
 * prevent other state cleanup or release of the native/JNI reference.</p>
 */
@SuppressWarnings("try")
public interface DuckDBTableFunctionState extends AutoCloseable {
    /**
     * Releases resources owned by this table-function state.
     *
     * <p>This method may be called from a different thread, and its failure is
     * not reported through the query.</p>
     *
     * @throws Exception if releasing the resource fails; the exception is
     *                   handled as best effort by the table-function runtime
     */
    @Override void close() throws Exception;
}

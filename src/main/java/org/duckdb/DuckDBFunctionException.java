package org.duckdb;

public final class DuckDBFunctionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DuckDBFunctionException(String message) {
        super(message);
    }

    public DuckDBFunctionException(String message, Throwable cause) {
        super(message, cause);
    }
}

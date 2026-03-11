package org.duckdb.udf;

public final class TableFunctionOptions {
    public boolean threadSafe = false;
    public int maxThreads = 1;

    public TableFunctionOptions() {
    }

    public TableFunctionOptions threadSafe(boolean value) {
        this.threadSafe = value;
        return this;
    }

    public TableFunctionOptions maxThreads(int value) {
        if (value < 1) {
            throw new IllegalArgumentException("maxThreads must be >= 1");
        }
        this.maxThreads = value;
        return this;
    }
}

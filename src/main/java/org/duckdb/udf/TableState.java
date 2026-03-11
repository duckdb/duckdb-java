package org.duckdb.udf;

public final class TableState {
    private final Object state;

    public TableState(Object state) {
        this.state = state;
    }

    public Object getState() {
        return state;
    }
}

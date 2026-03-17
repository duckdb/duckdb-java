package org.duckdb.udf;

import java.util.Arrays;
import java.util.Objects;
import org.duckdb.DuckDBColumnType;

public final class TableBindResult {
    private final String[] columnNames;
    private final DuckDBColumnType[] columnTypes;
    private final UdfLogicalType[] columnLogicalTypes;
    private final Object bindState;

    public TableBindResult(String[] columnNames, DuckDBColumnType[] columnTypes) {
        this(columnNames, columnTypes, null);
    }

    public TableBindResult(String[] columnNames, DuckDBColumnType[] columnTypes, Object bindState) {
        this.columnNames = Objects.requireNonNull(columnNames, "columnNames").clone();
        this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes").clone();
        if (this.columnNames.length != this.columnTypes.length) {
            throw new IllegalArgumentException("columnNames and columnTypes must have same length");
        }
        this.columnLogicalTypes = null;
        this.bindState = bindState;
    }

    public TableBindResult(String[] columnNames, UdfLogicalType[] columnLogicalTypes) {
        this(columnNames, columnLogicalTypes, null);
    }

    public TableBindResult(String[] columnNames, UdfLogicalType[] columnLogicalTypes, Object bindState) {
        this.columnNames = Objects.requireNonNull(columnNames, "columnNames").clone();
        this.columnLogicalTypes = Objects.requireNonNull(columnLogicalTypes, "columnLogicalTypes").clone();
        if (this.columnNames.length != this.columnLogicalTypes.length) {
            throw new IllegalArgumentException("columnNames and columnLogicalTypes must have same length");
        }
        this.columnTypes = new DuckDBColumnType[this.columnLogicalTypes.length];
        for (int i = 0; i < this.columnLogicalTypes.length; i++) {
            if (this.columnLogicalTypes[i] == null) {
                throw new IllegalArgumentException("columnLogicalTypes[" + i + "] must not be null");
            }
            this.columnTypes[i] = this.columnLogicalTypes[i].getType();
        }
        this.bindState = bindState;
    }

    public String[] getColumnNames() {
        return columnNames.clone();
    }

    public DuckDBColumnType[] getColumnTypes() {
        return columnTypes.clone();
    }

    public UdfLogicalType[] getColumnLogicalTypes() {
        return columnLogicalTypes == null ? null : columnLogicalTypes.clone();
    }

    public Object getBindState() {
        return bindState;
    }

    @Override
    public String toString() {
        return "TableBindResult{"
            + "columnNames=" + Arrays.toString(columnNames) + ", columnTypes=" + Arrays.toString(columnTypes) +
            ", columnLogicalTypes=" + Arrays.toString(columnLogicalTypes) + "}";
    }
}

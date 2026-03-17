package org.duckdb.udf;

import java.util.Objects;

public final class TableInitContext implements InitContext {
    private final int[] columnIndexes;

    public TableInitContext(int[] columnIndexes) {
        this.columnIndexes = Objects.requireNonNull(columnIndexes, "columnIndexes").clone();
    }

    @Override
    public int getColumnCount() {
        return columnIndexes.length;
    }

    @Override
    public int getColumnIndex(int projectedColumnIndex) {
        return columnIndexes[projectedColumnIndex];
    }
}

package org.duckdb;

import java.util.Objects;
import java.util.StringJoiner;

public class QueryProgress {
    private final double percentage;
    private final long rowsProcessed;
    private final long totalRowsToProcess;

    QueryProgress(double percentage, long rowsProcessed, long totalRowsToProcess) {
        this.percentage = percentage;
        this.rowsProcessed = rowsProcessed;
        this.totalRowsToProcess = totalRowsToProcess;
    }

    public double getPercentage() {
        return percentage;
    }

    public long getRowsProcessed() {
        return rowsProcessed;
    }

    public long getTotalRowsToProcess() {
        return totalRowsToProcess;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryProgress.class.getSimpleName() + "[", "]")
            .add("percentage=" + percentage)
            .add("rowsProcessed=" + rowsProcessed)
            .add("totalRowsToProcess=" + totalRowsToProcess)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryProgress that = (QueryProgress) o;
        return Double.compare(that.percentage, percentage) == 0 && rowsProcessed == that.rowsProcessed &&
            totalRowsToProcess == that.totalRowsToProcess;
    }

    @Override
    public int hashCode() {
        return Objects.hash(percentage, rowsProcessed, totalRowsToProcess);
    }
}

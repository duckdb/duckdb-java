package org.duckdb;

import java.sql.SQLException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class DuckDBScalarContext {
    private final DuckDBDataChunkReader input;
    private final DuckDBWritableVector output;
    private final boolean propagateNulls;

    DuckDBScalarContext(DuckDBDataChunkReader input, DuckDBWritableVector output, boolean propagateNulls) {
        if (input == null) {
            throw new IllegalArgumentException("Input chunk cannot be null");
        }
        if (output == null) {
            throw new IllegalArgumentException("Output vector cannot be null");
        }
        this.input = input;
        this.output = output;
        this.propagateNulls = propagateNulls;
    }

    public long rowCount() {
        return input.rowCount();
    }

    public long columnCount() {
        return input.columnCount();
    }

    public DuckDBReadableVector input(long columnIndex) throws SQLException {
        return input.vector(columnIndex);
    }

    public DuckDBWritableVector output() {
        return output;
    }

    public boolean propagateNulls() {
        return propagateNulls;
    }

    DuckDBDataChunkReader inputChunk() {
        return input;
    }

    public Stream<DuckDBScalarRow> stream() {
        LongStream rows = LongStream.range(0, rowCount());
        if (propagateNulls) {
            rows = rows.filter(this::rowHasNoNullInputs);
        }
        return rows.mapToObj(this::row);
    }

    public DuckDBScalarRow row(long rowIndex) {
        checkRowIndex(rowIndex);
        return new DuckDBScalarRow(this, rowIndex);
    }

    DuckDBReadableVector inputUnchecked(long columnIndex) {
        try {
            return input(columnIndex);
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to access input column " + columnIndex, exception);
        }
    }

    private void checkRowIndex(long rowIndex) {
        if (rowIndex < 0 || rowIndex >= rowCount()) {
            throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
        }
    }

    private boolean rowHasNoNullInputs(long rowIndex) {
        for (long columnIndex = 0; columnIndex < columnCount(); columnIndex++) {
            if (inputUnchecked(columnIndex).isNull(rowIndex)) {
                try {
                    output.setNull(rowIndex);
                } catch (SQLException exception) {
                    throw new IllegalStateException("Failed to write NULL to output row " + rowIndex, exception);
                }
                return false;
            }
        }
        return true;
    }
}

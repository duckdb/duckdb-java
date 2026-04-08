package org.duckdb;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Per-invocation scalar callback context.
 *
 * <p>Runtime type/value failures are surfaced as {@link DuckDBFunctionException}. Invalid row or
 * column indexes throw {@link IndexOutOfBoundsException}.
 */
public final class DuckDBScalarContext {
    private final DuckDBDataChunkReader input;
    private final DuckDBWritableVector output;
    private boolean propagateNulls;

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

    public DuckDBReadableVector input(long columnIndex) {
        return input.vector(columnIndex);
    }

    public DuckDBWritableVector output() {
        return output;
    }

    public boolean nullsPropagated() {
        return propagateNulls;
    }

    public DuckDBScalarContext propagateNulls(boolean propagateNulls) {
        this.propagateNulls = propagateNulls;
        return this;
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
        return input(columnIndex);
    }

    private void checkRowIndex(long rowIndex) {
        if (rowIndex < 0 || rowIndex >= rowCount()) {
            throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
        }
    }

    private boolean rowHasNoNullInputs(long rowIndex) {
        for (long columnIndex = 0; columnIndex < columnCount(); columnIndex++) {
            if (inputUnchecked(columnIndex).isNull(rowIndex)) {
                output.setNull(rowIndex);
                return false;
            }
        }
        return true;
    }
}

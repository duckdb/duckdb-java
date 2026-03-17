package org.duckdb.udf;

public interface InitContext {
    // The projection list is ordered to match the output columns passed to produce().
    int getColumnCount();

    int getColumnIndex(int idx);
}

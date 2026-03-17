package org.duckdb.udf;

import org.duckdb.UdfReader;
import org.duckdb.UdfScalarWriter;

@FunctionalInterface
public interface ScalarUdf {
    void apply(UdfContext ctx, UdfReader[] args, UdfScalarWriter out, int rowCount) throws Exception;
}

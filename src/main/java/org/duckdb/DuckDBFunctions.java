package org.duckdb;

import java.sql.SQLException;

public final class DuckDBFunctions {
    public enum DuckDBFunctionKind { SCALAR }

    private DuckDBFunctions() {
    }

    public static DuckDBScalarFunctionBuilder scalarFunction() throws SQLException {
        return new DuckDBScalarFunctionBuilder();
    }
}

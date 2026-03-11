package org.duckdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public final class UdfNative {
    private UdfNative() {
    }

    public static String getVarchar(ByteBuffer vectorRef, int row) throws SQLException {
        byte[] bytes = DuckDBBindings.duckdb_udf_get_varchar_bytes(vectorRef, row);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void setVarchar(ByteBuffer vectorRef, int row, String value) throws SQLException {
        DuckDBBindings.duckdb_udf_set_varchar_bytes(vectorRef, row, value.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] getBlob(ByteBuffer vectorRef, int row) throws SQLException {
        return DuckDBBindings.duckdb_udf_get_blob_bytes(vectorRef, row);
    }

    public static void setBlob(ByteBuffer vectorRef, int row, byte[] value) throws SQLException {
        DuckDBBindings.duckdb_udf_set_blob_bytes(vectorRef, row, value);
    }

    public static BigDecimal getDecimal(ByteBuffer vectorRef, int row) throws SQLException {
        return DuckDBBindings.duckdb_udf_get_decimal(vectorRef, row);
    }

    public static void setDecimal(ByteBuffer vectorRef, int row, BigDecimal value) throws SQLException {
        DuckDBBindings.duckdb_udf_set_decimal(vectorRef, row, value);
    }
}

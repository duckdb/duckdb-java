package org.duckdb;

import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public final class DuckDBLogicalType implements AutoCloseable {
    private ByteBuffer logicalTypeRef;

    private DuckDBLogicalType(ByteBuffer logicalTypeRef) throws SQLException {
        if (logicalTypeRef == null) {
            throw new SQLException("Failed to create logical type");
        }
        this.logicalTypeRef = logicalTypeRef;
    }

    public static DuckDBLogicalType of(DuckDBColumnType type) throws SQLException {
        if (type == null) {
            throw new SQLException("Logical type cannot be null");
        }
        switch (type) {
        case BOOLEAN:
            return createPrimitive(DUCKDB_TYPE_BOOLEAN);
        case TINYINT:
            return createPrimitive(DUCKDB_TYPE_TINYINT);
        case SMALLINT:
            return createPrimitive(DUCKDB_TYPE_SMALLINT);
        case INTEGER:
            return createPrimitive(DUCKDB_TYPE_INTEGER);
        case BIGINT:
            return createPrimitive(DUCKDB_TYPE_BIGINT);
        case HUGEINT:
            return createPrimitive(DUCKDB_TYPE_HUGEINT);
        case UTINYINT:
            return createPrimitive(DUCKDB_TYPE_UTINYINT);
        case USMALLINT:
            return createPrimitive(DUCKDB_TYPE_USMALLINT);
        case UINTEGER:
            return createPrimitive(DUCKDB_TYPE_UINTEGER);
        case UBIGINT:
            return createPrimitive(DUCKDB_TYPE_UBIGINT);
        case UHUGEINT:
            return createPrimitive(DUCKDB_TYPE_UHUGEINT);
        case FLOAT:
            return createPrimitive(DUCKDB_TYPE_FLOAT);
        case DOUBLE:
            return createPrimitive(DUCKDB_TYPE_DOUBLE);
        case VARCHAR:
            return createPrimitive(DUCKDB_TYPE_VARCHAR);
        case DATE:
            return createPrimitive(DUCKDB_TYPE_DATE);
        case TIMESTAMP_S:
            return createPrimitive(DUCKDB_TYPE_TIMESTAMP_S);
        case TIMESTAMP_MS:
            return createPrimitive(DUCKDB_TYPE_TIMESTAMP_MS);
        case TIMESTAMP:
            return createPrimitive(DUCKDB_TYPE_TIMESTAMP);
        case TIMESTAMP_NS:
            return createPrimitive(DUCKDB_TYPE_TIMESTAMP_NS);
        case TIMESTAMP_WITH_TIME_ZONE:
            return createPrimitive(DUCKDB_TYPE_TIMESTAMP_TZ);
        default:
            throw new SQLException("Unsupported logical type for scalar UDF registration: " + type);
        }
    }

    public static DuckDBLogicalType decimal(int width, int scale) throws SQLException {
        if (width < 1 || width > 38) {
            throw new SQLException("DECIMAL width must be between 1 and 38, got: " + width);
        }
        if (scale < 0 || scale > width) {
            throw new SQLException("DECIMAL scale must be between 0 and width, got: " + scale);
        }
        return new DuckDBLogicalType(duckdb_create_decimal_type(width, scale));
    }

    ByteBuffer logicalTypeRef() throws SQLException {
        if (logicalTypeRef == null) {
            throw new SQLException("Logical type is already closed");
        }
        return logicalTypeRef;
    }

    @Override
    public void close() {
        if (logicalTypeRef != null) {
            duckdb_destroy_logical_type(logicalTypeRef);
            logicalTypeRef = null;
        }
    }

    private static DuckDBLogicalType createPrimitive(DuckDBBindings.CAPIType type) throws SQLException {
        return new DuckDBLogicalType(duckdb_create_logical_type(type.typeId));
    }
}

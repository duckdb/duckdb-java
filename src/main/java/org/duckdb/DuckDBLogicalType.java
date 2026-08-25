package org.duckdb;

import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.JdbcUtils.createSQLException;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public final class DuckDBLogicalType implements AutoCloseable {
    private ByteBuffer logicalTypeRef;

    private DuckDBLogicalType(ByteBuffer logicalTypeRef) throws SQLException {
        if (logicalTypeRef == null) {
            throw createSQLException("Failed to create logical type", ErrorCode.LOGICAL_TYPE_CREATE, null);
        }
        this.logicalTypeRef = logicalTypeRef;
    }

    public static DuckDBLogicalType of(DuckDBColumnType type) throws SQLException {
        if (type == null) {
            throw createSQLException("Logical type cannot be null", ErrorCode.LOGICAL_TYPE_NULL, null);
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
        case DECIMAL:
            return decimal(38, 18);
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
            throw createSQLException("Unsupported logical type for UDF registration: " + type,
                                     ErrorCode.LOGICAL_TYPE_UDF, null);
        }
    }

    public static DuckDBLogicalType decimal(int width, int scale) throws SQLException {
        if (width < 1 || width > 38) {
            throw createSQLException("DECIMAL width must be between 1 and 38, got: " + width,
                                     ErrorCode.LOGICAL_TYPE_DECIMAL_WIDTH, null);
        }
        if (scale < 0 || scale > width) {
            throw createSQLException("DECIMAL scale must be between 0 and width, got: " + scale,
                                     ErrorCode.LOGICAL_TYPE_DECIMAL_SCALE, null);
        }
        return new DuckDBLogicalType(duckdb_create_decimal_type(width, scale));
    }

    ByteBuffer logicalTypeRef() throws SQLException {
        if (logicalTypeRef == null) {
            throw createSQLException("Logical type is already closed", ErrorCode.LOGICAL_TYPE_CLOSED, null);
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

package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DuckDBLogicalType implements AutoCloseable {
    private static final Pattern DECIMAL_PATTERN =
        Pattern.compile("^(DECIMAL|NUMERIC)\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)$", Pattern.CASE_INSENSITIVE);

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
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_BOOLEAN);
        case TINYINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TINYINT);
        case SMALLINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_SMALLINT);
        case INTEGER:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_INTEGER);
        case BIGINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_BIGINT);
        case UTINYINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_UTINYINT);
        case USMALLINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_USMALLINT);
        case UINTEGER:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_UINTEGER);
        case UBIGINT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_UBIGINT);
        case FLOAT:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_FLOAT);
        case DOUBLE:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_DOUBLE);
        case VARCHAR:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_VARCHAR);
        case DATE:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_DATE);
        case TIMESTAMP_S:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_S);
        case TIMESTAMP_MS:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_MS);
        case TIMESTAMP:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP);
        case TIMESTAMP_NS:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_NS);
        case TIMESTAMP_WITH_TIME_ZONE:
            return createPrimitive(DuckDBBindings.CAPIType.DUCKDB_TYPE_TIMESTAMP_TZ);
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

    public static DuckDBLogicalType parse(String typeName) throws SQLException {
        if (typeName == null) {
            throw new SQLException("Logical type cannot be null");
        }

        String normalized = normalizeTypeName(typeName);
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(normalized);
        if (decimalMatcher.matches()) {
            try {
                int width = Integer.parseInt(decimalMatcher.group(2));
                String scaleText = decimalMatcher.group(3);
                int scale = scaleText == null ? 0 : Integer.parseInt(scaleText);
                return decimal(width, scale);
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid DECIMAL precision/scale: " + typeName, e);
            }
        }

        switch (normalized) {
        case "BOOLEAN":
        case "BOOL":
            return of(DuckDBColumnType.BOOLEAN);
        case "TINYINT":
            return of(DuckDBColumnType.TINYINT);
        case "SMALLINT":
            return of(DuckDBColumnType.SMALLINT);
        case "INTEGER":
        case "INT":
            return of(DuckDBColumnType.INTEGER);
        case "BIGINT":
            return of(DuckDBColumnType.BIGINT);
        case "UTINYINT":
            return of(DuckDBColumnType.UTINYINT);
        case "USMALLINT":
            return of(DuckDBColumnType.USMALLINT);
        case "UINTEGER":
            return of(DuckDBColumnType.UINTEGER);
        case "UBIGINT":
            return of(DuckDBColumnType.UBIGINT);
        case "FLOAT":
        case "REAL":
            return of(DuckDBColumnType.FLOAT);
        case "DOUBLE":
            return of(DuckDBColumnType.DOUBLE);
        case "VARCHAR":
        case "TEXT":
        case "STRING":
            return of(DuckDBColumnType.VARCHAR);
        case "DATE":
            return of(DuckDBColumnType.DATE);
        case "TIMESTAMP":
            return of(DuckDBColumnType.TIMESTAMP);
        case "TIMESTAMP_S":
            return of(DuckDBColumnType.TIMESTAMP_S);
        case "TIMESTAMP_MS":
            return of(DuckDBColumnType.TIMESTAMP_MS);
        case "TIMESTAMP_NS":
            return of(DuckDBColumnType.TIMESTAMP_NS);
        case "TIMESTAMPTZ":
        case "TIMESTAMP WITH TIME ZONE":
            return of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE);
        default:
            throw new SQLException("Unsupported scalar UDF logical type: " + typeName);
        }
    }

    ByteBuffer logicalTypeRef() throws SQLException {
        if (logicalTypeRef == null) {
            throw new SQLException("Logical type is already closed");
        }
        return logicalTypeRef;
    }

    static DuckDBLogicalType fromLogicalTypeRef(ByteBuffer logicalTypeRef) throws SQLException {
        return new DuckDBLogicalType(logicalTypeRef);
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

    private static String normalizeTypeName(String typeName) {
        return typeName.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    }
}

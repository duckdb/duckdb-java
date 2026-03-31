package org.duckdb;

import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;

final class DuckDBVectorTypeInfo {
    final DuckDBColumnType columnType;
    final DuckDBBindings.CAPIType capiType;
    final DuckDBBindings.CAPIType storageType;
    final int widthBytes;
    final DuckDBColumnTypeMetaData decimalMeta;

    private DuckDBVectorTypeInfo(DuckDBColumnType columnType, DuckDBBindings.CAPIType capiType,
                                 DuckDBBindings.CAPIType storageType, int widthBytes,
                                 DuckDBColumnTypeMetaData decimalMeta) {
        this.columnType = columnType;
        this.capiType = capiType;
        this.storageType = storageType;
        this.widthBytes = widthBytes;
        this.decimalMeta = decimalMeta;
    }

    static DuckDBVectorTypeInfo fromVector(ByteBuffer vectorRef) throws SQLException {
        ByteBuffer logicalType = duckdb_vector_get_column_type(vectorRef);
        if (logicalType == null) {
            throw new SQLException("Cannot read vector type");
        }

        try {
            DuckDBBindings.CAPIType capiType =
                DuckDBBindings.CAPIType.capiTypeFromTypeId(duckdb_get_type_id(logicalType));
            switch (capiType) {
            case DUCKDB_TYPE_BOOLEAN:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.BOOLEAN, capiType, capiType, 1, null);
            case DUCKDB_TYPE_TINYINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TINYINT, capiType, capiType, 1, null);
            case DUCKDB_TYPE_UTINYINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.UTINYINT, capiType, capiType, 1, null);
            case DUCKDB_TYPE_SMALLINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.SMALLINT, capiType, capiType, 2, null);
            case DUCKDB_TYPE_USMALLINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.USMALLINT, capiType, capiType, 2, null);
            case DUCKDB_TYPE_INTEGER:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.INTEGER, capiType, capiType, 4, null);
            case DUCKDB_TYPE_UINTEGER:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.UINTEGER, capiType, capiType, 4, null);
            case DUCKDB_TYPE_BIGINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.BIGINT, capiType, capiType, 8, null);
            case DUCKDB_TYPE_UBIGINT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.UBIGINT, capiType, capiType, 8, null);
            case DUCKDB_TYPE_FLOAT:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.FLOAT, capiType, capiType, 4, null);
            case DUCKDB_TYPE_DOUBLE:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.DOUBLE, capiType, capiType, 8, null);
            case DUCKDB_TYPE_DATE:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.DATE, capiType, capiType, 4, null);
            case DUCKDB_TYPE_TIMESTAMP_S:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TIMESTAMP_S, capiType, capiType, 8, null);
            case DUCKDB_TYPE_TIMESTAMP_MS:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TIMESTAMP_MS, capiType, capiType, 8, null);
            case DUCKDB_TYPE_TIMESTAMP:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TIMESTAMP, capiType, capiType, 8, null);
            case DUCKDB_TYPE_TIMESTAMP_NS:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TIMESTAMP_NS, capiType, capiType, 8, null);
            case DUCKDB_TYPE_TIMESTAMP_TZ:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE, capiType, capiType, 8, null);
            case DUCKDB_TYPE_VARCHAR:
                return new DuckDBVectorTypeInfo(DuckDBColumnType.VARCHAR, capiType, capiType, 16, null);
            case DUCKDB_TYPE_DECIMAL: {
                DuckDBBindings.CAPIType internalType =
                    DuckDBBindings.CAPIType.capiTypeFromTypeId(duckdb_decimal_internal_type(logicalType));
                DuckDBColumnTypeMetaData decimalMeta = new DuckDBColumnTypeMetaData(
                    (short) (internalType.widthBytes * 8), (short) duckdb_decimal_width(logicalType),
                    (short) duckdb_decimal_scale(logicalType));
                return new DuckDBVectorTypeInfo(DuckDBColumnType.DECIMAL, capiType, internalType,
                                                (int) internalType.widthBytes, decimalMeta);
            }
            default:
                throw new SQLException("Unsupported vectorized scalar function type: " + capiType);
            }
        } finally {
            duckdb_destroy_logical_type(logicalType);
        }
    }
}

package org.duckdb;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class DuckDBParameterMetaData implements ParameterMetaData {

    private int param_count;
    private DuckDBColumnType[] param_types;
    private DuckDBColumnTypeMetaData[] param_types_meta;
    private String[] param_types_string;

    public DuckDBParameterMetaData(int param_count, String[] param_types_string, DuckDBColumnType[] param_types,
                                   DuckDBColumnTypeMetaData[] param_types_meta) {
        this.param_count = param_count;
        this.param_types_string = param_types_string;
        this.param_types = param_types;
        this.param_types_meta = param_types_meta;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return param_count;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        return DuckDBResultSetMetaData.is_signed(param_types[param - 1]);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        DuckDBColumnTypeMetaData typeMetaData = param_types_meta[param - 1];
        if (typeMetaData == null) {
            return 0;
        }

        return typeMetaData.width;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        DuckDBColumnTypeMetaData typeMetaData = param_types_meta[param - 1];
        if (typeMetaData == null) {
            return 0;
        }

        return typeMetaData.scale;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        return DuckDBResultSetMetaData.type_to_int(param_types[param - 1]);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        return param_types_string[param - 1];
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (param > param_count) {
            throw new SQLException("Parameter index out of bounds");
        }
        return DuckDBResultSetMetaData.type_to_javaString(param_types[param - 1]);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}

package org.duckdb.user;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

public class DuckDBUserArray implements Array {
    private final String typeName;
    private final Object[] elements;

    public DuckDBUserArray(String typeName, Object[] elements) {
        this.typeName = typeName;
        this.elements = elements;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        throw new SQLFeatureNotSupportedException("getBaseType");
    }

    @Override
    public Object getArray() throws SQLException {
        return elements;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet");
    }

    @Override
    public void free() throws SQLException {
        // no-op
    }
}

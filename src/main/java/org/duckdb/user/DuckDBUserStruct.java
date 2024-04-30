package org.duckdb.user;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

public class DuckDBUserStruct implements Struct {
    private final String typeName;
    private final Object[] attributes;

    public DuckDBUserStruct(String typeName, Object[] attributes) {
        this.typeName = typeName;
        this.attributes = attributes;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return getAttributes();
    }
}

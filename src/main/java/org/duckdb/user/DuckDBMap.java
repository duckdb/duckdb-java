package org.duckdb.user;

import java.util.HashMap;
import java.util.Map;

public class DuckDBMap<K, V> extends HashMap<K, V> {
    private final String typeName;

    public DuckDBMap(String typeName, Map<K, V> map) {
        super(map);
        this.typeName = typeName;
    }

    public String getSQLTypeName() {
        return typeName;
    }
}

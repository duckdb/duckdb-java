package org.duckdb.test;

import java.util.LinkedHashMap;

public class Helpers {

    @SuppressWarnings("unchecked")
    public static <K, V> LinkedHashMap<K, V> createMap(Object... entries) {
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((K) entries[i], (V) entries[i + 1]);
        }
        return map;
    }
}

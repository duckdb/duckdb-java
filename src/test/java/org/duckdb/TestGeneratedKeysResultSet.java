package org.duckdb;

import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * Isolated unit tests for the in-memory {@link DuckDBGeneratedKeysResultSet}.
 */
public class TestGeneratedKeysResultSet {

    private static DuckDBResultSetMetaData meta(String... columnNames) {
        String[] columnTypes = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnTypes[i] = "VARCHAR";
        }
        return new DuckDBResultSetMetaData(0, columnNames.length, columnNames, columnTypes, columnTypes, "NOTHING",
                                           new String[0], new String[0]);
    }

    public static void test_generated_keys_resultset_navigation() throws Exception {
        DuckDBGeneratedKeysResultSet rs =
            new DuckDBGeneratedKeysResultSet(meta("id"), new Object[][] {{1L}, {2L}, {3L}});

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isLast());
        assertEquals(rs.getRow(), 1);
        assertEquals(rs.getLong("id"), 1L);

        assertTrue(rs.next());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(rs.getRow(), 2);
        assertEquals(rs.getLong("id"), 2L);

        assertTrue(rs.next());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());
        assertEquals(rs.getRow(), 3);

        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());

        assertTrue(rs.first());
        assertEquals(rs.getLong("id"), 1L);
        assertTrue(rs.last());
        assertEquals(rs.getLong("id"), 3L);
        assertTrue(rs.previous());
        assertEquals(rs.getLong("id"), 2L);

        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 1L);
        rs.close();
    }

    public static void test_generated_keys_resultset_empty() throws Exception {
        DuckDBGeneratedKeysResultSet rs = new DuckDBGeneratedKeysResultSet(meta("id"), new Object[0][]);
        assertFalse(rs.next());
        assertFalse(rs.first());
        assertFalse(rs.last());
        assertTrue(rs.isAfterLast());
        rs.close();
    }

    public static void test_generated_keys_resultset_absolute_relative() throws Exception {
        DuckDBGeneratedKeysResultSet rs =
            new DuckDBGeneratedKeysResultSet(meta("id"), new Object[][] {{10L}, {20L}, {30L}});

        assertTrue(rs.absolute(2));
        assertEquals(rs.getLong(1), 20L);
        assertFalse(rs.absolute(0));
        assertTrue(rs.absolute(-1));
        assertEquals(rs.getLong(1), 30L);
        assertTrue(rs.absolute(-2));
        assertEquals(rs.getLong(1), 20L);
        assertFalse(rs.absolute(100));
        assertTrue(rs.absolute(1));
        assertTrue(rs.relative(1));
        assertEquals(rs.getLong(1), 20L);
        assertTrue(rs.relative(-1));
        assertEquals(rs.getLong(1), 10L);
        rs.close();
    }

    public static void test_generated_keys_resultset_typed_getters() throws Exception {
        Object[][] rows = {{1L, "duck", true, 4.5d, BigDecimal.valueOf(12, 1)},
                           {2L, "goose", false, 9.5d, BigDecimal.valueOf(34, 1)}};
        DuckDBGeneratedKeysResultSet rs =
            new DuckDBGeneratedKeysResultSet(meta("id", "name", "flag", "ratio", "dec"), rows);

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 1L);
        assertEquals(rs.getLong("id"), 1L);
        assertEquals(rs.getString(2), "duck");
        assertEquals(rs.getString("name"), "duck");
        assertEquals(rs.getBoolean(3), true);
        assertEquals(rs.getBoolean("flag"), true);
        assertEquals(rs.getDouble(4), 4.5, 0.0001);
        assertEquals(rs.getDouble("ratio"), 4.5, 0.0001);
        assertEquals(rs.getBigDecimal(5), new BigDecimal("1.2"));
        assertEquals(rs.getBigDecimal(5, 2), new BigDecimal("1.2"));
        assertEquals(rs.getObject(1), 1L);
        assertEquals(rs.getObject("name"), "duck");
        assertEquals(rs.getBytes(2), "duck".getBytes());
        assertEquals(rs.getNString(2), "duck");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 2L);
        assertEquals(rs.getBoolean(3), false);

        assertFalse(rs.next());
        rs.close();
    }

    public static void test_generated_keys_resultset_null_handling() throws Exception {
        DuckDBGeneratedKeysResultSet rs =
            new DuckDBGeneratedKeysResultSet(meta("id", "name"), new Object[][] {new Object[] {null, "duck"}});

        assertTrue(rs.next());
        assertNull(rs.getObject(1));
        assertTrue(rs.wasNull());
        assertNull(rs.getString(1));
        assertEquals(rs.getLong(1), 0L);
        assertEquals(rs.getBoolean(1), false);
        assertEquals(rs.getString(2), "duck");
        assertFalse(rs.wasNull());
        rs.close();
    }

    public static void test_generated_keys_resultset_coercion() throws Exception {
        DuckDBGeneratedKeysResultSet rs = new DuckDBGeneratedKeysResultSet(
            meta("a", "b", "c", "d"), new Object[][] {new Object[] {1.0d, 2.0f, 3, "true"}});

        assertTrue(rs.next());
        // getBoolean from a Number
        assertEquals(rs.getBoolean(1), true);
        // getBoolean from a String
        assertEquals(rs.getBoolean(4), true);
        // getBigDecimal from a Number (via double) produces BigDecimal.valueOf(double)
        assertEquals(rs.getBigDecimal(3), new BigDecimal("3.0"));
        // non-numeric value fails numeric getters
        assertThrows(() -> rs.getLong(4), SQLException.class);
        rs.close();
    }

    public static void test_generated_keys_resultset_metadata_and_findcolumn() throws Exception {
        DuckDBGeneratedKeysResultSet rs =
            new DuckDBGeneratedKeysResultSet(meta("id", "name"), new Object[][] {{1L, "duck"}});

        assertEquals(rs.getMetaData().getColumnCount(), 2);
        assertEquals(rs.getMetaData().getColumnName(1), "id");
        assertEquals(rs.getMetaData().getColumnName(2), "name");
        assertEquals(rs.findColumn("name"), 2);
        assertThrows(() -> rs.findColumn("nope"), SQLException.class);
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 1L);
        rs.close();
    }

    public static void test_generated_keys_resultset_errors() throws Exception {
        DuckDBGeneratedKeysResultSet rs = new DuckDBGeneratedKeysResultSet(meta("id"), new Object[][] {{1L}});

        // getter before next()
        assertThrows(() -> rs.getLong(1), SQLException.class);
        assertThrows(() -> rs.getObject(1), SQLException.class);

        rs.next();
        // out of bounds column index
        assertThrows(() -> rs.getLong(0), SQLException.class);
        assertThrows(() -> rs.getObject(2), SQLException.class);

        rs.close();
        // operations after close
        assertThrows(() -> rs.next(), SQLException.class);
        assertThrows(() -> rs.getMetaData(), SQLException.class);
        assertThrows(() -> rs.getLong(1), SQLException.class);
    }

    public static void test_generated_keys_resultset_unwrap() throws Exception {
        DuckDBGeneratedKeysResultSet rs = new DuckDBGeneratedKeysResultSet(meta("id"), new Object[][] {{1L}});
        assertTrue(rs.isWrapperFor(DuckDBGeneratedKeysResultSet.class));
        assertTrue(rs.isWrapperFor(java.sql.ResultSet.class));
        assertEquals(rs.unwrap(DuckDBGeneratedKeysResultSet.class), rs);
        rs.close();
    }
}
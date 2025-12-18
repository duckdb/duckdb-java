package org.duckdb;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertEquals;

import java.sql.*;
import java.util.*;

public class TestMetadata {

    public static void test_get_table_types_bug1258() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE a1 (i INTEGER)");
                stmt.execute("CREATE TABLE a2 (i INTEGER)");
                stmt.execute("CREATE TEMPORARY TABLE b (i INTEGER)");
                stmt.execute("CREATE VIEW c AS SELECT * FROM a1");
            }
            DatabaseMetaData dm = conn.getMetaData();

            try (ResultSet rs = dm.getTables(null, null, null, null)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a1");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a2");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "b");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "c");
                assertFalse(rs.next());
            }

            try (ResultSet rs = dm.getTables(null, null, null, new String[] {})) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a1");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a2");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "b");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "c");
                assertFalse(rs.next());
            }

            try (ResultSet rs = dm.getTables(null, null, null, new String[] {"BASE TABLE"})) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a1");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a2");
                assertFalse(rs.next());
            }

            try (ResultSet rs = dm.getTables(null, null, null, new String[] {"BASE TABLE", "VIEW"})) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a1");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a2");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "c");
                assertFalse(rs.next());
            }

            try (ResultSet rs = dm.getTables(null, null, null, new String[] {"XXXX"})) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_utf_string_bug1271() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();

             ResultSet rs = stmt.executeQuery("SELECT 'Mühleisen', '🦆', '🦄ྀི123456789'")) {
            assertEquals(rs.getMetaData().getColumnName(1), "'Mühleisen'");
            assertEquals(rs.getMetaData().getColumnName(2), "'🦆'");
            assertEquals(rs.getMetaData().getColumnName(3), "'🦄ྀི123456789'");

            assertTrue(rs.next());

            assertEquals(rs.getString(1), "Mühleisen");
            assertEquals(rs.getString(2), "🦆");
            assertEquals(rs.getString(3), "🦄ྀི123456789");
        }
    }

    public static void test_get_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (ResultSet functions =
                     conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "string_split")) {

                assertTrue(functions.next());
                assertNull(functions.getObject("FUNCTION_CAT"));
                assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
                assertEquals("string_split", functions.getString("FUNCTION_NAME"));
                assertEquals(DatabaseMetaData.functionNoTable, functions.getInt("FUNCTION_TYPE"));

                assertFalse(functions.next());
            }

            // two items for two overloads?
            try (ResultSet functions =
                     conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "read_csv_auto")) {
                assertTrue(functions.next());
                assertNull(functions.getObject("FUNCTION_CAT"));
                assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
                assertEquals("read_csv_auto", functions.getString("FUNCTION_NAME"));
                assertEquals(DatabaseMetaData.functionReturnsTable, functions.getInt("FUNCTION_TYPE"));

                assertTrue(functions.next());
                assertNull(functions.getObject("FUNCTION_CAT"));
                assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
                assertEquals("read_csv_auto", functions.getString("FUNCTION_NAME"));
                assertEquals(DatabaseMetaData.functionReturnsTable, functions.getInt("FUNCTION_TYPE"));

                assertFalse(functions.next());
            }

            try (ResultSet rs = conn.getMetaData().getFunctions(null, null, "read_csv_auto")) {
                assertTrue(rs.next());
            }

            try (ResultSet rs = conn.getMetaData().getFunctions(null, null, "read\\_%")) {
                assertTrue(rs.next());
            }

            try (ResultSet rs = conn.getMetaData().getFunctions("", "", "read_csv_auto")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = conn.getMetaData().getFunctions(null, null, null)) {
                assertTrue(rs.next());
            }
        }
    }

    public static void test_get_primary_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();) {
            Object[][] testData = new Object[12][6];
            int testDataIndex = 0;

            Object[][] params = new Object[6][5];
            int paramIndex = 0;

            String catalog = conn.getCatalog();

            for (int schemaNumber = 1; schemaNumber <= 2; schemaNumber++) {
                String schemaName = "schema" + schemaNumber;
                stmt.executeUpdate("CREATE SCHEMA " + schemaName);
                stmt.executeUpdate("SET SCHEMA = '" + schemaName + "'");
                for (int tableNumber = 1; tableNumber <= 3; tableNumber++) {
                    String tableName = "table" + tableNumber;
                    params[paramIndex] = new Object[] {catalog, schemaName, tableName, testDataIndex, -1};
                    String columns = null;
                    String pk = null;
                    for (int columnNumber = 1; columnNumber <= tableNumber; columnNumber++) {
                        String columnName = "column" + columnNumber;
                        String columnDef = columnName + " int not null";
                        columns = columns == null ? columnDef : columns + "," + columnDef;
                        pk = pk == null ? columnName : pk + "," + columnName;
                        testData[testDataIndex++] =
                            new Object[] {catalog, schemaName, tableName, columnName, columnNumber, null};
                    }
                    stmt.executeUpdate("CREATE TABLE " + tableName + "(" + columns + ",PRIMARY KEY(" + pk + ") )");
                    params[paramIndex][4] = testDataIndex;
                    paramIndex += 1;
                }
            }

            DatabaseMetaData databaseMetaData = conn.getMetaData();
            for (paramIndex = 0; paramIndex < 6; paramIndex++) {
                Object[] paramSet = params[paramIndex];
                ResultSet resultSet =
                    databaseMetaData.getPrimaryKeys((String) paramSet[0], (String) paramSet[1], (String) paramSet[2]);
                for (testDataIndex = (int) paramSet[3]; testDataIndex < (int) paramSet[4]; testDataIndex++) {
                    assertTrue(resultSet.next(), "Expected a row at position " + testDataIndex);
                    Object[] testDataRow = testData[testDataIndex];
                    for (int columnIndex = 0; columnIndex < testDataRow.length; columnIndex++) {
                        Object value = testDataRow[columnIndex];
                        if (value == null || value instanceof String) {
                            String columnValue = resultSet.getString(columnIndex + 1);
                            assertTrue(value == null ? columnValue == null : value.equals(columnValue),
                                       "row value " + testDataIndex + ", " + columnIndex + " " + value +
                                           " should equal column value " + columnValue);
                        } else {
                            int testValue = ((Integer) value).intValue();
                            int columnValue = resultSet.getInt(columnIndex + 1);
                            assertTrue(testValue == columnValue, "row value " + testDataIndex + ", " + columnIndex +
                                                                     " " + testValue + " should equal column value " +
                                                                     columnValue);
                        }
                    }
                }
                resultSet.close();
            }

            try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, "table1")) {
                assertTrue(rs.next());
            }

            try (ResultSet rs = conn.getMetaData().getPrimaryKeys("", "", "table1")) {
                assertFalse(rs.next());
            }
        }
    }
    public static void test_supports_catalogs_in_data_manipulation() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String COLUMN_NAME = "id";
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;

        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection(JDBC_URL);
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));
            statement.execute(String.format("CREATE TABLE %s(%s int)", QUALIFIED_TABLE_NAME, COLUMN_NAME));

            final boolean supportsCatalogsInDataManipulation = databaseMetaData.supportsCatalogsInDataManipulation();
            try {
                statement.execute(String.format("INSERT INTO %s VALUES(1)", QUALIFIED_TABLE_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from " + QUALIFIED_TABLE_NAME + ", got 0");
                assertTrue(resultSet.getInt(COLUMN_NAME) == 1, "Value for " + COLUMN_NAME + " should be 1");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but INSERT in " + QUALIFIED_TABLE_NAME +
                         " is not allowed." + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute(
                    String.format("UPDATE %1$s SET %2$s = 2 WHERE %2$s = 1", QUALIFIED_TABLE_NAME, COLUMN_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from " + QUALIFIED_TABLE_NAME + ", got 0");
                assertTrue(resultSet.getInt(COLUMN_NAME) == 2, "Value for " + COLUMN_NAME + " should be 2");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but UPDATE of " + QUALIFIED_TABLE_NAME +
                         " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute(String.format("DELETE FROM %s WHERE %s = 2", QUALIFIED_TABLE_NAME, COLUMN_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next() == false, "Expected 0 rows from " + QUALIFIED_TABLE_NAME + ", got > 0");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but UPDATE of " + QUALIFIED_TABLE_NAME +
                         " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            assertTrue(supportsCatalogsInDataManipulation, "supportsCatalogsInDataManipulation should return true.");
        }
    }

    public static void test_supports_catalogs_in_index_definitions() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String INDEX_NAME = "idx1";
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;
        final String QUALIFIED_INDEX_NAME = CATALOG_NAME + "." + INDEX_NAME;

        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection(JDBC_URL);
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));

            final boolean supportsCatalogsInIndexDefinitions = databaseMetaData.supportsCatalogsInIndexDefinitions();
            try {
                statement.execute(String.format("CREATE TABLE %s(id int)", QUALIFIED_TABLE_NAME));
                statement.execute(String.format("CREATE INDEX %s ON %s(id)", INDEX_NAME, QUALIFIED_TABLE_NAME));
                resultSet = statement.executeQuery(
                    String.format("SELECT * FROM duckdb_indexes() "
                                      + "WHERE database_name = '%s' AND table_name = '%s' AND index_name = '%s' ",
                                  CATALOG_NAME, TABLE_NAME, INDEX_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from duckdb_indexes(), got 0");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInIndexDefinitions) {
                    fail("supportsCatalogsInIndexDefinitions is true but "
                         + "CREATE INDEX on " + QUALIFIED_TABLE_NAME + " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute("DROP index " + QUALIFIED_INDEX_NAME);
                resultSet = statement.executeQuery(
                    String.format("SELECT * FROM duckdb_indexes() "
                                      + "WHERE database_name = '%s' AND table_name = '%s' AND index_name = '%s'",
                                  CATALOG_NAME, TABLE_NAME, INDEX_NAME));
                assertFalse(resultSet.next());
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInIndexDefinitions) {
                    fail("supportsCatalogsInIndexDefinitions is true but DROP of " + QUALIFIED_INDEX_NAME +
                         " is not allowed." + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            assertTrue(supportsCatalogsInIndexDefinitions, "supportsCatalogsInIndexDefinitions should return true.");
        }
    }

    public static void test_column_metadata() throws Exception {
        Map<String, JDBCType> expectedTypes = new HashMap<>();
        expectedTypes.put("bool", JDBCType.BOOLEAN);
        expectedTypes.put("tinyint", JDBCType.TINYINT);
        expectedTypes.put("utinyint", JDBCType.SMALLINT);
        expectedTypes.put("smallint", JDBCType.SMALLINT);
        expectedTypes.put("usmallint", JDBCType.INTEGER);
        expectedTypes.put("int", JDBCType.INTEGER);
        expectedTypes.put("uint", JDBCType.BIGINT);
        expectedTypes.put("bigint", JDBCType.BIGINT);
        expectedTypes.put("date", JDBCType.DATE);
        expectedTypes.put("time", JDBCType.TIME);
        expectedTypes.put("timestamp", JDBCType.TIMESTAMP);
        expectedTypes.put("time_tz", JDBCType.TIME_WITH_TIMEZONE);
        expectedTypes.put("timestamp_tz", JDBCType.TIMESTAMP_WITH_TIMEZONE);
        expectedTypes.put("float", JDBCType.FLOAT);
        expectedTypes.put("double", JDBCType.DOUBLE);
        expectedTypes.put("varchar", JDBCType.VARCHAR);
        expectedTypes.put("blob", JDBCType.BLOB);
        expectedTypes.put("bit", JDBCType.BIT);
        expectedTypes.put("struct", JDBCType.STRUCT);
        expectedTypes.put("struct_of_arrays", JDBCType.STRUCT);
        expectedTypes.put("struct_of_fixed_array", JDBCType.STRUCT);
        expectedTypes.put("dec_4_1", JDBCType.DECIMAL);
        expectedTypes.put("dec_9_4", JDBCType.DECIMAL);
        expectedTypes.put("dec_18_6", JDBCType.DECIMAL);
        expectedTypes.put("dec38_10", JDBCType.DECIMAL);

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test_all_types_metadata AS SELECT * from test_all_types()");
            }

            try (ResultSet rs = conn.getMetaData().getColumns(null, null, "test_all_types_metadata", null)) {
                while (rs.next()) {
                    String column = rs.getString("COLUMN_NAME");
                    JDBCType expectedType = expectedTypes.get(column);
                    if (expectedType == null) {
                        expectedType = JDBCType.OTHER;
                    }
                    assertEquals(rs.getInt("DATA_TYPE"), expectedType.getVendorTypeNumber(), column);
                }
            }
        }
    }

    public static void test_metadata_get_string_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String rs = conn.getMetaData().getStringFunctions();
            String[] functions = rs.split(",");
            List<String> list = asList(functions);
            assertTrue(list.contains("md5"));
            assertTrue(list.contains("json_keys"));
            assertTrue(list.contains("repeat"));
            assertTrue(list.contains("from_base64"));
        }
    }

    public static void test_metadata_get_system_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String rs = conn.getMetaData().getSystemFunctions();
            String[] functions = rs.split(",");
            List<String> list = asList(functions);
            assertTrue(list.contains("current_date"));
            assertTrue(list.contains("now"));
        }
    }

    public static void test_metadata_get_time_date_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String rs = conn.getMetaData().getTimeDateFunctions();
            String[] functions = rs.split(",");
            List<String> list = asList(functions);
            assertTrue(list.contains("day"));
            assertTrue(list.contains("dayname"));
            assertTrue(list.contains("timezone_hour"));
        }
    }

    public static void test_metadata_get_index_info() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, ok INT)");
                stmt.execute("CREATE INDEX idx_test_ok ON test(ok)");
            }

            try (ResultSet rs = conn.getMetaData().getIndexInfo(null, null, "test", false, false)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "test");
                assertEquals(rs.getString("INDEX_NAME"), "idx_test_ok");
                assertEquals(rs.getBoolean("NON_UNIQUE"), true);
            }
            try (ResultSet rs = conn.getMetaData().getIndexInfo(null, null, "test", true, false)) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = conn.getMetaData().getIndexInfo("", "", "test", false, false)) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = conn.getMetaData().getIndexInfo(null, null, null, false, false)) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_metadata_get_sql_keywords() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String rs = conn.getMetaData().getSQLKeywords();
            String[] keywords = rs.split(",");
            List<String> list = asList(keywords);
            assertTrue(list.contains("select"));
            assertTrue(list.contains("update"));
            assertTrue(list.contains("delete"));
            assertTrue(list.contains("drop"));
        }
    }

    public static void test_metadata_get_numeric_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String rs = conn.getMetaData().getNumericFunctions();
            // print out rs
            String[] functions = rs.split(",");
            List<String> list = asList(functions);
            assertTrue(list.contains("abs"));
            assertTrue(list.contains("ceil"));
            assertTrue(list.contains("floor"));
            assertTrue(list.contains("round"));
        }
    }

    public static void test_wildcard_reflection() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE _a (_i INTEGER, xi INTEGER)");
            stmt.execute("CREATE TABLE xa (i INTEGER)");

            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables(null, null, "\\_a", null)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "_a");
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "\\_a", "\\_i")) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "_a");
                assertEquals(rs.getString("COLUMN_NAME"), "_i");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_schema_reflection() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE a (i INTEGER)");
            stmt.execute("CREATE VIEW b AS SELECT i::STRING AS j FROM a");
            stmt.execute("COMMENT ON TABLE a IS 'a table'");
            stmt.execute("COMMENT ON COLUMN a.i IS 'a column'");
            stmt.execute("COMMENT ON VIEW b IS 'a view'");
            stmt.execute("COMMENT ON COLUMN b.j IS 'a column'");

            DatabaseMetaData md = conn.getMetaData();

            try (ResultSet rs = md.getCatalogs()) {
                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
            }

            try (ResultSet rs = md.getSchemas(null, "ma%")) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertTrue(rs.getObject("TABLE_CATALOG") != null);
                assertEquals(rs.getString(1), DuckDBConnection.DEFAULT_SCHEMA);
            }

            try (ResultSet rs = md.getSchemas(null, null)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertTrue(rs.getObject("TABLE_CATALOG") != null);
                assertEquals(rs.getString(1), DuckDBConnection.DEFAULT_SCHEMA);
            }

            try (ResultSet rs = md.getSchemas("", "")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getSchemas(null, "xxx")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getTables(null, null, "%", null)) {
                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString(3), "a");
                assertEquals(rs.getString("TABLE_TYPE"), "BASE TABLE");
                assertEquals(rs.getString(4), "BASE TABLE");
                assertEquals(rs.getObject("REMARKS"), "a table");
                assertEquals(rs.getObject(5), "a table");
                assertNull(rs.getObject("TYPE_CAT"));
                assertNull(rs.getObject(6));
                assertNull(rs.getObject("TYPE_SCHEM"));
                assertNull(rs.getObject(7));
                assertNull(rs.getObject("TYPE_NAME"));
                assertNull(rs.getObject(8));
                assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
                assertNull(rs.getObject(9));
                assertNull(rs.getObject("REF_GENERATION"));
                assertNull(rs.getObject(10));

                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "b");
                assertEquals(rs.getString(3), "b");
                assertEquals(rs.getString("TABLE_TYPE"), "VIEW");
                assertEquals(rs.getString(4), "VIEW");
                assertEquals(rs.getObject("REMARKS"), "a view");
                assertEquals(rs.getObject(5), "a view");
                assertNull(rs.getObject("TYPE_CAT"));
                assertNull(rs.getObject(6));
                assertNull(rs.getObject("TYPE_SCHEM"));
                assertNull(rs.getObject(7));
                assertNull(rs.getObject("TYPE_NAME"));
                assertNull(rs.getObject(8));
                assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
                assertNull(rs.getObject(9));
                assertNull(rs.getObject("REF_GENERATION"));
                assertNull(rs.getObject(10));

                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getTables(null, DuckDBConnection.DEFAULT_SCHEMA, "a", null)) {

                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString(3), "a");
                assertEquals(rs.getString("TABLE_TYPE"), "BASE TABLE");
                assertEquals(rs.getString(4), "BASE TABLE");
                assertEquals(rs.getObject("REMARKS"), "a table");
                assertEquals(rs.getObject(5), "a table");
                assertNull(rs.getObject("TYPE_CAT"));
                assertNull(rs.getObject(6));
                assertNull(rs.getObject("TYPE_SCHEM"));
                assertNull(rs.getObject(7));
                assertNull(rs.getObject("TYPE_NAME"));
                assertNull(rs.getObject(8));
                assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
                assertNull(rs.getObject(9));
                assertNull(rs.getObject("REF_GENERATION"));
                assertNull(rs.getObject(10));
            }

            try (ResultSet rs = md.getTables(null, DuckDBConnection.DEFAULT_SCHEMA, "xxx", null)) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getTables("", "", "%", null)) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns(null, null, "a", null)) {
                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString(3), "a");
                assertEquals(rs.getString("COLUMN_NAME"), "i");
                assertEquals(rs.getString("REMARKS"), "a column");
                assertEquals(rs.getString(4), "i");
                assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getInt(5), Types.INTEGER);
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(rs.getString(6), "INTEGER");
                assertEquals(rs.getInt("COLUMN_SIZE"), 32); // this should 10 for INTEGER
                assertEquals(rs.getInt(7), 32);
                assertNull(rs.getObject("BUFFER_LENGTH"));
                assertNull(rs.getObject(8));
                // and so on but whatever
            }

            try (ResultSet rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "a", "i")) {
                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString(3), "a");
                assertEquals(rs.getString("COLUMN_NAME"), "i");
                assertEquals(rs.getString(4), "i");
                assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getInt(5), Types.INTEGER);
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(rs.getString(6), "INTEGER");
                assertEquals(rs.getInt("COLUMN_SIZE"), 32);
                assertEquals(rs.getInt(7), 32);
                assertNull(rs.getObject("BUFFER_LENGTH"));
                assertNull(rs.getObject(8));
                assertEquals(rs.getString("REMARKS"), "a column");
            }

            // try with catalog as well
            try (ResultSet rs = md.getColumns(conn.getCatalog(), DuckDBConnection.DEFAULT_SCHEMA, "a", "i")) {
                assertTrue(rs.next());
                assertTrue(rs.getObject("TABLE_CAT") != null);
                assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString(3), "a");
                assertEquals(rs.getString("COLUMN_NAME"), "i");
                assertEquals(rs.getString(4), "i");
                assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getInt(5), Types.INTEGER);
                assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                assertEquals(rs.getString(6), "INTEGER");
                assertEquals(rs.getInt("COLUMN_SIZE"), 32);
                assertEquals(rs.getInt(7), 32);
                assertNull(rs.getObject("BUFFER_LENGTH"));
                assertNull(rs.getObject(8));
            }

            try (ResultSet rs = md.getColumns(null, "xxx", "a", "i")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "xxx", "i")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "a", "xxx")) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getColumns("", "", "%", "%")) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_column_reflection() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE a (a DECIMAL(20,5), b CHAR(10), c VARCHAR(30), d LONG)");

            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, "a", null)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), "a");
                assertEquals(rs.getString("COLUMN_NAME"), "a");
                assertEquals(rs.getInt("DATA_TYPE"), Types.DECIMAL);
                assertEquals(rs.getString("TYPE_NAME"), "DECIMAL(20,5)");
                assertEquals(rs.getString(6), "DECIMAL(20,5)");
                assertEquals(rs.getInt("COLUMN_SIZE"), 20);
                assertEquals(rs.getInt("DECIMAL_DIGITS"), 5);

                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "b");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                assertNull(rs.getObject("COLUMN_SIZE"));
                assertNull(rs.getObject("DECIMAL_DIGITS"));

                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "c");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                assertNull(rs.getObject("COLUMN_SIZE"));
                assertNull(rs.getObject("DECIMAL_DIGITS"));

                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "d");
                assertEquals(rs.getInt("DATA_TYPE"), Types.BIGINT);
                assertEquals(rs.getString("TYPE_NAME"), "BIGINT");
                assertEquals(rs.getInt("COLUMN_SIZE"), 64); // should be 19
                assertEquals(rs.getInt("DECIMAL_DIGITS"), 0);
            }
        }
    }

    public static void test_get_tables_with_current_catalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            final String currentCatalog = conn.getCatalog();
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            Statement statement = conn.createStatement();
            statement.execute("CREATE TABLE T1(ID INT)");
            // verify that the catalog argument is supported and does not throw
            try (ResultSet resultSet = databaseMetaData.getTables(currentCatalog, null, "%", null)) {
                assertTrue(resultSet.next(), "getTables should return exactly 1 table");
                final String returnedCatalog = resultSet.getString("TABLE_CAT");
                assertTrue(currentCatalog.equals(returnedCatalog),
                           String.format("Returned catalog %s should equal current catalog %s", returnedCatalog,
                                         currentCatalog));
                assertFalse(resultSet.next(), "getTables should return exactly 1 table");
            }
        }
    }

    public static void test_get_tables_with_attached_catalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement statement = conn.createStatement()) {
            final String currentCatalog = conn.getCatalog();
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            // create one table in the current catalog
            final String TABLE_NAME1 = "T1";
            statement.execute(String.format("CREATE TABLE %s(ID INT)", TABLE_NAME1));

            // create one table in an attached catalog
            String returnedCatalog, returnedTableName;
            final String ATTACHED_CATALOG = "ATTACHED_CATALOG";
            final String TABLE_NAME2 = "T2";
            statement.execute(String.format("ATTACH '' AS \"%s\"", ATTACHED_CATALOG));
            statement.execute(String.format("CREATE TABLE %s.%s(ID INT)", ATTACHED_CATALOG, TABLE_NAME2));

            // test if getTables can get tables from the remote catalog.
            try (ResultSet resultSet = databaseMetaData.getTables(ATTACHED_CATALOG, null, "%", null)) {
                assertTrue(resultSet.next(), "getTables should return exactly 1 table");
                returnedCatalog = resultSet.getString("TABLE_CAT");
                assertTrue(ATTACHED_CATALOG.equals(returnedCatalog),
                           String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog,
                                         ATTACHED_CATALOG));
                assertFalse(resultSet.next(), "getTables should return exactly 1 table");
            }

            // test if getTables with null catalog returns all tables.
            try (ResultSet resultSet = databaseMetaData.getTables(null, null, "%", null)) {

                assertTrue(resultSet.next(), "getTables should return 2 tables, got 0");
                // first table should be ATTACHED_CATALOG.T2
                returnedCatalog = resultSet.getString("TABLE_CAT");
                assertTrue(ATTACHED_CATALOG.equals(returnedCatalog),
                           String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog,
                                         ATTACHED_CATALOG));
                returnedTableName = resultSet.getString("TABLE_NAME");
                assertTrue(TABLE_NAME2.equals(returnedTableName),
                           String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME2));

                assertTrue(resultSet.next(), "getTables should return 2 tables, got 1");
                // second table should be <current catalog>.T1
                returnedCatalog = resultSet.getString("TABLE_CAT");
                assertTrue(currentCatalog.equals(returnedCatalog),
                           String.format("Returned catalog %s should equal current catalog %s", returnedCatalog,
                                         currentCatalog));
                returnedTableName = resultSet.getString("TABLE_NAME");
                assertTrue(TABLE_NAME1.equals(returnedTableName),
                           String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME1));

                assertFalse(resultSet.next(), "getTables should return 2 tables, got > 2");
            }
        }
    }

    public static void test_get_tables_param_binding_for_table_types() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             ResultSet rs = conn.getMetaData().getTables(null, null, null,
                                                         new String[] {"') UNION ALL "
                                                                       + "SELECT"
                                                                       + " 'fake catalog'"
                                                                       + ", ?"
                                                                       + ", ?"
                                                                       + ", 'fake table type'"
                                                                       + ", 'fake remarks'"
                                                                       + ", 'fake type cat'"
                                                                       + ", 'fake type schem'"
                                                                       + ", 'fake type name'"
                                                                       + ", 'fake self referencing col name'"
                                                                       + ", 'fake ref generation' -- "})) {
            assertFalse(rs.next());
        }
    }

    public static void test_get_table_types() throws Exception {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        List<String> tableTypesList = new ArrayList<>(asList(tableTypesArray));
        tableTypesList.sort(Comparator.naturalOrder());

        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             ResultSet rs = conn.getMetaData().getTableTypes()) {

            for (int i = 0; i < tableTypesArray.length; i++) {
                assertTrue(rs.next(), "Expected a row from table types resultset");
                String tableTypeFromResultSet = rs.getString("TABLE_TYPE");
                String tableTypeFromList = tableTypesList.get(i);
                assertTrue(tableTypeFromList.equals(tableTypeFromResultSet),
                           "Error in tableTypes at row " + (i + 1) + ": "
                               + "value from list " + tableTypeFromList + " should equal "
                               + "value from resultset " + tableTypeFromResultSet);
            }
        }
    }

    public static void test_get_schemas_with_params() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String inputCatalog = conn.getCatalog();
            String inputSchema = conn.getSchema();
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            // catalog equal to current_catalog, schema null
            try (ResultSet resultSet = databaseMetaData.getSchemas(inputCatalog, null)) {
                assertTrue(resultSet.next(), "Expected at least exactly 1 row, got 0");
                do {
                    String outputCatalog = resultSet.getString("TABLE_CATALOG");
                    assertTrue(inputCatalog.equals(outputCatalog),
                               "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                                   inputCatalog);
                } while (resultSet.next());
            }

            // catalog equal to current_catalog, schema '%'
            try (ResultSet resultSet = databaseMetaData.getSchemas(inputCatalog, "%");
                 ResultSet resultSetWithNullSchema = databaseMetaData.getSchemas(inputCatalog, null)) {
                assertTrue(resultSet.next(), "Expected at least exactly 1 row, got 0");
                assertTrue(resultSetWithNullSchema.next(), "Expected at least exactly 1 row, got 0");
                do {
                    String outputCatalog;
                    outputCatalog = resultSet.getString("TABLE_CATALOG");
                    assertTrue(inputCatalog.equals(outputCatalog),
                               "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                                   inputCatalog);
                    outputCatalog = resultSetWithNullSchema.getString("TABLE_CATALOG");
                    assertTrue(inputCatalog.equals(outputCatalog),
                               "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                                   inputCatalog);
                    String schema1 = resultSet.getString("TABLE_SCHEM");
                    String schema2 = resultSetWithNullSchema.getString("TABLE_SCHEM");
                    assertTrue(schema1.equals(schema2), "schema " + schema1 + " from getSchemas with % should equal " +
                                                            schema2 + " from getSchemas with null");
                } while (resultSet.next() && resultSetWithNullSchema.next());
            }

            // empty catalog
            try (ResultSet resultSet = databaseMetaData.getSchemas("", null)) {
                assertFalse(resultSet.next(), "Expected 0 schemas, got > 0");
            }
        }
    }

    public static void test_get_catalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); ResultSet rs = conn.getMetaData().getCatalogs()) {
            HashSet<String> set = new HashSet<String>();
            while (rs.next()) {
                set.add(rs.getString(1));
            }
            assertTrue(!set.isEmpty());
            assertTrue(set.contains(conn.getCatalog()));
        }
    }

    public static void test_supportsLikeEscapeClause_shouldBe_true() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            assertTrue(databaseMetaData.supportsLikeEscapeClause(),
                       "DatabaseMetaData.supportsLikeEscapeClause() should be true.");
        }
    }

    public static void test_supports_catalogs_in_table_definitions() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String IS_TablesQuery = "SELECT * FROM information_schema.tables " +
                                      String.format("WHERE table_catalog = '%s' ", CATALOG_NAME) +
                                      String.format("AND table_name = '%s'", TABLE_NAME);
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;
        try (final Connection connection = DriverManager.getConnection(JDBC_URL);
             final Statement statement = connection.createStatement()) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));

            statement.execute(String.format("CREATE TABLE %s (id int)", QUALIFIED_TABLE_NAME));
            try (ResultSet resultSet = statement.executeQuery(IS_TablesQuery)) {
                assertTrue(resultSet.next(), "Expected exactly 1 row from information_schema.tables, got 0");
                assertFalse(resultSet.next());
            }

            statement.execute(String.format("DROP TABLE %s", QUALIFIED_TABLE_NAME));
            try (ResultSet resultSet = statement.executeQuery(IS_TablesQuery)) {
                assertFalse(resultSet.next(), "Expected exactly 0 rows from information_schema.tables, got > 0");
            }

            assertTrue(databaseMetaData.supportsCatalogsInIndexDefinitions(),
                       "supportsCatalogsInTableDefinitions should return true.");
        }
    }

    private static void createForeignKeysSchema(Statement stmt) throws Exception {
        stmt.execute("CREATE TABLE students(id INTEGER PRIMARY KEY, name VARCHAR)");
        stmt.execute("CREATE TABLE exams(exam_id INTEGER REFERENCES students(id), grade INTEGER)");
    }

    private static void checkForeignColumns(ResultSet rs) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(rsmd.getColumnName(1), "PKTABLE_CAT");
        assertEquals(rsmd.getColumnName(2), "PKTABLE_SCHEM");
        assertEquals(rsmd.getColumnName(3), "PKTABLE_NAME");
        assertEquals(rsmd.getColumnName(4), "PKCOLUMN_NAME");
        assertEquals(rsmd.getColumnName(5), "FKTABLE_CAT");
        assertEquals(rsmd.getColumnName(6), "FKTABLE_SCHEM");
        assertEquals(rsmd.getColumnName(7), "FKTABLE_NAME");
        assertEquals(rsmd.getColumnName(8), "FKCOLUMN_NAME");
        assertEquals(rsmd.getColumnName(9), "KEY_SEQ");
        assertEquals(rsmd.getColumnName(10), "UPDATE_RULE");
        assertEquals(rsmd.getColumnName(11), "DELETE_RULE");
        assertEquals(rsmd.getColumnName(12), "FK_NAME");
        assertEquals(rsmd.getColumnName(13), "PK_NAME");
        assertEquals(rsmd.getColumnName(14), "DEFERRABILITY");
    }

    private static void checkForeignKeys(ResultSet rs) throws Exception {
        checkForeignColumns(rs);
        assertTrue(rs.next());
        assertEquals(rs.getString("PKTABLE_CAT"), "memory");
        assertEquals(rs.getString("PKTABLE_SCHEM"), "main");
        assertEquals(rs.getString("PKTABLE_NAME"), "students");
        assertEquals(rs.getString("PKCOLUMN_NAME"), "id");
        assertEquals(rs.getString("FKTABLE_CAT"), "memory");
        assertEquals(rs.getString("FKTABLE_SCHEM"), "main");
        assertEquals(rs.getString("FKTABLE_NAME"), "exams");
        assertEquals(rs.getInt("KEY_SEQ"), 1);
        assertEquals(rs.getInt("UPDATE_RULE"), 3);
        assertEquals(rs.getInt("DELETE_RULE"), 3);
        assertEquals(rs.getString("FK_NAME"), "exams_exam_id_id_fkey");
        assertEquals(rs.getString("PK_NAME"), "students_id_pkey");
        assertEquals(rs.getInt("DEFERRABILITY"), 7);
        assertFalse(rs.next());
    }

    public static void test_medatada_imported_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            createForeignKeysSchema(stmt);
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getImportedKeys(null, null, "exams")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getImportedKeys("memory", "main", "exams")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getImportedKeys(null, null, null)) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getImportedKeys(null, null, "students")) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_medatada_exported_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            createForeignKeysSchema(stmt);
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getExportedKeys(null, null, "students")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getExportedKeys("memory", "main", "students")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getExportedKeys(null, null, null)) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getExportedKeys(null, null, "exams")) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_medatada_cross_reference() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            createForeignKeysSchema(stmt);
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getCrossReference(null, null, "students", null, null, null)) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getCrossReference("memory", "main", "students", null, null, null)) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getCrossReference(null, null, "exams", null, null, null)) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = dm.getCrossReference(null, null, null, null, null, "exams")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getCrossReference(null, null, null, "memory", "main", "exams")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getCrossReference(null, null, null, null, null, "students")) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = dm.getCrossReference("memory", "main", "students", "memory", "main", "exams")) {
                checkForeignKeys(rs);
            }
            try (ResultSet rs = dm.getCrossReference(null, null, null, null, null, null)) {
                checkForeignKeys(rs);
            }
        }
    }
}

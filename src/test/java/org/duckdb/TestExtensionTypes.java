package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.assertEquals;

import java.sql.*;

public class TestExtensionTypes {

    public static void test_extension_type() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement stmt = connection.createStatement()) {

            DuckDBNative.duckdb_jdbc_create_extension_type((DuckDBConnection) connection);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT {\"hello\": 'foo', \"world\": 'bar'}::test_type, '\\xAA'::byte_test_type")) {
                rs.next();
                Struct struct = (Struct) rs.getObject(1);
                Object[] attrs = struct.getAttributes();
                assertEquals(attrs[0], "foo");
                assertEquals(attrs[1], "bar");
                Blob blob = rs.getBlob(2);
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                assertEquals(bytes.length, 1);
                assertEquals(bytes[0] & 0xff, 0xaa);
            }
        }
    }

    public static void test_extension_type_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();) {
            DuckDBNative.duckdb_jdbc_create_extension_type((DuckDBConnection) conn);

            stmt.execute("CREATE TABLE test (foo test_type, bar byte_test_type);");
            stmt.execute("INSERT INTO test VALUES ({\"hello\": 'foo', \"world\": 'bar'}, '\\xAA');");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(meta.getColumnCount(), 2);

                assertEquals(meta.getColumnName(1), "foo");
                assertEquals(meta.getColumnTypeName(1), "test_type");
                assertEquals(meta.getColumnType(1), Types.OTHER);
                assertEquals(meta.getColumnClassName(1), "java.lang.String");

                assertEquals(meta.getColumnName(2), "bar");
                assertEquals(meta.getColumnTypeName(2), "byte_test_type");
                assertEquals(meta.getColumnType(2), Types.OTHER);
                assertEquals(meta.getColumnClassName(2), "java.lang.String");
            }
        }
    }
}

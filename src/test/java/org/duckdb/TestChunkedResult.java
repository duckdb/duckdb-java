package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.sql.DriverManager;

public class TestChunkedResult {

    public static void test_chunked_result_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             DuckDBPreparedStatement ps = conn.prepare("SELECT 42"); DuckDBChunkedResult res = ps.query()) {
            assertTrue(res.nextChunk());
            assertNotNull(res.chunk());
            assertEquals(res.chunk().vector(0).getInt(0), 42);
            assertFalse(res.nextChunk());
        }
    }

    public static void test_chunked_result_multiple_chunks() throws Exception {
        long count = (1 << 16) + 7;
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             DuckDBPreparedStatement ps = conn.prepare("SELECT\n"
                                                       + "    num::BIGINT AS col1,\n"
                                                       + "    repeat(num::VARCHAR, 16) AS col2\n"
                                                       + "FROM range(" + count + ") t(num);");
             DuckDBChunkedResult res = ps.query()) {
            assertEquals(res.columnCount(), (long) 2);
            assertEquals(res.columnName(0), "col1");
            assertEquals(res.columnTypeId(0), 5);
            long chunksCount = 0;
            long num = 0;
            while (res.nextChunk()) {
                assertNotNull(res.chunk());
                for (long row = 0; row < res.chunk().rowCount(); row++) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i++) {
                        sb.append(num);
                    }
                    assertEquals(res.chunk().vector(0).getLong(row), num);
                    assertEquals(res.chunk().vector(1).getString(row), sb.toString());
                    num += 1;
                }
                chunksCount += 1;
            }
            assertFalse(res.nextChunk());
            assertFalse(res.nextChunk());
            assertEquals(chunksCount, count / 2048 + 1);
        }
    }

    public static void test_chunked_result_params() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             DuckDBPreparedStatement ps = conn.prepare("SELECT ?")) {
            ps.setInt(1, 42);
            try (DuckDBChunkedResult res = ps.query()) {
                assertEquals(res.columnTypeId(0), 4);
                assertTrue(res.nextChunk());
                assertNotNull(res.chunk());
                assertEquals(res.chunk().vector(0).getInt(0), 42);
                assertFalse(res.nextChunk());
            }
            ps.setLong(1, 43);
            try (DuckDBChunkedResult res = ps.query()) {
                assertEquals(res.columnTypeId(0), 5);
                assertTrue(res.nextChunk());
                assertNotNull(res.chunk());
                assertEquals(res.chunk().vector(0).getLong(0), (long) 43);
                assertFalse(res.nextChunk());
            }
        }
    }
}

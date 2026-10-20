import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.duckdb.DuckDBDriver.JDBC_STREAM_RESULTS;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Runner.runTests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

/**
 * Tests for Arrow integration: {@link DuckDBConnection#registerArrowStream} and
 * {@link DuckDBResultSet#arrowExportStream}.
 *
 * Launched directly from source with test_arrow.sh, requires duckdb_jdbc.jar, duckdb_jdbc_tests.jar
 * and Arrow JARs (listed in data/arrow/arrow_jars.txt) on classpath.
 */
public class TestArrow {

    private static final Schema PEOPLE_SCHEMA = new Schema(
        asList(Field.nullable("id", new ArrowType.Int(32, true)), Field.nullable("name", ArrowType.Utf8.INSTANCE),
               Field.nullable("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
               Field.nullable("active", ArrowType.Bool.INSTANCE)));

    private static final Schema NUMBERS_SCHEMA = new Schema(asList(Field.nullable("v", new ArrowType.Int(64, true))));

    // registerArrowStream

    public static void test_arrow_register_count() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc =
                peopleIpc(allocator, person(1, "a", 1.0, true), person(2, "b", 2.0, false), person(3, "c", 3.0, true));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM people")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 3L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_values() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc = peopleIpc(allocator, person(1, "alice", 1.5, true), person(2, "bob", 2.5, false));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs = stmt.executeQuery("SELECT id, name, score, active FROM people ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt("id"), 1);
                assertEquals(rs.getString("name"), "alice");
                assertEquals(rs.getDouble("score"), 1.5d);
                assertTrue(rs.getBoolean("active"));

                assertTrue(rs.next());
                assertEquals(rs.getInt("id"), 2);
                assertEquals(rs.getString("name"), "bob");
                assertEquals(rs.getDouble("score"), 2.5d);
                assertFalse(rs.getBoolean("active"));

                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_column_types() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            register(conn, allocator, stream, "people", peopleIpc(allocator, person(1, "a", 1.0, true)));

            List<String> columns = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery("DESCRIBE SELECT * FROM people")) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name") + ":" + rs.getString("column_type"));
                }
            }
            assertListsEqual(columns, asList("id:INTEGER", "name:VARCHAR", "score:DOUBLE", "active:BOOLEAN"));
        }
    }

    public static void test_arrow_register_nulls() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc = peopleIpc(allocator, person(1, null, null, null), person(2, "x", 4.0, true));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs = stmt.executeQuery("SELECT name, score, active FROM people ORDER BY id")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
                rs.getDouble(2);
                assertTrue(rs.wasNull());
                rs.getBoolean(3);
                assertTrue(rs.wasNull());

                assertTrue(rs.next());
                assertEquals(rs.getString(1), "x");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_multiple_batches() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            // 5 batches of 1000 rows, values 0..4999
            register(conn, allocator, stream, "numbers", numbersIpc(allocator, 5, 1000));

            try (ResultSet rs = stmt.executeQuery("SELECT count(*), sum(v), min(v), max(v) FROM numbers")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 5000L);
                assertEquals(rs.getLong(2), 4999L * 5000L / 2);
                assertEquals(rs.getLong(3), 0L);
                assertEquals(rs.getLong(4), 4999L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_large_batches() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            // batches much larger than DuckDB vector size
            int batchCount = 10;
            int batchSize = 100_000;
            register(conn, allocator, stream, "numbers", numbersIpc(allocator, batchCount, batchSize));

            try (ResultSet rs = stmt.executeQuery("SELECT count(*), sum(v) FROM numbers")) {
                assertTrue(rs.next());
                long total = (long) batchCount * batchSize;
                assertEquals(rs.getLong(1), total);
                assertEquals(rs.getLong(2), (total - 1) * total / 2);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_empty_stream() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            register(conn, allocator, stream, "people", peopleIpc(allocator));

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM people")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(meta.getColumnCount(), 4);
                assertEquals(meta.getColumnName(1), "id");
                assertEquals(meta.getColumnName(4), "active");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_aggregation() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc = peopleIpc(allocator, person(1, "a", 10.0, true), person(2, "b", 20.0, false),
                                   person(3, "c", 30.0, true), person(4, "d", 40.0, true));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs = stmt.executeQuery("SELECT active, count(*) AS n, avg(score) AS avg_score "
                                                  + "FROM people WHERE id > 1 GROUP BY active ORDER BY active")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("active"));
                assertEquals(rs.getLong("n"), 1L);
                assertEquals(rs.getDouble("avg_score"), 20.0d);

                assertTrue(rs.next());
                assertTrue(rs.getBoolean("active"));
                assertEquals(rs.getLong("n"), 2L);
                assertEquals(rs.getDouble("avg_score"), 35.0d);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_join_two_streams() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream peopleStream = ArrowArrayStream.allocateNew(allocator);
             ArrowArrayStream numbersStream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] peopleIpc = peopleIpc(allocator, person(0, "alice", 0.0, true), person(1, "bob", 0.0, true),
                                         person(2, "carol", 0.0, true));
            register(conn, allocator, peopleStream, "people", peopleIpc);
            // values 0..9
            register(conn, allocator, numbersStream, "numbers", numbersIpc(allocator, 2, 5));

            try (ResultSet rs = stmt.executeQuery("SELECT p.name, count(*) FROM people p "
                                                  + "JOIN numbers n ON p.id = n.v % 3 "
                                                  + "GROUP BY p.name ORDER BY p.name")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "alice");
                assertEquals(rs.getLong(2), 4L);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "bob");
                assertEquals(rs.getLong(2), 3L);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "carol");
                assertEquals(rs.getLong(2), 3L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_join_table() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE bonus (id INTEGER, bonus DOUBLE)");
            stmt.execute("INSERT INTO bonus VALUES (1, 0.5), (3, 1.5)");
            byte[] ipc =
                peopleIpc(allocator, person(1, "a", 1.0, true), person(2, "b", 2.0, true), person(3, "c", 3.0, true));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT sum(p.score + b.bonus) FROM people p JOIN bonus b USING (id)")) {
                assertTrue(rs.next());
                assertEquals(rs.getDouble(1), 1.0d + 0.5d + 3.0d + 1.5d);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_arrow_register_create_table_as() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc = peopleIpc(allocator, person(1, "a", 1.0, true), person(2, "b", 2.0, false));
            register(conn, allocator, stream, "people", ipc);

            stmt.execute("CREATE TABLE people_copy AS SELECT * FROM people");

            // copy is a regular table and can be queried multiple times
            for (int i = 0; i < 2; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT count(*) FILTER (active), count(*) FROM people_copy")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 1L);
                    assertEquals(rs.getLong(2), 2L);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_arrow_register_stream_consumed_once() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            byte[] ipc = peopleIpc(allocator, person(1, "a", 1.0, true), person(2, "b", 2.0, true));
            register(conn, allocator, stream, "people", ipc);

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM people")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 2L);
            }

            // Arrow stream can only be read once, it is released after the first scan
            String msg = assertThrows(() -> { stmt.executeQuery("SELECT count(*) FROM people"); }, SQLException.class);
            assertTrue(msg.contains("This stream has been released"), msg);
        }
    }

    public static void test_arrow_register_connection_scope() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            register(conn, allocator, stream, "people", peopleIpc(allocator, person(1, "a", 1.0, true)));

            try (Connection dup = conn.duplicate(); Statement dupStmt = dup.createStatement()) {
                String msg =
                    assertThrows(() -> { dupStmt.executeQuery("SELECT count(*) FROM people"); }, SQLException.class);
                assertTrue(msg.contains("Table with name people does not exist"), msg);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM people")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1L);
            }
        }
    }

    public static void test_arrow_register_replace() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream1 = ArrowArrayStream.allocateNew(allocator);
             ArrowArrayStream stream2 = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            register(conn, allocator, stream1, "tab1", peopleIpc(allocator, person(1, "a", 1.0, true)));
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1L);
            }

            byte[] ipc =
                peopleIpc(allocator, person(1, "a", 1.0, true), person(2, "b", 2.0, true), person(3, "c", 3.0, true));
            register(conn, allocator, stream2, "tab1", ipc);
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 3L);
            }
        }
    }

    public static void test_arrow_register_stream_moved() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            TrackingReader reader = new TrackingReader(allocator, peopleIpc(allocator, person(1, "a", 1.0, true)));
            // stream is moved into the connection on registration, the passed struct can be closed right away
            try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                Data.exportArrayStream(allocator, reader, stream);
                conn.registerArrowStream("people", stream);
            }
            assertFalse(reader.closed);

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM people")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1L);
            }
            assertTrue(reader.closed);
        }
    }

    public static void test_arrow_register_released_on_connection_close() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            TrackingReader reader1 = new TrackingReader(allocator, peopleIpc(allocator, person(1, "a", 1.0, true)));
            TrackingReader reader2 = new TrackingReader(allocator, numbersIpc(allocator, 1, 10));
            try (ArrowArrayStream stream1 = ArrowArrayStream.allocateNew(allocator);
                 ArrowArrayStream stream2 = ArrowArrayStream.allocateNew(allocator)) {
                DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                Data.exportArrayStream(allocator, reader1, stream1);
                conn.registerArrowStream("people", stream1);
                Data.exportArrayStream(allocator, reader2, stream2);
                conn.registerArrowStream("numbers", stream2);

                // streams that were never consumed are released when connection is closed
                assertFalse(reader1.closed);
                assertFalse(reader2.closed);
                conn.close();
                assertTrue(reader1.closed);
                assertTrue(reader2.closed);

                assertThrows(() -> { conn.registerArrowStream("people", stream1); }, RuntimeException.class);
            }
        }
    }

    public static void test_arrow_register_invalid_argument() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            String msg =
                assertThrows(() -> { conn.registerArrowStream("tab1", "not a stream"); }, RuntimeException.class);
            assertTrue(msg.contains("Need to pass an ArrowArrayStream"), msg);
        }
    }

    public static void test_arrow_register_closed_connection() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
            conn.close();
            assertThrows(() -> { conn.registerArrowStream("tab1", stream); }, RuntimeException.class);
        }
    }

    // arrowExportStream

    public static void test_arrow_export_values() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT i::INTEGER AS id, 'name_' || i AS name, "
                                                    + "i / 2 AS half, i % 2 = 0 AS even FROM range(0, 3) t(i)")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(root.getRowCount(), 3);

            IntVector id = (IntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            Float8Vector half = (Float8Vector) root.getVector("half");
            BitVector even = (BitVector) root.getVector("even");
            for (int i = 0; i < 3; i++) {
                assertEquals(id.get(i), i);
                assertEquals(new String(name.get(i), UTF_8), "name_" + i);
                assertEquals(half.get(i), i / 2.0d);
                assertEquals(even.get(i), i % 2 == 0 ? 1 : 0);
            }

            assertFalse(reader.loadNextBatch());
        }
    }

    public static void test_arrow_export_schema() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT 42::INTEGER AS c_int, 42::BIGINT AS c_bigint, "
                                                    + "'foo' AS c_varchar, 1.5::DOUBLE AS c_double, "
                                                    + "true AS c_bool, DATE '2020-01-02' AS c_date")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            List<String> names = new ArrayList<>();
            for (Field field : schema.getFields()) {
                names.add(field.getName());
            }
            assertListsEqual(names, asList("c_int", "c_bigint", "c_varchar", "c_double", "c_bool", "c_date"));
            assertEquals(schema.findField("c_int").getType(), new ArrowType.Int(32, true));
            assertEquals(schema.findField("c_bigint").getType(), new ArrowType.Int(64, true));
            assertEquals(schema.findField("c_varchar").getType(), ArrowType.Utf8.INSTANCE);
            assertEquals(schema.findField("c_double").getType(),
                         new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            assertEquals(schema.findField("c_bool").getType(), ArrowType.Bool.INSTANCE);
            assertTrue(schema.findField("c_date").getType() instanceof ArrowType.Date);
        }
    }

    public static void test_arrow_export_temporal_and_decimal() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT DATE '1970-01-11' AS c_date, "
                                                    + "TIMESTAMP '2020-01-02 03:04:05.123456' AS c_ts, "
                                                    + "123.456::DECIMAL(18,3) AS c_dec")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(root.getRowCount(), 1);

            DateDayVector date = (DateDayVector) root.getVector("c_date");
            assertEquals(date.get(0), 10);

            TimeStampMicroVector ts = (TimeStampMicroVector) root.getVector("c_ts");
            assertEquals(ts.getObject(0), LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123456000));

            DecimalVector dec = (DecimalVector) root.getVector("c_dec");
            assertEquals(dec.getObject(0), new BigDecimal("123.456"));
        }
    }

    public static void test_arrow_export_nested() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT [1, 2, NULL, 4] AS c_list, "
                                                    + "{'a': 42, 'b': 'foo'} AS c_struct")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            ListVector list = (ListVector) root.getVector("c_list");
            assertEquals(list.getObject(0), asList(1, 2, null, 4));

            StructVector struct = (StructVector) root.getVector("c_struct");
            IntVector a = (IntVector) struct.getChild("a");
            VarCharVector b = (VarCharVector) struct.getChild("b");
            assertEquals(a.get(0), 42);
            assertEquals(new String(b.get(0), UTF_8), "foo");
        }
    }

    public static void test_arrow_export_nulls() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT CASE WHEN i % 2 = 0 THEN NULL ELSE i END AS v, "
                                                    + "CASE WHEN i % 2 = 0 THEN NULL ELSE 'foo' END AS s "
                                                    + "FROM range(0, 4) t(i)")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector v = root.getVector("v");
            FieldVector s = root.getVector("s");
            assertEquals(root.getRowCount(), 4);
            assertEquals(v.getNullCount(), 2);
            assertEquals(s.getNullCount(), 2);
            for (int i = 0; i < 4; i++) {
                assertEquals(v.isNull(i), i % 2 == 0);
                assertEquals(s.isNull(i), i % 2 == 0);
            }
        }
    }

    public static void test_arrow_export_batch_size() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT i FROM range(0, 10000) t(i)").unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 4096)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<Integer> batchSizes = new ArrayList<>();
            long sum = 0;
            while (reader.loadNextBatch()) {
                batchSizes.add(root.getRowCount());
                BigIntVector vec = (BigIntVector) root.getVector("i");
                for (int i = 0; i < root.getRowCount(); i++) {
                    sum += vec.get(i);
                }
            }
            assertListsEqual(batchSizes, asList(4096, 4096, 1808));
            assertEquals(sum, 9999L * 10000L / 2);
        }
    }

    public static void test_arrow_export_streaming_result() throws Exception {
        Properties config = new Properties();
        config.put(JDBC_STREAM_RESULTS, true);
        try (BufferAllocator allocator = new RootAllocator();
             Connection conn = DriverManager.getConnection(JDBC_URL, config); Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT i, 'name_' || i AS name FROM range(0, 100000) t(i)")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 10000)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            long count = 0;
            long sum = 0;
            int batches = 0;
            while (reader.loadNextBatch()) {
                assertTrue(root.getRowCount() <= 10000);
                BigIntVector vec = (BigIntVector) root.getVector("i");
                VarCharVector name = (VarCharVector) root.getVector("name");
                for (int i = 0; i < root.getRowCount(); i++) {
                    assertEquals(new String(name.get(i), UTF_8), "name_" + vec.get(i));
                    sum += vec.get(i);
                }
                count += root.getRowCount();
                batches++;
            }
            assertEquals(count, 100000L);
            assertEquals(sum, 99999L * 100000L / 2);
            assertEquals(batches, 10);
        }
    }

    public static void test_arrow_export_empty_result() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT i, i::VARCHAR AS s FROM range(0, 10) t(i) WHERE i < 0")
                                      .unwrap(DuckDBResultSet.class);
             ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
            assertEquals(reader.getVectorSchemaRoot().getSchema().getFields().size(), 2);
            assertFalse(reader.loadNextBatch());
        }
    }

    public static void test_arrow_export_invalid_argument() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT 42").unwrap(DuckDBResultSet.class)) {
            String msg =
                assertThrows(() -> { rs.arrowExportStream("not an allocator", 1024); }, RuntimeException.class);
            assertTrue(msg.contains("Need to pass an Arrow BufferAllocator"), msg);
        }
    }

    public static void test_arrow_export_closed_result_set() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            DuckDBResultSet rs = stmt.executeQuery("SELECT 42").unwrap(DuckDBResultSet.class);
            rs.close();
            assertThrows(() -> { rs.arrowExportStream(allocator, 1024); }, SQLException.class);
        }
    }

    public static void test_arrow_export_connection_closed() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            Connection conn = DriverManager.getConnection(JDBC_URL);
            Statement stmt = conn.createStatement();
            DuckDBResultSet rs = stmt.executeQuery("SELECT i FROM range(0, 10000) t(i)").unwrap(DuckDBResultSet.class);
            try (ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
                assertTrue(reader.loadNextBatch());
                assertEquals(reader.getVectorSchemaRoot().getRowCount(), 1024);

                // result is released when connection is closed, reader stays usable only for closing
                conn.close();
                String msg = assertThrows(reader::loadNextBatch, Exception.class);
                assertTrue(msg.contains("Connection was closed"), msg);
            }
            assertThrows(() -> { rs.arrowExportStream(allocator, 1024); }, SQLException.class);
        }
    }

    public static void test_arrow_export_reader_closed_before_connection() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 10; i++) {
                try (DuckDBResultSet rs =
                         stmt.executeQuery("SELECT i FROM range(0, 10000) t(i)").unwrap(DuckDBResultSet.class);
                     ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024)) {
                    assertTrue(reader.loadNextBatch());
                }
            }
            // connection stays usable after the exported readers are closed
            try (ResultSet rs = stmt.executeQuery("SELECT 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
            }
        }
    }

    // arrowExportStream -> registerArrowStream

    public static void test_arrow_export_register_round_trip() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT i AS id, 'name_' || i AS name FROM range(0, 10000) t(i)")
                                      .unwrap(DuckDBResultSet.class)) {
            ArrowReader reader = (ArrowReader) rs.arrowExportStream(allocator, 1024);
            // reader is owned by the exported stream and is closed by DuckDB when the stream is released
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream("exported", stream);

            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT count(*), sum(id), count(DISTINCT name) FROM exported")) {
                assertTrue(rs2.next());
                assertEquals(rs2.getLong(1), 10000L);
                assertEquals(rs2.getLong(2), 9999L * 10000L / 2);
                assertEquals(rs2.getLong(3), 10000L);
                assertFalse(rs2.next());
            }
        }
    }

    // helpers

    private static class Person {
        final Integer id;
        final String name;
        final Double score;
        final Boolean active;

        Person(Integer id, String name, Double score, Boolean active) {
            this.id = id;
            this.name = name;
            this.score = score;
            this.active = active;
        }
    }

    private static Person person(Integer id, String name, Double score, Boolean active) {
        return new Person(id, name, score, active);
    }

    // records whether the reader was closed by its consumer
    private static class TrackingReader extends ArrowStreamReader {
        boolean closed = false;

        TrackingReader(BufferAllocator allocator, byte[] ipc) {
            super(new ByteArrayInputStream(ipc), allocator);
        }

        @Override
        protected void closeReadSource() throws IOException {
            closed = true;
            super.closeReadSource();
        }
    }

    private interface BatchFiller {
        void fill(VectorSchemaRoot root, int batchIndex);
    }

    private static byte[] writeIpc(BufferAllocator allocator, Schema schema, int batchCount, BatchFiller filler)
        throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            for (int b = 0; b < batchCount; b++) {
                filler.fill(root, b);
                writer.writeBatch();
            }
            writer.end();
        }
        return out.toByteArray();
    }

    // single batch, or schema-only stream when no rows are passed
    private static byte[] peopleIpc(BufferAllocator allocator, Person... rows) throws Exception {
        return writeIpc(allocator, PEOPLE_SCHEMA, rows.length == 0 ? 0 : 1, (root, b) -> {
            IntVector id = (IntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            Float8Vector score = (Float8Vector) root.getVector("score");
            BitVector active = (BitVector) root.getVector("active");
            root.allocateNew();
            for (int i = 0; i < rows.length; i++) {
                Person p = rows[i];
                if (p.id == null) {
                    id.setNull(i);
                } else {
                    id.setSafe(i, p.id);
                }
                if (p.name == null) {
                    name.setNull(i);
                } else {
                    name.setSafe(i, p.name.getBytes(UTF_8));
                }
                if (p.score == null) {
                    score.setNull(i);
                } else {
                    score.setSafe(i, p.score);
                }
                if (p.active == null) {
                    active.setNull(i);
                } else {
                    active.setSafe(i, p.active ? 1 : 0);
                }
            }
            root.setRowCount(rows.length);
        });
    }

    // values are 0 .. batchCount * batchSize - 1
    private static byte[] numbersIpc(BufferAllocator allocator, int batchCount, int batchSize) throws Exception {
        return writeIpc(allocator, NUMBERS_SCHEMA, batchCount, (root, b) -> {
            BigIntVector vec = (BigIntVector) root.getVector("v");
            vec.allocateNew(batchSize);
            for (int i = 0; i < batchSize; i++) {
                vec.set(i, (long) b * batchSize + i);
            }
            root.setRowCount(batchSize);
        });
    }

    // reader is owned by the stream after export, it is closed when DuckDB releases the stream
    private static void register(DuckDBConnection conn, BufferAllocator allocator, ArrowArrayStream stream, String name,
                                 byte[] ipc) {
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
        Data.exportArrayStream(allocator, reader, stream);
        conn.registerArrowStream(name, stream);
    }

    public static void main(String[] args) throws Exception {
        System.exit(runTests(args, TestArrow.class));
    }
}

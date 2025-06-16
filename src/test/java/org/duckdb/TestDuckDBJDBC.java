package org.duckdb;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.OFFSET_SECONDS;
import static java.time.temporal.ChronoField.YEAR_OF_ERA;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.duckdb.DuckDBDriver.DUCKDB_USER_AGENT_PROPERTY;
import static org.duckdb.DuckDBDriver.JDBC_STREAM_RESULTS;
import static org.duckdb.DuckDBTimestamp.localDateTimeFromTimestamp;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Runner.runTests;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

public class TestDuckDBJDBC {

    public static final String JDBC_URL = "jdbc:duckdb:";

    private static void createTable(Connection conn) throws SQLException {
        try (Statement createStmt = conn.createStatement()) {
            createStmt.execute("CREATE TABLE foo as select * from range(1000000);");
        }
    }

    private static void executeStatementWithThread(Statement statement, ExecutorService executor_service)
        throws InterruptedException {
        executor_service.submit(() -> {
            try (ResultSet resultSet = statement.executeQuery("SELECT * from foo")) {
                assertThrowsMaybe(() -> {
                    DuckDBResultSet duckdb_result_set = resultSet.unwrap(DuckDBResultSet.class);
                    while (duckdb_result_set.next()) {
                        // do nothing with the results
                    }
                }, SQLException.class);

            } catch (Exception e) {
                System.out.println("Error executing query: " + e.getMessage());
            }
        });

        Thread.sleep(10); // wait for query to start running
        try {
            statement.cancel();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void test_connection() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        assertTrue(conn.isValid(0));
        assertFalse(conn.isClosed());

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT 42 as a");
        assertFalse(stmt.isClosed());
        assertFalse(rs.isClosed());

        assertTrue(rs.next());
        int res = rs.getInt(1);
        assertEquals(res, 42);
        assertFalse(rs.wasNull());

        res = rs.getInt(1);
        assertEquals(res, 42);
        assertFalse(rs.wasNull());

        res = rs.getInt("a");
        assertEquals(res, 42);
        assertFalse(rs.wasNull());

        assertThrows(() -> rs.getInt(0), SQLException.class);

        assertThrows(() -> rs.getInt(2), SQLException.class);

        assertThrows(() -> rs.getInt("b"), SQLException.class);

        assertFalse(rs.next());
        assertFalse(rs.next());

        rs.close();
        rs.close();
        assertTrue(rs.isClosed());

        assertThrows(() -> rs.getInt(1), SQLException.class);

        stmt.close();
        stmt.close();
        assertTrue(stmt.isClosed());

        conn.close();
        conn.close();
        assertFalse(conn.isValid(0));
        assertTrue(conn.isClosed());

        assertThrows(conn::createStatement, SQLException.class);
    }

    public static void test_execute_exception() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        assertThrows(() -> {
            ResultSet rs = stmt.executeQuery("SELECT");
            rs.next();
        }, SQLException.class);
    }

    public static void test_autocommit_off() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs;

        conn.setAutoCommit(false);

        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t (id INT);");
        conn.commit();

        stmt.execute("INSERT INTO t (id) VALUES (1);");
        stmt.execute("INSERT INTO t (id) VALUES (2);");
        stmt.execute("INSERT INTO t (id) VALUES (3);");
        conn.commit();

        rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        rs.close();

        stmt.execute("INSERT INTO t (id) VALUES (4);");
        stmt.execute("INSERT INTO t (id) VALUES (5);");
        conn.rollback();

        // After the rollback both inserts must be reverted
        rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
        rs.next();
        assertEquals(rs.getInt(1), 3);

        stmt.execute("INSERT INTO t (id) VALUES (6);");
        stmt.execute("INSERT INTO t (id) VALUES (7);");

        conn.setAutoCommit(true);

        // Turning auto-commit on triggers a commit
        rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
        rs.next();
        assertEquals(rs.getInt(1), 5);

        // This means a rollback must not be possible now
        assertThrows(conn::rollback, SQLException.class);

        stmt.execute("INSERT INTO t (id) VALUES (8);");
        rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
        rs.next();
        assertEquals(rs.getInt(1), 6);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_enum() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        ResultSet rs;

        // Test 8 bit enum + different access ways
        stmt.execute("CREATE TYPE enum_test AS ENUM ('Enum1', 'enum2', '1üöñ');");
        stmt.execute("CREATE TABLE t (id INT, e1 enum_test);");
        stmt.execute("INSERT INTO t (id, e1) VALUES (1, 'Enum1');");
        stmt.execute("INSERT INTO t (id, e1) VALUES (2, 'enum2');");
        stmt.execute("INSERT INTO t (id, e1) VALUES (3, '1üöñ');");

        PreparedStatement ps = conn.prepareStatement("SELECT e1 FROM t WHERE id = ?");
        ps.setObject(1, 1);
        rs = ps.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1, String.class).equals("Enum1"));
        assertTrue(rs.getString(1).equals("Enum1"));
        assertTrue(rs.getString("e1").equals("Enum1"));
        rs.close();

        ps.setObject(1, 2);
        rs = ps.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1, String.class).equals("enum2"));
        assertTrue(rs.getObject(1).equals("enum2"));
        rs.close();

        ps.setObject(1, 3);
        rs = ps.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1, String.class).equals("1üöñ"));
        assertTrue(rs.getObject(1).equals("1üöñ"));
        assertTrue(rs.getObject("e1").equals("1üöñ"));
        rs.close();

        ps = conn.prepareStatement("SELECT e1 FROM t WHERE e1 = ?");
        ps.setObject(1, "1üöñ");
        rs = ps.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1, String.class).equals("1üöñ"));
        assertTrue(rs.getString(1).equals("1üöñ"));
        assertTrue(rs.getString("e1").equals("1üöñ"));
        rs.close();

        // Test 16 bit enum
        stmt.execute(
            "CREATE TYPE enum_long AS ENUM ('enum0' ,'enum1' ,'enum2' ,'enum3' ,'enum4' ,'enum5' ,'enum6'"
            +
            ",'enum7' ,'enum8' ,'enum9' ,'enum10' ,'enum11' ,'enum12' ,'enum13' ,'enum14' ,'enum15' ,'enum16' ,'enum17'"
            +
            ",'enum18' ,'enum19' ,'enum20' ,'enum21' ,'enum22' ,'enum23' ,'enum24' ,'enum25' ,'enum26' ,'enum27' ,'enum28'"
            +
            ",'enum29' ,'enum30' ,'enum31' ,'enum32' ,'enum33' ,'enum34' ,'enum35' ,'enum36' ,'enum37' ,'enum38' ,'enum39'"
            +
            ",'enum40' ,'enum41' ,'enum42' ,'enum43' ,'enum44' ,'enum45' ,'enum46' ,'enum47' ,'enum48' ,'enum49' ,'enum50'"
            +
            ",'enum51' ,'enum52' ,'enum53' ,'enum54' ,'enum55' ,'enum56' ,'enum57' ,'enum58' ,'enum59' ,'enum60' ,'enum61'"
            +
            ",'enum62' ,'enum63' ,'enum64' ,'enum65' ,'enum66' ,'enum67' ,'enum68' ,'enum69' ,'enum70' ,'enum71' ,'enum72'"
            +
            ",'enum73' ,'enum74' ,'enum75' ,'enum76' ,'enum77' ,'enum78' ,'enum79' ,'enum80' ,'enum81' ,'enum82' ,'enum83'"
            +
            ",'enum84' ,'enum85' ,'enum86' ,'enum87' ,'enum88' ,'enum89' ,'enum90' ,'enum91' ,'enum92' ,'enum93' ,'enum94'"
            +
            ",'enum95' ,'enum96' ,'enum97' ,'enum98' ,'enum99' ,'enum100' ,'enum101' ,'enum102' ,'enum103' ,'enum104' "
            +
            ",'enum105' ,'enum106' ,'enum107' ,'enum108' ,'enum109' ,'enum110' ,'enum111' ,'enum112' ,'enum113' ,'enum114'"
            +
            ",'enum115' ,'enum116' ,'enum117' ,'enum118' ,'enum119' ,'enum120' ,'enum121' ,'enum122' ,'enum123' ,'enum124'"
            +
            ",'enum125' ,'enum126' ,'enum127' ,'enum128' ,'enum129' ,'enum130' ,'enum131' ,'enum132' ,'enum133' ,'enum134'"
            +
            ",'enum135' ,'enum136' ,'enum137' ,'enum138' ,'enum139' ,'enum140' ,'enum141' ,'enum142' ,'enum143' ,'enum144'"
            +
            ",'enum145' ,'enum146' ,'enum147' ,'enum148' ,'enum149' ,'enum150' ,'enum151' ,'enum152' ,'enum153' ,'enum154'"
            +
            ",'enum155' ,'enum156' ,'enum157' ,'enum158' ,'enum159' ,'enum160' ,'enum161' ,'enum162' ,'enum163' ,'enum164'"
            +
            ",'enum165' ,'enum166' ,'enum167' ,'enum168' ,'enum169' ,'enum170' ,'enum171' ,'enum172' ,'enum173' ,'enum174'"
            +
            ",'enum175' ,'enum176' ,'enum177' ,'enum178' ,'enum179' ,'enum180' ,'enum181' ,'enum182' ,'enum183' ,'enum184'"
            +
            ",'enum185' ,'enum186' ,'enum187' ,'enum188' ,'enum189' ,'enum190' ,'enum191' ,'enum192' ,'enum193' ,'enum194'"
            +
            ",'enum195' ,'enum196' ,'enum197' ,'enum198' ,'enum199' ,'enum200' ,'enum201' ,'enum202' ,'enum203' ,'enum204'"
            +
            ",'enum205' ,'enum206' ,'enum207' ,'enum208' ,'enum209' ,'enum210' ,'enum211' ,'enum212' ,'enum213' ,'enum214'"
            +
            ",'enum215' ,'enum216' ,'enum217' ,'enum218' ,'enum219' ,'enum220' ,'enum221' ,'enum222' ,'enum223' ,'enum224'"
            +
            ",'enum225' ,'enum226' ,'enum227' ,'enum228' ,'enum229' ,'enum230' ,'enum231' ,'enum232' ,'enum233' ,'enum234'"
            +
            ",'enum235' ,'enum236' ,'enum237' ,'enum238' ,'enum239' ,'enum240' ,'enum241' ,'enum242' ,'enum243' ,'enum244'"
            +
            ",'enum245' ,'enum246' ,'enum247' ,'enum248' ,'enum249' ,'enum250' ,'enum251' ,'enum252' ,'enum253' ,'enum254'"
            +
            ",'enum255' ,'enum256' ,'enum257' ,'enum258' ,'enum259' ,'enum260' ,'enum261' ,'enum262' ,'enum263' ,'enum264'"
            +
            ",'enum265' ,'enum266' ,'enum267' ,'enum268' ,'enum269' ,'enum270' ,'enum271' ,'enum272' ,'enum273' ,'enum274'"
            +
            ",'enum275' ,'enum276' ,'enum277' ,'enum278' ,'enum279' ,'enum280' ,'enum281' ,'enum282' ,'enum283' ,'enum284'"
            +
            ",'enum285' ,'enum286' ,'enum287' ,'enum288' ,'enum289' ,'enum290' ,'enum291' ,'enum292' ,'enum293' ,'enum294'"
            + ",'enum295' ,'enum296' ,'enum297' ,'enum298' ,'enum299');");

        stmt.execute("CREATE TABLE t2 (id INT, e1 enum_long);");
        stmt.execute("INSERT INTO t2 (id, e1) VALUES (1, 'enum290');");

        ps = conn.prepareStatement("SELECT e1 FROM t2 WHERE id = ?");
        ps.setObject(1, 1);
        rs = ps.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1, String.class).equals("enum290"));
        assertTrue(rs.getString(1).equals("enum290"));
        assertTrue(rs.getString("e1").equals("enum290"));
        rs.close();
        conn.close();
    }

    public static void test_list_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT generate_series(2) as list");) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "list");
            assertEquals(meta.getColumnTypeName(1), "BIGINT[]");
            assertEquals(meta.getColumnType(1), Types.ARRAY);
        }
    }

    public static void test_struct_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT {'i': 42, 'j': 'a'} as struct")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "struct");
            assertEquals(meta.getColumnTypeName(1), "STRUCT(i INTEGER, j VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.STRUCT);
        }
    }

    public static void test_map_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT map([1,2],['a','b']) as map")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "map");
            assertEquals(meta.getColumnTypeName(1), "MAP(INTEGER, VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.OTHER);
        }
    }

    public static void test_union_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT union_value(str := 'three') as union")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "union");
            assertEquals(meta.getColumnTypeName(1), "UNION(str VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.OTHER);
        }
    }

    public static void test_native_duckdb_array() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT generate_series(1)::BIGINT[2] as \"array\"");
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "array");
            assertEquals(meta.getColumnTypeName(1), "BIGINT[2]");
            assertEquals(meta.getColumnType(1), Types.ARRAY);
            assertEquals(meta.getColumnClassName(1), DuckDBArray.class.getName());

            assertTrue(rs.next());
            assertListsEqual(toJavaObject(rs.getObject(1)), asList(0L, 1L));
        }
    }

    public static void test_multiple_connections() throws Exception {
        Connection conn1 = DriverManager.getConnection(JDBC_URL);
        Statement stmt1 = conn1.createStatement();
        Connection conn2 = DriverManager.getConnection(JDBC_URL);
        Statement stmt2 = conn2.createStatement();
        Statement stmt3 = conn2.createStatement();

        ResultSet rs1 = stmt1.executeQuery("SELECT 42");
        assertTrue(rs1.next());
        assertEquals(42, rs1.getInt(1));
        rs1.close();

        ResultSet rs2 = stmt2.executeQuery("SELECT 43");
        assertTrue(rs2.next());
        assertEquals(43, rs2.getInt(1));

        ResultSet rs3 = stmt3.executeQuery("SELECT 44");
        assertTrue(rs3.next());
        assertEquals(44, rs3.getInt(1));
        rs3.close();

        // creative closing sequence should also work
        stmt2.close();

        rs3 = stmt3.executeQuery("SELECT 44");
        assertTrue(rs3.next());
        assertEquals(44, rs3.getInt(1));

        stmt2.close();
        rs2.close();
        rs3.close();

        System.gc();
        System.gc();

        // stmt1 still works
        rs1 = stmt1.executeQuery("SELECT 42");
        assertTrue(rs1.next());
        assertEquals(42, rs1.getInt(1));
        rs1.close();

        // stmt3 still works
        rs3 = stmt3.executeQuery("SELECT 42");
        assertTrue(rs3.next());
        assertEquals(42, rs3.getInt(1));
        rs3.close();

        conn2.close();

        stmt3.close();

        rs2 = null;
        rs3 = null;
        stmt2 = null;
        stmt3 = null;
        conn2 = null;

        System.gc();
        System.gc();

        // stmt1 still works
        rs1 = stmt1.executeQuery("SELECT 42");
        assertTrue(rs1.next());
        assertEquals(42, rs1.getInt(1));
        rs1.close();
        conn1.close();
        stmt1.close();
    }

    public static void test_multiple_statements_execution() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("CREATE TABLE integers(i integer);\n"
                                         + "insert into integers select * from range(10);"
                                         + "select * from integers;");
        int i = 0;
        while (rs.next()) {
            assertEquals(rs.getInt("i"), i);
            i++;
        }
        assertEquals(i, 10);
    }

    public static void test_multiple_statements_exception() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        boolean succ = false;
        try {
            stmt.executeQuery("CREATE TABLE integers(i integer, i boolean);\n"
                              + "CREATE TABLE integers2(i integer);\n"
                              + "insert into integers2 select * from range(10);\n"
                              + "select * from integers2;");
            succ = true;
        } catch (Exception ex) {
            assertFalse(succ);
        }
    }

    public static void test_crash_bug496() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE t0(c0 BOOLEAN, c1 INT)");
        stmt.execute("CREATE INDEX i0 ON t0(c1, c0)");
        stmt.execute("INSERT INTO t0(c1) VALUES (0)");
        stmt.close();
        conn.close();
    }

    public static void test_tablepragma_bug491() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE t0(c0 INT)");

        ResultSet rs = stmt.executeQuery("PRAGMA table_info('t0')");
        assertTrue(rs.next());

        assertEquals(rs.getInt("cid"), 0);
        assertEquals(rs.getString("name"), "c0");
        assertEquals(rs.getString("type"), "INTEGER");
        assertEquals(rs.getBoolean("notnull"), false);
        rs.getString("dflt_value");
        // assertTrue(rs.wasNull());
        assertEquals(rs.getBoolean("pk"), false);

        assertFalse(rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_nulltruth_bug489() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE t0(c0 INT)");
        stmt.execute("INSERT INTO t0(c0) VALUES (0)");

        ResultSet rs = stmt.executeQuery("SELECT * FROM t0 WHERE NOT(NULL OR TRUE)");
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT NOT(NULL OR TRUE)");
        assertTrue(rs.next());
        boolean res = rs.getBoolean(1);
        assertEquals(res, false);
        assertFalse(rs.wasNull());

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_empty_prepare_bug500() throws Exception {
        String fileContent = "CREATE TABLE t0(c0 VARCHAR, c1 DOUBLE);\n"
                             + "CREATE TABLE t1(c0 DOUBLE, PRIMARY KEY(c0));\n"
                             + "INSERT INTO t0(c0) VALUES (0), (0), (0), (0);\n"
                             + "INSERT INTO t0(c0) VALUES (NULL), (NULL);\n"
                             + "INSERT INTO t1(c0) VALUES (0), (1);\n"
                             + "\n"
                             + "SELECT t0.c0 FROM t0, t1;";
        Connection con = DriverManager.getConnection(JDBC_URL);
        for (String s : fileContent.split("\n")) {
            Statement st = con.createStatement();
            try {
                st.execute(s);
            } catch (SQLException e) {
                // e.printStackTrace();
            }
        }
        con.close();
    }

    public static void test_borked_string_bug539() throws Exception {
        Connection con = DriverManager.getConnection(JDBC_URL);
        Statement s = con.createStatement();
        s.executeUpdate("CREATE TABLE t0 (c0 VARCHAR)");
        String q = String.format("INSERT INTO t0 VALUES('%c')", 55995);
        s.executeUpdate(q);
        s.close();
        con.close();
    }

    public static void test_read_only() throws Exception {
        Path database_file = Files.createTempFile("duckdb-jdbc-test-", ".duckdb");
        Files.deleteIfExists(database_file);

        String jdbc_url = JDBC_URL + database_file;
        Properties ro_prop = new Properties();
        ro_prop.setProperty("duckdb.read_only", "true");

        Connection conn_rw = DriverManager.getConnection(jdbc_url);
        assertFalse(conn_rw.isReadOnly());
        assertFalse(conn_rw.getMetaData().isReadOnly());
        Statement stmt = conn_rw.createStatement();
        stmt.execute("CREATE TABLE test (i INTEGER)");
        stmt.execute("INSERT INTO test VALUES (42)");
        stmt.close();

        // Verify we can open additional write connections
        // Using the Driver
        try (Connection conn = DriverManager.getConnection(jdbc_url); Statement stmt1 = conn.createStatement();
             ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
            rs1.next();
            assertEquals(rs1.getInt(1), 42);
        }
        // Using the direct API
        try (Connection conn = conn_rw.unwrap(DuckDBConnection.class).duplicate();
             Statement stmt1 = conn.createStatement(); ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
            rs1.next();
            assertEquals(rs1.getInt(1), 42);
        }

        // At this time, mixing read and write connections on Windows doesn't work
        // Read-only when we already have a read-write
        //		try (Connection conn = DriverManager.getConnection(jdbc_url, ro_prop);
        //				 Statement stmt1 = conn.createStatement();
        //				 ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
        //			rs1.next();
        //			assertEquals(rs1.getInt(1), 42);
        //		}

        conn_rw.close();

        try (Statement ignored = conn_rw.createStatement()) {
            fail("Connection was already closed; shouldn't be able to create a statement");
        } catch (SQLException e) {
        }

        try (Connection ignored = conn_rw.unwrap(DuckDBConnection.class).duplicate()) {
            fail("Connection was already closed; shouldn't be able to duplicate");
        } catch (SQLException e) {
        }

        // // we can create two parallel read only connections and query them, too
        try (Connection conn_ro1 = DriverManager.getConnection(jdbc_url, ro_prop);
             Connection conn_ro2 = DriverManager.getConnection(jdbc_url, ro_prop)) {

            assertTrue(conn_ro1.isReadOnly());
            assertTrue(conn_ro1.getMetaData().isReadOnly());
            assertTrue(conn_ro2.isReadOnly());
            assertTrue(conn_ro2.getMetaData().isReadOnly());

            try (Statement stmt1 = conn_ro1.createStatement();
                 ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
                rs1.next();
                assertEquals(rs1.getInt(1), 42);
            }

            try (Statement stmt2 = conn_ro2.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT * FROM test")) {
                rs2.next();
                assertEquals(rs2.getInt(1), 42);
            }
        }
    }

    public static void test_temporal_types() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(
            "SELECT '2019-11-26 21:11:00'::timestamp ts, '2019-11-26'::date dt, interval '5 days' iv, '21:11:00'::time te");
        assertTrue(rs.next());
        assertEquals(rs.getObject("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));
        assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));

        assertEquals(rs.getObject("dt"), LocalDate.parse("2019-11-26"));
        assertEquals(rs.getDate("dt"), Date.valueOf("2019-11-26"));

        assertEquals(rs.getObject("iv"), "5 days");

        assertEquals(rs.getObject("te"), LocalTime.parse("21:11:00"));
        assertEquals(rs.getTime("te"), Time.valueOf("21:11:00"));

        assertFalse(rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_decimal() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT '1.23'::decimal(3,2) d");

        assertTrue(rs.next());
        assertEquals(rs.getDouble("d"), 1.23);

        assertFalse(rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }
    public static void test_wildcard_reflection() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE _a (_i INTEGER, xi INTEGER)");
        stmt.execute("CREATE TABLE xa (i INTEGER)");

        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs;
        rs = md.getTables(null, null, "\\_a", null);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "_a");
        assertFalse(rs.next());
        rs.close();

        rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "\\_a", "\\_i");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "_a");
        assertEquals(rs.getString("COLUMN_NAME"), "_i");
        assertFalse(rs.next());

        rs.close();
        conn.close();
    }

    public static void test_schema_reflection() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
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

        conn.close();
    }

    public static void test_column_reflection() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE a (a DECIMAL(20,5), b CHAR(10), c VARCHAR(30), d LONG)");

        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs;
        rs = md.getColumns(null, null, "a", null);
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

        rs.close();
        conn.close();
    }

    public static void test_get_tables_with_current_catalog() throws Exception {
        ResultSet resultSet = null;
        Connection conn = DriverManager.getConnection(JDBC_URL);
        final String currentCatalog = conn.getCatalog();
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE T1(ID INT)");
        // verify that the catalog argument is supported and does not throw
        try {
            resultSet = databaseMetaData.getTables(currentCatalog, null, "%", null);
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("Actual catalog argument is not supported"));
        }
        assertTrue(resultSet.next(), "getTables should return exactly 1 table");
        final String returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            currentCatalog.equals(returnedCatalog),
            String.format("Returned catalog %s should equal current catalog %s", returnedCatalog, currentCatalog));
        assertTrue(resultSet.next() == false, "getTables should return exactly 1 table");

        resultSet.close();
    }

    public static void test_get_tables_with_attached_catalog() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        final String currentCatalog = conn.getCatalog();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        Statement statement = conn.createStatement();

        // create one table in the current catalog
        final String TABLE_NAME1 = "T1";
        statement.execute(String.format("CREATE TABLE %s(ID INT)", TABLE_NAME1));

        // create one table in an attached catalog
        String returnedCatalog, returnedTableName;
        ResultSet resultSet = null;
        final String ATTACHED_CATALOG = "ATTACHED_CATALOG";
        final String TABLE_NAME2 = "T2";
        statement.execute(String.format("ATTACH '' AS \"%s\"", ATTACHED_CATALOG));
        statement.execute(String.format("CREATE TABLE %s.%s(ID INT)", ATTACHED_CATALOG, TABLE_NAME2));

        // test if getTables can get tables from the remote catalog.
        resultSet = databaseMetaData.getTables(ATTACHED_CATALOG, null, "%", null);
        assertTrue(resultSet.next(), "getTables should return exactly 1 table");
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            ATTACHED_CATALOG.equals(returnedCatalog),
            String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog, ATTACHED_CATALOG));
        assertTrue(resultSet.next() == false, "getTables should return exactly 1 table");
        resultSet.close();

        // test if getTables with null catalog returns all tables.
        resultSet = databaseMetaData.getTables(null, null, "%", null);

        assertTrue(resultSet.next(), "getTables should return 2 tables, got 0");
        // first table should be ATTACHED_CATALOG.T2
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            ATTACHED_CATALOG.equals(returnedCatalog),
            String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog, ATTACHED_CATALOG));
        returnedTableName = resultSet.getString("TABLE_NAME");
        assertTrue(TABLE_NAME2.equals(returnedTableName),
                   String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME2));

        assertTrue(resultSet.next(), "getTables should return 2 tables, got 1");
        // second table should be <current catalog>.T1
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            currentCatalog.equals(returnedCatalog),
            String.format("Returned catalog %s should equal current catalog %s", returnedCatalog, currentCatalog));
        returnedTableName = resultSet.getString("TABLE_NAME");
        assertTrue(TABLE_NAME1.equals(returnedTableName),
                   String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME1));

        assertTrue(resultSet.next() == false, "getTables should return 2 tables, got > 2");
        resultSet.close();
        statement.close();
        conn.close();
    }

    public static void test_get_tables_param_binding_for_table_types() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet rs = databaseMetaData.getTables(null, null, null,
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
                                                                + ", 'fake ref generation' -- "});
        assertFalse(rs.next());
        rs.close();
    }

    public static void test_get_table_types() throws Exception {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        List<String> tableTypesList = new ArrayList<>(asList(tableTypesArray));
        tableTypesList.sort(Comparator.naturalOrder());

        Connection conn = DriverManager.getConnection(JDBC_URL);
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet rs = databaseMetaData.getTableTypes();

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

    public static void test_get_schemas_with_params() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        String inputCatalog = conn.getCatalog();
        String inputSchema = conn.getSchema();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet resultSet = null;

        // catalog equal to current_catalog, schema null
        try {
            resultSet = databaseMetaData.getSchemas(inputCatalog, null);
            assertTrue(resultSet.next(), "Expected at least exactly 1 row, got 0");
            do {
                String outputCatalog = resultSet.getString("TABLE_CATALOG");
                assertTrue(inputCatalog.equals(outputCatalog),
                           "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                               inputCatalog);
            } while (resultSet.next());
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }

        // catalog equal to current_catalog, schema '%'
        ResultSet resultSetWithNullSchema = null;
        try {
            resultSet = databaseMetaData.getSchemas(inputCatalog, "%");
            resultSetWithNullSchema = databaseMetaData.getSchemas(inputCatalog, null);
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
                String schema1 = resultSet.getString("TABLE_SCHEMA");
                String schema2 = resultSetWithNullSchema.getString("TABLE_SCHEMA");
                assertTrue(schema1.equals(schema2), "schema " + schema1 + " from getSchemas with % should equal " +
                                                        schema2 + " from getSchemas with null");
            } while (resultSet.next() && resultSetWithNullSchema.next());
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }

        // empty catalog
        try {
            resultSet = databaseMetaData.getSchemas("", null);
            assertTrue(resultSet.next() == false, "Expected 0 schemas, got > 0");
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }
    }

    public static void test_connect_wrong_url_bug848() throws Exception {
        Driver d = new DuckDBDriver();
        assertNull(d.connect("jdbc:h2:", null));
    }

    public static void test_new_connection_wrong_url_bug10441() throws Exception {
        assertThrows(() -> {
            Connection connection = DuckDBConnection.newConnection("jdbc:duckdb@", false, new Properties());
            try {
                connection.close();
            } catch (SQLException e) {
                // ignored
            }
        }, SQLException.class);
    }

    public static void test_parquet_reader() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM parquet_scan('data/parquet-testing/userdata1.parquet')");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1000);
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_crash_autocommit_bug939() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE ontime(flightdate DATE)");
        conn.setAutoCommit(false); // The is the key to getting the crash to happen.
        stmt.executeUpdate();
        stmt.close();
        conn.close();
    }

    public static void test_explain_bug958() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN SELECT 42");
        assertTrue(rs.next());
        assertTrue(rs.getString(1) != null);
        assertTrue(rs.getString(2) != null);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_numbers() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        // int8, int4, int2, int1, float8, float4
        stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers");

        for (int i = 0; i < 50; i++) {
            appender.beginRow();
            appender.append(Long.MAX_VALUE - i);
            appender.append(Integer.MAX_VALUE - i);
            appender.append(Short.MAX_VALUE - i);
            appender.append(Byte.MAX_VALUE - i);
            appender.append(i);
            appender.append(i);
            appender.endRow();
        }
        appender.close();

        ResultSet rs = stmt.executeQuery("SELECT max(a), max(b), max(c), max(d), max(e), max(f) FROM numbers");
        assertFalse(rs.isClosed());
        assertTrue(rs.next());

        long resA = rs.getLong(1);
        assertEquals(resA, Long.MAX_VALUE);

        int resB = rs.getInt(2);
        assertEquals(resB, Integer.MAX_VALUE);

        short resC = rs.getShort(3);
        assertEquals(resC, Short.MAX_VALUE);

        byte resD = rs.getByte(4);
        assertEquals(resD, Byte.MAX_VALUE);

        double resE = rs.getDouble(5);
        assertEquals(resE, 49.0d);

        float resF = rs.getFloat(6);
        assertEquals(resF, 49.0f);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_date_and_time() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE date_and_time (id INT4, a TIMESTAMP)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "date_and_time");

        LocalDateTime ldt1 = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
        LocalDateTime ldt2 = LocalDateTime.of(-23434, 3, 5, 23, 2);
        LocalDateTime ldt3 = LocalDateTime.of(1970, 1, 1, 0, 0);
        LocalDateTime ldt4 = LocalDateTime.of(11111, 12, 31, 23, 59, 59, 999999000);

        appender.beginRow();
        appender.append(1);
        appender.appendLocalDateTime(ldt1);
        appender.endRow();
        appender.beginRow();
        appender.append(2);
        appender.appendLocalDateTime(ldt2);
        appender.endRow();
        appender.beginRow();
        appender.append(3);
        appender.appendLocalDateTime(ldt3);
        appender.endRow();
        appender.beginRow();
        appender.append(4);
        appender.appendLocalDateTime(ldt4);
        appender.endRow();
        appender.close();

        ResultSet rs = stmt.executeQuery("SELECT a FROM date_and_time ORDER BY id");
        assertFalse(rs.isClosed());
        assertTrue(rs.next());

        LocalDateTime res1 = (LocalDateTime) rs.getObject(1, LocalDateTime.class);
        assertEquals(res1, ldt1);
        assertTrue(rs.next());

        LocalDateTime res2 = (LocalDateTime) rs.getObject(1, LocalDateTime.class);
        assertEquals(res2, ldt2);
        assertTrue(rs.next());

        LocalDateTime res3 = (LocalDateTime) rs.getObject(1, LocalDateTime.class);
        assertEquals(res3, ldt3);
        assertTrue(rs.next());

        LocalDateTime res4 = (LocalDateTime) rs.getObject(1, LocalDateTime.class);
        assertEquals(res4, ldt4);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_decimal() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute(
            "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");

        BigDecimal bigdec16 = new BigDecimal("12.34").setScale(2);
        BigDecimal bigdec32 = new BigDecimal("1234.5678").setScale(4);
        BigDecimal bigdec64 = new BigDecimal("123456789012.345678").setScale(6);
        BigDecimal bigdec128 = new BigDecimal("123456789012345678.90123456789012345678").setScale(20);
        BigDecimal negbigdec16 = new BigDecimal("-12.34").setScale(2);
        BigDecimal negbigdec32 = new BigDecimal("-1234.5678").setScale(4);
        BigDecimal negbigdec64 = new BigDecimal("-123456789012.345678").setScale(6);
        BigDecimal negbigdec128 = new BigDecimal("-123456789012345678.90123456789012345678").setScale(20);
        BigDecimal smallbigdec16 = new BigDecimal("-1.34").setScale(2);
        BigDecimal smallbigdec32 = new BigDecimal("-123.5678").setScale(4);
        BigDecimal smallbigdec64 = new BigDecimal("-12345678901.345678").setScale(6);
        BigDecimal smallbigdec128 = new BigDecimal("-12345678901234567.90123456789012345678").setScale(20);
        BigDecimal intbigdec16 = new BigDecimal("-1").setScale(2);
        BigDecimal intbigdec32 = new BigDecimal("-123").setScale(4);
        BigDecimal intbigdec64 = new BigDecimal("-12345678901").setScale(6);
        BigDecimal intbigdec128 = new BigDecimal("-12345678901234567").setScale(20);
        BigDecimal onebigdec16 = new BigDecimal("1").setScale(2);
        BigDecimal onebigdec32 = new BigDecimal("1").setScale(4);
        BigDecimal onebigdec64 = new BigDecimal("1").setScale(6);
        BigDecimal onebigdec128 = new BigDecimal("1").setScale(20);

        appender.beginRow();
        appender.append(1);
        appender.appendBigDecimal(bigdec16);
        appender.appendBigDecimal(bigdec32);
        appender.appendBigDecimal(bigdec64);
        appender.appendBigDecimal(bigdec128);
        appender.endRow();
        appender.beginRow();
        appender.append(2);
        appender.appendBigDecimal(negbigdec16);
        appender.appendBigDecimal(negbigdec32);
        appender.appendBigDecimal(negbigdec64);
        appender.appendBigDecimal(negbigdec128);
        appender.endRow();
        appender.beginRow();
        appender.append(3);
        appender.appendBigDecimal(smallbigdec16);
        appender.appendBigDecimal(smallbigdec32);
        appender.appendBigDecimal(smallbigdec64);
        appender.appendBigDecimal(smallbigdec128);
        appender.endRow();
        appender.beginRow();
        appender.append(4);
        appender.appendBigDecimal(intbigdec16);
        appender.appendBigDecimal(intbigdec32);
        appender.appendBigDecimal(intbigdec64);
        appender.appendBigDecimal(intbigdec128);
        appender.endRow();
        appender.beginRow();
        appender.append(5);
        appender.appendBigDecimal(onebigdec16);
        appender.appendBigDecimal(onebigdec32);
        appender.appendBigDecimal(onebigdec64);
        appender.appendBigDecimal(onebigdec128);
        appender.endRow();
        appender.close();

        ResultSet rs = stmt.executeQuery("SELECT a,b,c,d FROM decimals ORDER BY id");
        assertFalse(rs.isClosed());
        assertTrue(rs.next());

        BigDecimal rs1 = (BigDecimal) rs.getObject(1, BigDecimal.class);
        BigDecimal rs2 = (BigDecimal) rs.getObject(2, BigDecimal.class);
        BigDecimal rs3 = (BigDecimal) rs.getObject(3, BigDecimal.class);
        BigDecimal rs4 = (BigDecimal) rs.getObject(4, BigDecimal.class);

        assertEquals(rs1, bigdec16);
        assertEquals(rs2, bigdec32);
        assertEquals(rs3, bigdec64);
        assertEquals(rs4, bigdec128);
        assertTrue(rs.next());

        BigDecimal nrs1 = (BigDecimal) rs.getObject(1, BigDecimal.class);
        BigDecimal nrs2 = (BigDecimal) rs.getObject(2, BigDecimal.class);
        BigDecimal nrs3 = (BigDecimal) rs.getObject(3, BigDecimal.class);
        BigDecimal nrs4 = (BigDecimal) rs.getObject(4, BigDecimal.class);

        assertEquals(nrs1, negbigdec16);
        assertEquals(nrs2, negbigdec32);
        assertEquals(nrs3, negbigdec64);
        assertEquals(nrs4, negbigdec128);
        assertTrue(rs.next());

        BigDecimal srs1 = (BigDecimal) rs.getObject(1, BigDecimal.class);
        BigDecimal srs2 = (BigDecimal) rs.getObject(2, BigDecimal.class);
        BigDecimal srs3 = (BigDecimal) rs.getObject(3, BigDecimal.class);
        BigDecimal srs4 = (BigDecimal) rs.getObject(4, BigDecimal.class);

        assertEquals(srs1, smallbigdec16);
        assertEquals(srs2, smallbigdec32);
        assertEquals(srs3, smallbigdec64);
        assertEquals(srs4, smallbigdec128);
        assertTrue(rs.next());

        BigDecimal irs1 = (BigDecimal) rs.getObject(1, BigDecimal.class);
        BigDecimal irs2 = (BigDecimal) rs.getObject(2, BigDecimal.class);
        BigDecimal irs3 = (BigDecimal) rs.getObject(3, BigDecimal.class);
        BigDecimal irs4 = (BigDecimal) rs.getObject(4, BigDecimal.class);

        assertEquals(irs1, intbigdec16);
        assertEquals(irs2, intbigdec32);
        assertEquals(irs3, intbigdec64);
        assertEquals(irs4, intbigdec128);
        assertTrue(rs.next());

        BigDecimal oners1 = (BigDecimal) rs.getObject(1, BigDecimal.class);
        BigDecimal oners2 = (BigDecimal) rs.getObject(2, BigDecimal.class);
        BigDecimal oners3 = (BigDecimal) rs.getObject(3, BigDecimal.class);
        BigDecimal oners4 = (BigDecimal) rs.getObject(4, BigDecimal.class);

        assertEquals(oners1, onebigdec16);
        assertEquals(oners2, onebigdec32);
        assertEquals(oners3, onebigdec64);
        assertEquals(oners4, onebigdec128);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_decimal_wrong_scale() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute(
            "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

        assertThrows(() -> {
            DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender.append(1);
            appender.beginRow();
            appender.appendBigDecimal(new BigDecimal("121.14").setScale(2));
        }, SQLException.class);

        assertThrows(() -> {
            DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender.beginRow();
            appender.append(2);
            appender.appendBigDecimal(new BigDecimal("21.1").setScale(2));
            appender.appendBigDecimal(new BigDecimal("12111.1411").setScale(4));
        }, SQLException.class);

        assertThrows(() -> {
            DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals");
            appender.beginRow();
            appender.append(3);
            appender.appendBigDecimal(new BigDecimal("21.1").setScale(2));
            appender.appendBigDecimal(new BigDecimal("21.1").setScale(4));
            appender.appendBigDecimal(new BigDecimal("1234567890123.123456").setScale(6));
        }, SQLException.class);

        stmt.close();
        conn.close();
    }

    public static void test_appender_int_string() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER, s VARCHAR)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        for (int i = 0; i < 1000; i++) {
            appender.beginRow();
            appender.append(i);
            appender.append("str " + i);
            appender.endRow();
        }
        appender.close();

        ResultSet rs = stmt.executeQuery("SELECT max(a), min(s) FROM data");
        assertFalse(rs.isClosed());

        assertTrue(rs.next());
        int resA = rs.getInt(1);
        assertEquals(resA, 999);
        String resB = rs.getString(2);
        assertEquals(resB, "str 0");

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_string_with_emoji() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (str_value VARCHAR(10))");
        String expectedValue = "䭔\uD86D\uDF7C🔥\uD83D\uDE1C";
        try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
            appender.beginRow();
            appender.append(expectedValue);
            appender.endRow();
        }

        ResultSet rs = stmt.executeQuery("SELECT str_value FROM data");
        assertFalse(rs.isClosed());
        assertTrue(rs.next());

        String appendedValue = rs.getString(1);
        assertEquals(appendedValue, expectedValue);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_table_does_not_exist() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        assertThrows(() -> {
            @SuppressWarnings("unused")
            DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
        }, SQLException.class);

        stmt.close();
        conn.close();
    }

    public static void test_appender_table_deleted() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        appender.beginRow();
        appender.append(1);
        appender.endRow();

        stmt.execute("DROP TABLE data");

        appender.beginRow();
        appender.append(2);
        appender.endRow();

        assertThrows(appender::close, SQLException.class);

        stmt.close();
        conn.close();
    }

    public static void test_appender_append_too_many_columns() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        stmt.close();
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        assertThrows(() -> {
            appender.beginRow();
            appender.append(1);
            appender.append(2);
        }, SQLException.class);

        conn.close();
    }

    public static void test_appender_append_too_few_columns() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
        stmt.close();
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        assertThrows(() -> {
            appender.beginRow();
            appender.append(1);
            appender.endRow();
        }, SQLException.class);

        conn.close();
    }

    public static void test_appender_type_mismatch() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        assertThrows(() -> {
            appender.beginRow();
            appender.append("str");
        }, SQLException.class);

        stmt.close();
        conn.close();
    }

    public static void test_appender_null_integer() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        appender.beginRow();

        // int foo = null won't compile
        // Integer foo = null will compile, but NPE on cast to int
        // So, use the String appender to pass an arbitrary null value
        appender.append((String) null);
        appender.endRow();
        appender.flush();
        appender.close();

        ResultSet results = stmt.executeQuery("SELECT * FROM data");
        assertTrue(results.next());
        // java.sql.ResultSet.getInt(int) returns 0 if the value is NULL
        assertEquals(0, results.getInt(1));
        assertTrue(results.wasNull());

        results.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_null_varchar() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a VARCHAR)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        appender.beginRow();
        appender.append((String) null);
        appender.endRow();
        appender.flush();
        appender.close();

        ResultSet results = stmt.executeQuery("SELECT * FROM data");
        assertTrue(results.next());
        assertNull(results.getString(1));
        assertTrue(results.wasNull());

        results.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_null_blob() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a BLOB)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

        appender.beginRow();
        appender.append((byte[]) null);
        appender.endRow();
        appender.flush();
        appender.close();

        ResultSet results = stmt.executeQuery("SELECT * FROM data");
        assertTrue(results.next());
        assertNull(results.getString(1));
        assertTrue(results.wasNull());

        results.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_roundtrip_blob() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a BLOB)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
        SecureRandom random = SecureRandom.getInstanceStrong();
        byte[] data = new byte[512];
        random.nextBytes(data);

        appender.beginRow();
        appender.append(data);
        appender.endRow();
        appender.flush();
        appender.close();

        ResultSet results = stmt.executeQuery("SELECT * FROM data");
        assertTrue(results.next());

        Blob resultBlob = results.getBlob(1);
        byte[] resultBytes = resultBlob.getBytes(1, (int) resultBlob.length());
        assertTrue(Arrays.equals(resultBytes, data), "byte[] data is round tripped untouched");

        results.close();
        stmt.close();
        conn.close();
    }

    public static void test_get_catalog() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        ResultSet rs = conn.getMetaData().getCatalogs();
        HashSet<String> set = new HashSet<String>();
        while (rs.next()) {
            set.add(rs.getString(1));
        }
        assertTrue(!set.isEmpty());
        rs.close();
        assertTrue(set.contains(conn.getCatalog()));
        conn.close();
    }

    public static void test_set_catalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {

            assertThrows(() -> conn.setCatalog("other"), SQLException.class);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH ':memory:' AS other;");
            }

            conn.setCatalog("other");
            assertEquals(conn.getCatalog(), "other");
        }
    }

    public static void test_get_table_types_bug1258() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE a1 (i INTEGER)");
        stmt.execute("CREATE TABLE a2 (i INTEGER)");
        stmt.execute("CREATE TEMPORARY TABLE b (i INTEGER)");
        stmt.execute("CREATE VIEW c AS SELECT * FROM a1");
        stmt.close();

        ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "b");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "c");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {});
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "b");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "c");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE"});
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE", "VIEW"});
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "c");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"XXXX"});
        assertFalse(rs.next());
        rs.close();

        conn.close();
    }

    public static void test_utf_string_bug1271() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT 'Mühleisen', '🦆', '🦄ྀི123456789'");
        assertEquals(rs.getMetaData().getColumnName(1), "'Mühleisen'");
        assertEquals(rs.getMetaData().getColumnName(2), "'🦆'");
        assertEquals(rs.getMetaData().getColumnName(3), "'🦄ྀི123456789'");

        assertTrue(rs.next());

        assertEquals(rs.getString(1), "Mühleisen");
        assertEquals(rs.getString(2), "🦆");
        assertEquals(rs.getString(3), "🦄ྀི123456789");

        rs.close();
        stmt.close();
        conn.close();
    }

    private static String blob_to_string(Blob b) throws SQLException {
        return new String(b.getBytes(1, (int) b.length()), StandardCharsets.US_ASCII);
    }

    public static void test_blob_bug1090() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        String test_str1 = "asdf";
        String test_str2 = "asdxxxxxxxxxxxxxxf";

        ResultSet rs =
            stmt.executeQuery("SELECT '" + test_str1 + "'::BLOB a, NULL::BLOB b, '" + test_str2 + "'::BLOB c");
        assertTrue(rs.next());

        assertTrue(test_str1.equals(blob_to_string(rs.getBlob(1))));
        assertTrue(test_str1.equals(blob_to_string(rs.getBlob("a"))));

        assertTrue(test_str2.equals(blob_to_string(rs.getBlob("c"))));

        rs.getBlob("a");
        assertFalse(rs.wasNull());

        rs.getBlob("b");
        assertTrue(rs.wasNull());

        assertEquals(blob_to_string(((Blob) rs.getObject(1))), test_str1);
        assertEquals(blob_to_string(((Blob) rs.getObject("a"))), test_str1);
        assertEquals(blob_to_string(((Blob) rs.getObject("c"))), test_str2);
        assertNull(rs.getObject(2));
        assertNull(rs.getObject("b"));

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_uuid() throws Exception {
        // Generated by DuckDB
        String testUuid = "a0a34a0a-1794-47b6-b45c-0ac68cc03702";

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT a, NULL::UUID b, a::VARCHAR c, '" + testUuid +
                                                    "'::UUID d FROM (SELECT uuid() a)")
                                      .unwrap(DuckDBResultSet.class)) {
            assertTrue(rs.next());

            // UUID direct
            UUID a = (UUID) rs.getObject(1);
            assertTrue(a != null);
            assertTrue(rs.getObject("a") instanceof UUID);
            assertFalse(rs.wasNull());

            // Null handling
            assertNull(rs.getObject(2));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("b"));
            assertTrue(rs.wasNull());

            // String interpreted as UUID in Java, rather than in DuckDB
            assertTrue(rs.getObject(3) instanceof String);
            assertEquals(rs.getUuid(3), a);
            assertFalse(rs.wasNull());

            // Verify UUID computation is correct
            assertEquals(rs.getObject(4), UUID.fromString(testUuid));
        }
    }

    public static void test_get_schema() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);

        assertEquals(conn.getSchema(), DuckDBConnection.DEFAULT_SCHEMA);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA alternate_schema;");
            stmt.execute("SET search_path = \"alternate_schema\";");
        }

        assertEquals(conn.getSchema(), "alternate_schema");

        conn.setSchema("main");
        assertEquals(conn.getSchema(), "main");

        conn.close();

        try {
            conn.getSchema();
            fail();
        } catch (SQLException e) {
            assertEquals(e.getMessage(), "Connection was closed");
        }
    }

    /**
     * @see {https://github.com/duckdb/duckdb/issues/3906}
     */
    public static void test_cached_row_set() throws Exception {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        rowSet.setUrl(JDBC_URL);
        rowSet.setCommand("select 1");
        rowSet.execute();

        rowSet.next();
        assertEquals(rowSet.getInt(1), 1);
    }

    public static void test_json() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select [1, 5]::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isArray());
            assertEquals(jsonNode.toString(), "[1,5]");
        }

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select '{\"key\": \"value\"}'::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isObject());
            assertEquals(jsonNode.toString(),
                         "{\"key\": \"value\"}"); // this isn't valid json output, must load json extension for that
        }

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select '\"hello\"'::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isString());
            assertEquals(jsonNode.toString(), "\"hello\"");
        }
    }

    public static void test_bug966_typeof() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select typeof(1);");

        rs.next();
        assertEquals(rs.getString(1), "INTEGER");
    }

    public static void test_config() throws Exception {
        String memory_limit = "memory_limit";
        String threads = "threads";

        Properties info = new Properties();
        info.put(memory_limit, "500MB");
        info.put(threads, "5");
        Connection conn = DriverManager.getConnection(JDBC_URL, info);

        assertEquals("476.8 MiB", getSetting(conn, memory_limit));
        assertEquals("5", getSetting(conn, threads));
    }

    public static void test_invalid_config() throws Exception {
        Properties info = new Properties();
        info.put("invalid config name", "true");

        String message = assertThrows(() -> DriverManager.getConnection(JDBC_URL, info), SQLException.class);

        assertTrue(message.contains("The following options were not recognized: invalid config name"));
    }

    public static void test_valid_but_local_config_throws_exception() throws Exception {
        Properties info = new Properties();
        info.put("ordered_aggregate_threshold", "123");

        String message = assertThrows(() -> DriverManager.getConnection(JDBC_URL, info), SQLException.class);

        assertTrue(message.contains("Could not set option \"ordered_aggregate_threshold\" as a global option"));
    }

    private static String getSetting(Connection conn, String settingName) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement("select value from duckdb_settings() where name = ?")) {
            stmt.setString(1, settingName);
            ResultSet rs = stmt.executeQuery();
            rs.next();

            return rs.getString(1);
        }
    }

    public static void test_describe() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TEST (COL INT DEFAULT 42)");

            ResultSet rs = stmt.executeQuery("DESCRIBE SELECT * FROM TEST");
            rs.next();
            assertEquals(rs.getString("column_name"), "COL");
            assertEquals(rs.getString("column_type"), "INTEGER");
            assertEquals(rs.getString("null"), "YES");
            assertNull(rs.getString("key"));
            assertEquals(rs.getString("default"), "42");
            assertNull(rs.getString("extra"));
        }
    }

    public static void test_null_bytes_in_string() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?::varchar")) {
                stmt.setObject(1, "bob\u0000r");
                ResultSet rs = stmt.executeQuery();

                rs.next();
                assertEquals(rs.getString(1), "bob\u0000r");
            }
        }
    }

    public static void test_get_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            ResultSet functions =
                conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "string_split");

            assertTrue(functions.next());
            assertNull(functions.getObject("FUNCTION_CAT"));
            assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
            assertEquals("string_split", functions.getString("FUNCTION_NAME"));
            assertEquals(DatabaseMetaData.functionNoTable, functions.getInt("FUNCTION_TYPE"));

            assertFalse(functions.next());

            // two items for two overloads?
            functions = conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "read_csv_auto");
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
            functions.close();

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

    public static void test_instance_cache() throws Exception {
        Path database_file = Files.createTempFile("duckdb-instance-cache-test-", ".duckdb");
        database_file.toFile().delete();

        String jdbc_url = JDBC_URL + database_file.toString();

        Connection conn = DriverManager.getConnection(jdbc_url);
        Connection conn2 = DriverManager.getConnection(jdbc_url);

        conn.close();
        conn2.close();
    }

    public static void test_user_password() throws Exception {
        String jdbc_url = JDBC_URL;
        Properties p = new Properties();
        p.setProperty("user", "wilbur");
        p.setProperty("password", "quack");
        Connection conn = DriverManager.getConnection(jdbc_url, p);
        conn.close();

        Properties p2 = new Properties();
        p2.setProperty("User", "wilbur");
        p2.setProperty("PASSWORD", "quack");
        Connection conn2 = DriverManager.getConnection(jdbc_url, p2);
        conn2.close();
    }

    public static void test_boolean_config() throws Exception {
        Properties config = new Properties();
        config.put("enable_external_access", false);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config);
             PreparedStatement stmt = conn.prepareStatement("SELECT current_setting('enable_external_access')");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            assertEquals("false", rs.getString(1));
        }
    }

    public static void test_autoloading_config() throws Exception {
        Properties config = new Properties();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config);
             PreparedStatement stmt = conn.prepareStatement("SELECT current_setting('autoload_known_extensions')");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            assertEquals("true", rs.getString(1));
        }
    }

    public static void test_autoinstall_config() throws Exception {
        Properties config = new Properties();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config);
             PreparedStatement stmt = conn.prepareStatement("SELECT current_setting('autoinstall_known_extensions')");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            assertEquals("true", rs.getString(1));
        }
    }

    public static void test_readonly_remains_bug5593() throws Exception {
        Path database_file = Files.createTempFile("duckdb-instance-cache-test-", ".duckdb");
        database_file.toFile().delete();
        String jdbc_url = JDBC_URL + database_file.toString();

        Properties p = new Properties();
        p.setProperty("duckdb.read_only", "true");
        try {
            Connection conn = DriverManager.getConnection(jdbc_url, p);
            conn.close();
        } catch (Exception e) {
            // nop
        }
        assertTrue(p.containsKey("duckdb.read_only"));
    }

    public static void test_supportsLikeEscapeClause_shouldBe_true() throws Exception {
        Connection connection = DriverManager.getConnection(JDBC_URL);
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        assertTrue(databaseMetaData.supportsLikeEscapeClause(),
                   "DatabaseMetaData.supportsLikeEscapeClause() should be true.");
    }

    public static void test_supports_catalogs_in_table_definitions() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String IS_TablesQuery = "SELECT * FROM information_schema.tables " +
                                      String.format("WHERE table_catalog = '%s' ", CATALOG_NAME) +
                                      String.format("AND table_name = '%s'", TABLE_NAME);
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;
        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection(JDBC_URL);
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));

            final boolean supportsCatalogsInTableDefinitions = databaseMetaData.supportsCatalogsInTableDefinitions();
            try {
                statement.execute(String.format("CREATE TABLE %s (id int)", QUALIFIED_TABLE_NAME));
            } catch (SQLException ex) {
                if (supportsCatalogsInTableDefinitions) {
                    fail(
                        "supportsCatalogsInTableDefinitions is true but CREATE TABLE in attached database is not allowed. " +
                        ex.getMessage());
                    ex.printStackTrace();
                }
            }
            resultSet = statement.executeQuery(IS_TablesQuery);
            assertTrue(resultSet.next(), "Expected exactly 1 row from information_schema.tables, got 0");
            assertFalse(resultSet.next());
            resultSet.close();

            try {
                statement.execute(String.format("DROP TABLE %s", QUALIFIED_TABLE_NAME));
            } catch (SQLException ex) {
                if (supportsCatalogsInTableDefinitions) {
                    fail(
                        "supportsCatalogsInTableDefinitions is true but DROP TABLE in attached database is not allowed. " +
                        ex.getMessage());
                    ex.printStackTrace();
                }
            }
            resultSet = statement.executeQuery(IS_TablesQuery);
            assertTrue(resultSet.next() == false, "Expected exactly 0 rows from information_schema.tables, got > 0");
            resultSet.close();

            assertTrue(supportsCatalogsInTableDefinitions, "supportsCatalogsInTableDefinitions should return true.");
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

    public static void test_structs() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             PreparedStatement statement = connection.prepareStatement("select {\"a\": 1}")) {
            ResultSet resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            Struct struct = (Struct) resultSet.getObject(1);
            assertEquals(toJavaObject(struct), mapOf("a", 1));
            assertEquals(struct.getSQLTypeName(), "STRUCT(a INTEGER)");

            String definition = "STRUCT(i INTEGER, j INTEGER)";
            String typeName = "POINT";
            try (PreparedStatement stmt =
                     connection.prepareStatement("CREATE TYPE " + typeName + " AS " + definition)) {
                stmt.execute();
            }

            testStruct(connection, connection.createStruct(definition, new Object[] {1, 2}));
            testStruct(connection, connection.createStruct(typeName, new Object[] {1, 2}));
        }
    }

    public static void test_struct_with_timestamp() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
            LocalDateTime now = LocalDateTime.of(LocalDate.of(2020, 5, 12), LocalTime.of(16, 20, 0, 0));
            Struct struct1 = connection.createStruct("STRUCT(start TIMESTAMP)", new Object[] {now});

            try (PreparedStatement stmt = connection.prepareStatement("SELECT ?")) {
                stmt.setObject(1, struct1);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());

                    Struct result = (Struct) rs.getObject(1);

                    assertEquals(Timestamp.valueOf(now), result.getAttributes()[0]);
                }
            }
        }
    }

    public static void test_struct_with_bad_type() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
            Struct struct1 = connection.createStruct("BAD TYPE NAME", new Object[0]);

            try (PreparedStatement stmt = connection.prepareStatement("SELECT ?")) {
                stmt.setObject(1, struct1);
                String message = assertThrows(stmt::executeQuery, SQLException.class);
                String expected = "Invalid Input Error: Value \"BAD TYPE NAME\" can not be converted to a DuckDB Type.";
                assertTrue(
                    message.contains(expected),
                    String.format("The message \"%s\" does not contain the expected string \"%s\"", message, expected));
            }
        }
    }

    private static void testStruct(Connection connection, Struct struct) throws SQLException, Exception {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT ?")) {
            stmt.setObject(1, struct);

            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());

                Struct result = (Struct) rs.getObject(1);

                assertEquals(Arrays.asList(1, 2), Arrays.asList(result.getAttributes()));
            }
        }
    }

    public static void test_write_map() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test (thing MAP(string, integer));");
            }
            Map<Object, Object> map = mapOf("hello", 42);
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test VALUES (?)")) {
                stmt.setObject(1, conn.createMap("MAP(string, integer)", map));
                assertEquals(stmt.executeUpdate(), 1);
            }
            try (PreparedStatement stmt = conn.prepareStatement("FROM test"); ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    assertEquals(rs.getObject(1), map);
                }
            }
        }
    }

    public static void test_union() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE tbl1(u UNION(num INT, str VARCHAR));");
            statement.execute("INSERT INTO tbl1 values (1) , ('two') , (union_value(str := 'three'));");

            ResultSet rs = statement.executeQuery("select * from tbl1");
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), 1);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), "two");
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), "three");
        }
    }

    public static void test_list() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("select [1]")) {
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), singletonList(1));
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[1], [42, 69]])")) {
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), singletonList(1));
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), asList(42, 69));
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[[42], [69]]])")) {
                assertTrue(rs.next());

                List<List<Integer>> expected = asList(singletonList(42), singletonList(69));
                List<Array> actual = arrayToList(rs.getArray(1));

                for (int i = 0; i < actual.size(); i++) {
                    assertEquals(actual.get(i), expected.get(i));
                }
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[], [69]])")) {
                assertTrue(rs.next());
                assertTrue(arrayToList(rs.getArray(1)).isEmpty());
            }

            try (ResultSet rs = statement.executeQuery("SELECT [0.0]::DECIMAL[]")) {
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), singletonList(new BigDecimal("0.000")));
            }
            try (PreparedStatement stmt = connection.prepareStatement("select ?")) {
                Array array = connection.createArrayOf("INTEGER", new Object[] {1});

                stmt.setObject(1, array);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(singletonList(1), arrayToList(rs.getArray(1)));
                }
            }
        }
    }

    public static void test_array_resultset() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("select [42, 69]")) {
                assertTrue(rs.next());
                ResultSet arrayResultSet = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 1);
                assertEquals(arrayResultSet.getInt("index"), 1);
                assertEquals(arrayResultSet.getInt("Index"), 1);
                assertEquals(arrayResultSet.getInt("INDEX"), 1);
                assertEquals(arrayResultSet.getByte(2), (byte) 42);
                assertEquals(arrayResultSet.getShort(2), (short) 42);
                assertEquals(arrayResultSet.getInt(2), 42);
                assertEquals(arrayResultSet.getLong(2), (long) 42);
                assertEquals(arrayResultSet.getFloat(2), (float) 42);
                assertEquals(arrayResultSet.getDouble(2), (double) 42);
                assertEquals(arrayResultSet.getBigDecimal(2), BigDecimal.valueOf(42));
                assertEquals(arrayResultSet.getInt("value"), 42);
                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 2);
                assertEquals(arrayResultSet.getInt(2), 69);
                assertFalse(arrayResultSet.next());
            }

            try (ResultSet rs = statement.executeQuery("select unnest([[[], [69]]])")) {
                assertTrue(rs.next());
                ResultSet arrayResultSet = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 1);
                {
                    Array subArray = arrayResultSet.getArray(2);
                    assertNotNull(subArray);
                    ResultSet subArrayResultSet = subArray.getResultSet();
                    assertFalse(subArrayResultSet.next()); // empty array
                }
                {
                    Array subArray = arrayResultSet.getArray("value");
                    assertNotNull(subArray);
                    ResultSet subArrayResultSet = subArray.getResultSet();
                    assertFalse(subArrayResultSet.next()); // empty array
                }

                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 2);
                Array subArray2 = arrayResultSet.getArray(2);
                assertNotNull(subArray2);
                ResultSet subArrayResultSet2 = subArray2.getResultSet();
                assertTrue(subArrayResultSet2.next());

                assertEquals(subArrayResultSet2.getInt(1), 1);
                assertEquals(subArrayResultSet2.getInt(2), 69);
                assertFalse(arrayResultSet.next());
            }

            try (ResultSet rs = statement.executeQuery("select [42, 69]")) {
                assertFalse(rs.isClosed());
                rs.close();
                assertTrue(rs.isClosed());
            }

            try (ResultSet rs = statement.executeQuery("select ['life', null, 'universe']")) {
                assertTrue(rs.next());

                ResultSet arrayResultSet = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet.isBeforeFirst());
                assertTrue(arrayResultSet.next());
                assertFalse(arrayResultSet.isBeforeFirst());
                assertEquals(arrayResultSet.getInt(1), 1);
                assertEquals(arrayResultSet.getString(2), "life");
                assertFalse(arrayResultSet.wasNull());

                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 2);
                assertFalse(arrayResultSet.wasNull());
                assertEquals(arrayResultSet.getObject(2), null);
                assertTrue(arrayResultSet.wasNull());

                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getInt(1), 3);
                assertFalse(arrayResultSet.wasNull());
                assertEquals(arrayResultSet.getString(2), "universe");
                assertFalse(arrayResultSet.wasNull());

                assertFalse(arrayResultSet.isBeforeFirst());
                assertFalse(arrayResultSet.isAfterLast());
                assertFalse(arrayResultSet.next());
                assertTrue(arrayResultSet.isAfterLast());

                arrayResultSet.first();
                assertEquals(arrayResultSet.getString(2), "life");
                assertTrue(arrayResultSet.isFirst());

                arrayResultSet.last();
                assertEquals(arrayResultSet.getString(2), "universe");
                assertTrue(arrayResultSet.isLast());

                assertFalse(arrayResultSet.next());
                assertTrue(arrayResultSet.isAfterLast());

                arrayResultSet.next(); // try to move past the end
                assertTrue(arrayResultSet.isAfterLast());

                arrayResultSet.relative(-1);
                assertEquals(arrayResultSet.getString(2), "universe");
            }

            try (ResultSet rs = statement.executeQuery("select UNNEST([[42], [69]])")) {
                assertTrue(rs.next());
                ResultSet arrayResultSet = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet.next());

                assertEquals(arrayResultSet.getInt(1), 1);
                assertEquals(arrayResultSet.getInt(2), 42);
                assertFalse(arrayResultSet.next());

                assertTrue(rs.next());
                ResultSet arrayResultSet2 = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet2.next());
                assertEquals(arrayResultSet2.getInt(1), 1);
                assertEquals(arrayResultSet2.getInt(2), 69);
                assertFalse(arrayResultSet2.next());
            }

            try (ResultSet rs = statement.executeQuery("select [" + Integer.MAX_VALUE + "::BIGINT + 1]")) {
                assertTrue(rs.next());
                ResultSet arrayResultSet = rs.getArray(1).getResultSet();
                assertTrue(arrayResultSet.next());
                assertEquals(arrayResultSet.getLong(2), ((long) Integer.MAX_VALUE) + 1);
            }
        }
    }

    private static <T> List<T> arrayToList(Array array) throws SQLException {
        return arrayToList((T[]) array.getArray());
    }

    private static <T> List<T> arrayToList(T[] array) throws SQLException {
        List<T> out = new ArrayList<>();
        for (Object t : array) {
            out.add((T) toJavaObject(t));
        }
        return out;
    }

    private static <T> T toJavaObject(Object t) {
        try {
            if (t instanceof Array) {
                t = arrayToList((Array) t);
            } else if (t instanceof Struct) {
                t = structToMap((DuckDBStruct) t);
            }
            return (T) t;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void test_map() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             PreparedStatement statement = connection.prepareStatement("select map([100, 5], ['a', 'b'])")) {
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), mapOf(100, "a", 5, "b"));
        }
    }

    public static void test_getColumnClassName() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement();) {
            try (ResultSet rs = s.executeQuery("select * from test_all_types()")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                rs.next();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    Object value = rs.getObject(i);

                    assertEquals(rsmd.getColumnClassName(i), value.getClass().getName());
                }
            }
        }
    }

    public static void test_get_result_set() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement p = conn.prepareStatement("select 1")) {
                p.executeQuery();
                try (ResultSet resultSet = p.getResultSet()) {
                    assertNotNull(resultSet);
                }
                assertNull(p.getResultSet()); // returns null after initial call
            }

            try (Statement s = conn.createStatement()) {
                s.execute("select 1");
                try (ResultSet resultSet = s.getResultSet()) {
                    assertNotNull(resultSet);
                }
                assertFalse(s.getMoreResults());
                assertNull(s.getResultSet()); // returns null after initial call
            }
        }
    }

    static List<Object> trio(Object... max) {
        return asList(emptyList(), asList(max), null);
    }

    static DuckDBResultSet.DuckDBBlobResult blobOf(String source) {
        return new DuckDBResultSet.DuckDBBlobResult(ByteBuffer.wrap(source.getBytes()));
    }

    private static final DateTimeFormatter FORMAT_DATE = new DateTimeFormatterBuilder()
                                                             .parseCaseInsensitive()
                                                             .appendValue(YEAR_OF_ERA)
                                                             .appendLiteral('-')
                                                             .appendValue(MONTH_OF_YEAR, 2)
                                                             .appendLiteral('-')
                                                             .appendValue(DAY_OF_MONTH, 2)
                                                             .toFormatter()
                                                             .withResolverStyle(ResolverStyle.LENIENT);
    public static final DateTimeFormatter FORMAT_DATETIME = new DateTimeFormatterBuilder()
                                                                .append(FORMAT_DATE)
                                                                .appendLiteral('T')
                                                                .append(ISO_LOCAL_TIME)
                                                                .toFormatter()
                                                                .withResolverStyle(ResolverStyle.LENIENT);
    public static final DateTimeFormatter FORMAT_TZ = new DateTimeFormatterBuilder()
                                                          .append(FORMAT_DATETIME)
                                                          .appendLiteral('+')
                                                          .appendValue(OFFSET_SECONDS)
                                                          .toFormatter()
                                                          .withResolverStyle(ResolverStyle.LENIENT);

    static <K, V> Map<K, V> mapOf(Object... pairs) {
        Map<K, V> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length - 1; i += 2) {
            result.put((K) pairs[i], (V) pairs[i + 1]);
        }
        return result;
    }

    private static Timestamp microsToTimestampNoThrow(long micros) {
        try {
            LocalDateTime ldt = localDateTimeFromTimestamp(micros, ChronoUnit.MICROS, null);
            return Timestamp.valueOf(ldt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static OffsetDateTime localDateTimeToOffset(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        ZoneId systemZone = ZoneId.systemDefault();
        ZoneOffset zoneOffset = systemZone.getRules().getOffset(instant);
        return ldt.atOffset(zoneOffset);
    }

    static Map<String, List<Object>> correct_answer_map = new HashMap<>();
    static final TimeZone ALL_TYPES_TIME_ZONE = TimeZone.getTimeZone("America/New_York");
    static {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(ALL_TYPES_TIME_ZONE);
        correct_answer_map.put("int_array", trio(42, 999, null, null, -42));
        correct_answer_map.put("double_array",
                               trio(42.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null, -42.0));
        correct_answer_map.put(
            "date_array", trio(LocalDate.parse("1970-01-01"), LocalDate.parse("5881580-07-11", FORMAT_DATE),
                               LocalDate.parse("-5877641-06-24", FORMAT_DATE), null, LocalDate.parse("2022-05-12")));
        correct_answer_map.put("timestamp_array", trio(Timestamp.valueOf("1970-01-01 00:00:00.0"),
                                                       microsToTimestampNoThrow(9223372036854775807L),
                                                       microsToTimestampNoThrow(-9223372036854775807L), null,
                                                       Timestamp.valueOf("2022-05-12 16:23:45.0")));
        correct_answer_map.put("timestamptz_array",
                               trio(localDateTimeToOffset(LocalDateTime.ofInstant(Instant.parse("1970-01-01T00:00:00Z"),
                                                                                  ZoneId.systemDefault())),
                                    localDateTimeToOffset(LocalDateTime.ofInstant(
                                        Instant.parse("+294247-01-10T04:00:54.775807Z"), ZoneId.systemDefault())),
                                    localDateTimeToOffset(LocalDateTime.ofInstant(
                                        Instant.parse("-290308-12-21T19:59:06.224193Z"), ZoneId.systemDefault())),
                                    null,
                                    localDateTimeToOffset(LocalDateTime.ofInstant(Instant.parse("2022-05-12T23:23:45Z"),
                                                                                  ZoneId.systemDefault()))));
        correct_answer_map.put("varchar_array", trio("🦆🦆🦆🦆🦆🦆", "goose", null, ""));
        List<Integer> numbers = asList(42, 999, null, null, -42);
        correct_answer_map.put("nested_int_array", trio(emptyList(), numbers, null, emptyList(), numbers));
        Map<Object, Object> abnull = mapOf("a", null, "b", null);
        correct_answer_map.put(
            "struct_of_arrays",
            asList(abnull, mapOf("a", numbers, "b", asList("🦆🦆🦆🦆🦆🦆", "goose", null, "")), null));
        Map<Object, Object> ducks = mapOf("a", 42, "b", "🦆🦆🦆🦆🦆🦆");
        correct_answer_map.put("array_of_structs", trio(abnull, ducks, null));
        correct_answer_map.put("bool", asList(false, true, null));
        correct_answer_map.put("tinyint", asList((byte) -128, (byte) 127, null));
        correct_answer_map.put("smallint", asList((short) -32768, (short) 32767, null));
        correct_answer_map.put("int", asList(-2147483648, 2147483647, null));
        correct_answer_map.put("bigint", asList(-9223372036854775808L, 9223372036854775807L, null));
        correct_answer_map.put("hugeint", asList(new BigInteger("-170141183460469231731687303715884105728"),
                                                 new BigInteger("170141183460469231731687303715884105727"), null));
        correct_answer_map.put(
            "uhugeint", asList(new BigInteger("0"), new BigInteger("340282366920938463463374607431768211455"), null));
        correct_answer_map.put("utinyint", asList((short) 0, (short) 255, null));
        correct_answer_map.put("usmallint", asList(0, 65535, null));
        correct_answer_map.put("uint", asList(0L, 4294967295L, null));
        correct_answer_map.put("ubigint", asList(BigInteger.ZERO, new BigInteger("18446744073709551615"), null));
        correct_answer_map.put(
            "varint",
            asList(
                "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368",
                "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368",
                null));
        correct_answer_map.put("time", asList(LocalTime.of(0, 0), LocalTime.parse("23:59:59.999999"), null));
        correct_answer_map.put("float", asList(-3.4028234663852886e+38f, 3.4028234663852886e+38f, null));
        correct_answer_map.put("double", asList(-1.7976931348623157e+308d, 1.7976931348623157e+308d, null));
        correct_answer_map.put("dec_4_1", asList(new BigDecimal("-999.9"), (new BigDecimal("999.9")), null));
        correct_answer_map.put("dec_9_4", asList(new BigDecimal("-99999.9999"), (new BigDecimal("99999.9999")), null));
        correct_answer_map.put(
            "dec_18_6", asList(new BigDecimal("-999999999999.999999"), (new BigDecimal("999999999999.999999")), null));
        correct_answer_map.put("dec38_10", asList(new BigDecimal("-9999999999999999999999999999.9999999999"),
                                                  (new BigDecimal("9999999999999999999999999999.9999999999")), null));
        correct_answer_map.put("uuid", asList(UUID.fromString("00000000-0000-0000-0000-000000000000"),
                                              (UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")), null));
        correct_answer_map.put("varchar", asList("🦆🦆🦆🦆🦆🦆", "goo\u0000se", null));
        correct_answer_map.put("json", asList("🦆🦆🦆🦆🦆", "goose", null));
        correct_answer_map.put(
            "blob", asList(blobOf("thisisalongblob\u0000withnullbytes"), blobOf("\u0000\u0000\u0000a"), null));
        correct_answer_map.put("bit", asList("0010001001011100010101011010111", "10101", null));
        correct_answer_map.put("small_enum", asList("DUCK_DUCK_ENUM", "GOOSE", null));
        correct_answer_map.put("medium_enum", asList("enum_0", "enum_299", null));
        correct_answer_map.put("large_enum", asList("enum_0", "enum_69999", null));
        correct_answer_map.put("struct", asList(abnull, ducks, null));
        correct_answer_map.put("map",
                               asList(mapOf(), mapOf("key1", "🦆🦆🦆🦆🦆🦆", "key2", "goose"), null));
        correct_answer_map.put("union", asList("Frank", (short) 5, null));
        correct_answer_map.put(
            "time_tz", asList(OffsetTime.parse("00:00+15:59:59"), OffsetTime.parse("23:59:59.999999-15:59:59"), null));
        correct_answer_map.put("interval", asList("00:00:00", "83 years 3 months 999 days 00:16:39.999999", null));
        correct_answer_map.put("timestamp", asList(microsToTimestampNoThrow(-9223372022400000000L),
                                                   microsToTimestampNoThrow(9223372036854775806L), null));
        correct_answer_map.put("date", asList(LocalDate.of(-5877641, 6, 25), LocalDate.of(5881580, 7, 10), null));
        correct_answer_map.put("timestamp_s",
                               asList(Timestamp.valueOf(LocalDateTime.of(-290308, 12, 22, 0, 0)),
                                      Timestamp.valueOf(LocalDateTime.of(294247, 1, 10, 4, 0, 54)), null));
        correct_answer_map.put("timestamp_ns",
                               asList(Timestamp.valueOf(LocalDateTime.parse("1677-09-22T00:00:00.0")),
                                      Timestamp.valueOf(LocalDateTime.parse("2262-04-11T23:47:16.854775806")), null));
        correct_answer_map.put("timestamp_ms",
                               asList(Timestamp.valueOf(LocalDateTime.of(-290308, 12, 22, 0, 0, 0)),
                                      Timestamp.valueOf(LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775000000)), null));
        correct_answer_map.put("timestamp_tz",
                               asList(localDateTimeToOffset(LocalDateTime.ofInstant(
                                          Instant.parse("-290308-12-21T19:03:58.00Z"), ZoneOffset.UTC)),
                                      localDateTimeToOffset(LocalDateTime.ofInstant(
                                          Instant.parse("+294247-01-09T23:00:54.775806Z"), ZoneOffset.UTC)),
                                      null));

        List<Integer> int_array = asList(null, 2, 3);
        List<String> varchar_array = asList("a", null, "c");
        List<Integer> int_list = asList(4, 5, 6);
        List<String> def = asList("d", "e", "f");

        correct_answer_map.put("fixed_int_array", asList(int_array, int_list, null));
        correct_answer_map.put("fixed_varchar_array", asList(varchar_array, def, null));
        correct_answer_map.put("fixed_nested_int_array",
                               asList(asList(int_array, null, int_array), asList(int_list, int_array, int_list), null));
        correct_answer_map.put("fixed_nested_varchar_array", asList(asList(varchar_array, null, varchar_array),
                                                                    asList(def, varchar_array, def), null));

        correct_answer_map.put("fixed_struct_array",
                               asList(asList(abnull, ducks, abnull), asList(ducks, abnull, ducks), null));

        correct_answer_map.put("struct_of_fixed_array",
                               asList(mapOf("a", int_array, "b", varchar_array), mapOf("a", int_list, "b", def), null));

        correct_answer_map.put("fixed_array_of_int_list", asList(asList(emptyList(), numbers, emptyList()),
                                                                 asList(numbers, emptyList(), numbers), null));

        correct_answer_map.put("list_of_fixed_int_array", asList(asList(int_array, int_list, int_array),
                                                                 asList(int_list, int_array, int_list), null));
        TimeZone.setDefault(defaultTimeZone);
    }

    public static void test_all_types() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(ALL_TYPES_TIME_ZONE);
        try {
            Logger logger = Logger.getAnonymousLogger();
            String sql =
                "select * EXCLUDE(time, time_tz)"
                + "\n    , CASE WHEN time = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE time END AS time"
                +
                "\n    , CASE WHEN time_tz = '24:00:00-15:59:59'::TIMETZ THEN '23:59:59.999999-15:59:59'::TIMETZ ELSE time_tz END AS time_tz"
                + "\nfrom test_all_types()";

            try (Connection conn = DriverManager.getConnection(JDBC_URL);
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                conn.createStatement().execute("set timezone = 'UTC'");

                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();

                    int rowIdx = 0;
                    while (rs.next()) {
                        for (int i = 0; i < metaData.getColumnCount(); i++) {
                            String columnName = metaData.getColumnName(i + 1);
                            List<Object> answers = correct_answer_map.get(columnName);
                            assertTrue(answers != null,
                                       "correct_answer_map lacks value for column: [" + columnName + "]");
                            Object expected = answers.get(rowIdx);

                            Object actual = toJavaObject(rs.getObject(i + 1));

                            String msg =
                                "test_all_types error, columnName: [" + columnName + "], rowIdx: [" + rowIdx + "]";
                            if (actual instanceof List) {
                                assertListsEqual((List) actual, (List) expected, msg);
                            } else {
                                assertEquals(actual, expected, msg);
                            }
                        }
                        rowIdx++;
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    private static Map<String, Object> structToMap(DuckDBStruct actual) throws SQLException {
        Map<String, Object> map = actual.getMap();
        Map<String, Object> result = new HashMap<>();
        map.forEach((key, value) -> result.put(key, toJavaObject(value)));
        return result;
    }

    public static void test_cancel() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(1);
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            Future<String> thread = service.submit(
                ()
                    -> assertThrows(()
                                        -> stmt.execute("select count(*) from range(10000000) t1, range(1000000) t2;"),
                                    SQLException.class));
            Thread.sleep(500); // wait for query to start running
            stmt.cancel();
            String message = thread.get(1, TimeUnit.SECONDS);
            assertEquals(message, "INTERRUPT Error: Interrupted!");
        }
    }

    public static void test_lots_of_races() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
            ExecutorService executorService = Executors.newFixedThreadPool(10);

            List<Callable<Object>> tasks = Collections.nCopies(1000, () -> {
                try {
                    try (PreparedStatement ps = connection.prepareStatement(
                             "SELECT count(*) FROM information_schema.tables WHERE table_name = 'test' LIMIT 1;")) {
                        ps.execute();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            List<Future<Object>> results = executorService.invokeAll(tasks);

            try {
                for (Future<Object> future : results) {
                    future.get();
                }
            } catch (java.util.concurrent.ExecutionException ee) {
                assertEquals(
                    ee.getCause().getCause().getMessage(),
                    "Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result");
            }
        }
    }

    public static void test_offset_limit() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement s = connection.createStatement()) {
            s.executeUpdate("create table t (i int not null)");
            s.executeUpdate("insert into t values (1), (1), (2), (3), (3), (3)");

            try (PreparedStatement ps =
                     connection.prepareStatement("select t.i from t order by t.i limit ? offset ?")) {
                ps.setLong(1, 2);
                ps.setLong(2, 1);

                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_UUID_binding() throws Exception {
        UUID uuid = UUID.fromString("7f649593-934e-4945-9bd6-9a554f25b573");
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                // UUID is parsed from string by UUID::FromString
                try (ResultSet rs = stmt.executeQuery("SELECT '" + uuid + "'::UUID")) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    assertEquals(uuid, obj);
                }
            }
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                // UUID is passed as 2 longs in JDBC ToValue
                stmt.setObject(1, uuid);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    assertEquals(uuid, obj);
                }
            }
        }
    }

    public static void test_struct_use_after_free() throws Exception {
        Object struct, array;
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT struct_pack(hello := 2), [42]");
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            struct = rs.getObject(1);
            array = rs.getObject(2);
        }
        assertEquals(struct.toString(), "{hello=2}");
        assertEquals(array.toString(), "[42]");
    }

    public static void test_user_agent_default() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            assertEquals(getSetting(conn, "custom_user_agent"), "");

            try (PreparedStatement stmt1 = conn.prepareStatement("PRAGMA user_agent");
                 ResultSet rs = stmt1.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).matches("duckdb/.*(.*) jdbc"));
            }
        }
    }

    public static void test_user_agent_custom() throws Exception {
        Properties props = new Properties();
        props.setProperty(DUCKDB_USER_AGENT_PROPERTY, "CUSTOM_STRING");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
            assertEquals(getSetting(conn, "custom_user_agent"), "CUSTOM_STRING");

            try (PreparedStatement stmt1 = conn.prepareStatement("PRAGMA user_agent");
                 ResultSet rs = stmt1.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).matches("duckdb/.*(.*) jdbc CUSTOM_STRING"));
            }
        }
    }

    public static void test_get_binary_stream() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement s = connection.prepareStatement("select ?")) {
            s.setObject(1, "YWJj".getBytes());
            String out = null;

            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    out = blob_to_string(rs.getBlob(1));
                }
            }

            assertEquals(out, "YWJj");
        }
    }

    public static void test_get_bytes() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement s = connection.prepareStatement("select ?")) {

            byte[] allTheBytes = new byte[256];
            for (int b = -128; b <= 127; b++) {
                allTheBytes[b + 128] = (byte) b;
            }

            // Test both all the possible bytes and with an empty array.
            byte[][] arrays = new byte[][] {allTheBytes, {}};

            for (byte[] array : arrays) {
                s.setBytes(1, array);

                int rowsReturned = 0;
                try (ResultSet rs = s.executeQuery()) {
                    assertTrue(rs instanceof DuckDBResultSet);
                    while (rs.next()) {
                        rowsReturned++;
                        byte[] result = rs.getBytes(1);
                        assertEquals(array, result, "Bytes were not the same after round trip.");
                    }
                }
                assertEquals(1, rowsReturned, "Got unexpected number of rows back.");
            }
        }
    }

    public static void test_set_streams() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             PreparedStatement ps = connection.prepareStatement("select ?::VARCHAR")) {

            String helloEn = "Hello";
            ps.setAsciiStream(1, new ByteArrayInputStream(helloEn.getBytes(US_ASCII)), 4);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(helloEn.substring(0, 4), rs.getString(1));
            }

            String helloBg = "\u0417\u0434\u0440\u0430\u0432\u0435\u0439\u0442\u0435";
            ps.setCharacterStream(1, new StringReader(helloBg), 7);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(helloBg.substring(0, 7), rs.getString(1));
            }
        }
    }

    public static void test_case_insensitivity() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE someTable (lowercase INT, mixedCASE INT, UPPERCASE INT)");
                s.execute("INSERT INTO someTable VALUES (0, 1, 2)");
            }

            String[] tableNameVariations = new String[] {"sometable", "someTable", "SOMETABLE"};
            String[][] columnNameVariations = new String[][] {{"lowercase", "mixedcase", "uppercase"},
                                                              {"lowerCASE", "mixedCASE", "upperCASE"},
                                                              {"LOWERCASE", "MIXEDCASE", "UPPERCASE"}};

            int totalTestsRun = 0;

            // Test every combination of upper, lower and mixedcase column and table names.
            for (String tableName : tableNameVariations) {
                for (int columnVariation = 0; columnVariation < columnNameVariations.length; columnVariation++) {
                    try (Statement s = connection.createStatement()) {
                        String query = String.format("SELECT %s, %s, %s from %s;", columnNameVariations[0][0],
                                                     columnNameVariations[0][1], columnNameVariations[0][2], tableName);

                        ResultSet resultSet = s.executeQuery(query);
                        assertTrue(resultSet.next());
                        for (int i = 0; i < columnNameVariations[0].length; i++) {
                            assertEquals(resultSet.getInt(columnNameVariations[columnVariation][i]), i,
                                         "Query " + query + " did not get correct result back for column number " + i);
                            totalTestsRun++;
                        }
                    }
                }
            }

            assertEquals(totalTestsRun,
                         tableNameVariations.length * columnNameVariations.length * columnNameVariations[0].length,
                         "Number of test cases actually run did not match number expected to be run.");
        }
    }

    public static void test_fractional_time() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT '01:02:03.123'::TIME");
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(rs.getTime(1), Time.valueOf(LocalTime.of(1, 2, 3, 123)));
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

    public static void test_blob_after_rs_next() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 'AAAA'::BLOB;")) {
                    Blob blob = null;
                    while (rs.next()) {
                        blob = rs.getBlob(1);
                    }
                    assertEquals(blob_to_string(blob), "AAAA");
                }
            }
        }
    }

    public static void test_typed_connection_properties() throws Exception {
        Properties config = new Properties();
        config.put("autoinstall_known_extensions", false); // BOOLEAN
        List<String> allowedDirsList = new ArrayList<>();
        allowedDirsList.add("path/to/dir1");
        allowedDirsList.add("path/to/dir2");
        config.put("allowed_directories", allowedDirsList); // VARCHAR[]
        config.put("catalog_error_max_schemas", 42);        // UBIGINT
        config.put("index_scan_percentage", 0.042);         // DOUBLE

        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('autoinstall_known_extensions')")) {
                    rs.next();
                    boolean val = rs.getBoolean(1);
                    assertFalse(val, "autoinstall_known_extensions");
                }
                try (ResultSet rs = stmt.executeQuery("SELECT UNNEST(current_setting('allowed_directories'))")) {
                    List<String> values = new ArrayList<>();
                    while (rs.next()) {
                        String val = rs.getString(1);
                        values.add(val);
                    }
                    assertTrue(values.size() >= 2);
                    boolean dir1Found = false;
                    boolean dir2Found = false;
                    for (String val : values) {
                        if (val.contains("dir1")) {
                            dir1Found = true;
                        }
                        if (val.contains("dir2")) {
                            dir2Found = true;
                        }
                    }
                    assertTrue(dir1Found && dir2Found, "allowed_directories 1");
                }
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('catalog_error_max_schemas')")) {
                    rs.next();
                    long val = rs.getLong(1);
                    assertEquals(val, 42l, "catalog_error_max_schemas");
                }
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('index_scan_percentage')")) {
                    rs.next();
                    double val = rs.getDouble(1);
                    assertEquals(val, 0.042d, "index_scan_percentage");
                }
            }
        }
    }

    public static void test_client_config_retained_on_dup() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt1 = conn.createStatement()) {
                stmt1.execute("set home_directory='test1'");
                try (ResultSet rs = stmt1.executeQuery("select current_setting('home_directory')")) {
                    rs.next();
                    assertEquals(rs.getString(1), "test1");
                }
            }
            try (Connection dup = ((DuckDBConnection) conn).duplicate(); Statement stmt2 = dup.createStatement();
                 ResultSet rs = stmt2.executeQuery("select current_setting('home_directory')")) {
                rs.next();
                assertEquals(rs.getString(1), "test1");
            }
        }
    }

    public static void test_empty_typemap_allowed() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Map<String, Class<?>> defaultMap = conn.getTypeMap();
            assertEquals(defaultMap.size(), 0);
            // check empty not throws
            conn.setTypeMap(new HashMap<>());
            // check custom map throws
            Map<String, Class<?>> customMap = new HashMap<>();
            customMap.put("foo", TestDuckDBJDBC.class);
            assertThrows(() -> { conn.setTypeMap(customMap); }, SQLException.class);
        }
    }

    public static void test_spark_path_option_ignored() throws Exception {
        Properties config = new Properties();
        config.put("path", "path/to/spark/catalog/dir");
        Connection conn = DriverManager.getConnection(JDBC_URL, config);
        conn.close();
    }

    public static void test_get_profiling_information() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("SET enable_profiling = 'no_output';");
            try (ResultSet rs = stmt.executeQuery("SELECT 1+1")) {
                String profile = ((DuckDBConnection) conn).getProfilingInformation(ProfilerPrintFormat.JSON);
                assertTrue(profile.contains("\"query_name\": \"SELECT 1+1\","));
            }
        }
    }

    public static void test_query_progress() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             DuckDBPreparedStatement stmt = conn.createStatement().unwrap(DuckDBPreparedStatement.class)) {

            QueryProgress qpBefore = stmt.getQueryProgress();
            assertEquals(qpBefore.getPercentage(), (double) -1);
            assertEquals(qpBefore.getRowsProcessed(), 0L);
            assertEquals(qpBefore.getTotalRowsToProcess(), 0L);

            stmt.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
            stmt.execute("INSERT INTO test_fib1 values(1, 0, 1)");
            stmt.execute("SET enable_progress_bar = true");

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<QueryProgress> future = executorService.submit(new Callable<QueryProgress>() {
                @Override
                public QueryProgress call() throws Exception {
                    try {
                        Thread.sleep(2500);
                        QueryProgress qp = stmt.getQueryProgress();
                        stmt.cancel();
                        return qp;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });
            assertThrows(
                ()
                    -> stmt.executeQuery(
                        "WITH RECURSIVE cte AS ("
                        +
                        "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 150000) "
                        + "SELECT avg(f) FROM cte"),
                SQLException.class);

            QueryProgress qpRunning = future.get();
            assertNotNull(qpRunning);
            assertEquals(qpRunning.getPercentage(), (double) 25);
            assertEquals(qpRunning.getRowsProcessed(), 1L);
            assertEquals(qpRunning.getTotalRowsToProcess(), 4L);

            assertThrows(stmt::getQueryProgress, SQLException.class);
        }
    }

    public static void test_memory_colon() throws Exception {
        try (Connection conn1 = DriverManager.getConnection("jdbc:duckdb::memory:");
             Statement stmt1 = conn1.createStatement();
             Connection conn2 = DriverManager.getConnection("jdbc:duckdb:memory:");
             Statement stmt2 = conn2.createStatement(); Statement stmt22 = conn2.createStatement()) {
            stmt1.execute("CREATE TABLE tab1(col1 int)");
            assertThrows(() -> { stmt2.execute("DROP TABLE tab1"); }, SQLException.class);
            stmt22.execute("CREATE TABLE tab1(col1 int)");
        }
        try (Connection conn1 = DriverManager.getConnection("jdbc:duckdb::memory:tag1");
             Statement stmt1 = conn1.createStatement(); Statement stmt12 = conn1.createStatement();
             Connection conn2 = DriverManager.getConnection("jdbc:duckdb:memory:tag1");
             Statement stmt2 = conn2.createStatement()) {
            stmt1.execute("CREATE TABLE tab1(col1 int)");
            stmt2.execute("DROP TABLE tab1");
            assertThrows(() -> { stmt1.execute("DROP TABLE tab1"); }, SQLException.class);
            stmt12.execute("CREATE TABLE tab1(col1 int)");
        }
    }

    public static void test_props_from_url() throws Exception {
        Properties config = new Properties();
        config.put("allow_community_extensions", false);
        config.put("allow_unsigned_extensions", true);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:", config);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('allow_community_extensions')")) {
                rs.next();
                assertEquals(rs.getString(1), "false");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('allow_unsigned_extensions')")) {
                rs.next();
                assertEquals(rs.getString(1), "true");
            }
        }

        try (Connection conn = DriverManager.getConnection(
                 "jdbc:duckdb:;allow_community_extensions=true;;allow_unsigned_extensions = false;", config);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('allow_community_extensions')")) {
                rs.next();
                assertEquals(rs.getString(1), "true");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('allow_unsigned_extensions')")) {
                rs.next();
                assertEquals(rs.getString(1), "false");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT current_catalog()")) {
                rs.next();
                assertEquals(rs.getString(1), "memory");
            }
        }

        try (Connection conn =
                 DriverManager.getConnection("jdbc:duckdb:test1.db;allow_community_extensions=true;", config);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('allow_community_extensions')")) {
                rs.next();
                assertEquals(rs.getString(1), "true");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT current_catalog()")) {
                rs.next();
                assertEquals(rs.getString(1), "test1");
            }
        }

        assertThrows(
            () -> { DriverManager.getConnection("jdbc:duckdb:;allow_unsigned_extensions"); }, SQLException.class);
        assertThrows(() -> { DriverManager.getConnection("jdbc:duckdb:;foo=bar"); }, SQLException.class);
    }

    public static void main(String[] args) throws Exception {
        String arg1 = args.length > 0 ? args[0] : "";
        final int statusCode;
        if (arg1.startsWith("Test")) {
            Class<?> clazz = Class.forName("org.duckdb." + arg1);
            statusCode = runTests(new String[0], clazz);
        } else {
            statusCode = runTests(args, TestDuckDBJDBC.class, TestBatch.class, TestClosure.class,
                                  TestExtensionTypes.class, TestSpatial.class, TestParameterMetadata.class,
                                  TestPrepare.class, TestResults.class, TestTimestamp.class);
        }
        System.exit(statusCode);
    }
}

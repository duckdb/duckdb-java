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

        assertThrows(conn_rw::createStatement, SQLException.class);
        assertThrows(() -> { conn_rw.unwrap(DuckDBConnection.class).duplicate(); }, SQLException.class);

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
        String memory_limit = "max_memory";
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
        info.put("errors_as_json", "true");

        String message = assertThrows(() -> DriverManager.getConnection(JDBC_URL, info), SQLException.class);

        assertTrue(message.contains("Could not set option \"errors_as_json\" as a global option"));
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

    @SuppressWarnings("try")
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

    @SuppressWarnings("unchecked")
    private static <T> List<T> arrayToList(Array array) throws SQLException {
        return arrayToList((T[]) array.getArray());
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> arrayToList(T[] array) throws SQLException {
        List<T> out = new ArrayList<>();
        for (Object t : array) {
            out.add((T) toJavaObject(t));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
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
                                        Instant.parse("-290308-12-21T19:59:05.224193Z"), ZoneId.systemDefault())),
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
            "bignum",
            asList(
                "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368",
                "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368",
                null));
        correct_answer_map.put("time", asList(LocalTime.of(0, 0), LocalTime.parse("23:59:59.999999"), null));
        correct_answer_map.put("time_ns", asList(LocalTime.of(0, 0), LocalTime.parse("23:59:59.999999"), null));
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

    @SuppressWarnings("unchecked")
    public static void test_all_types() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(ALL_TYPES_TIME_ZONE);
        try {
            Logger logger = Logger.getAnonymousLogger();
            String sql =
                "select * EXCLUDE(time, time_ns, time_tz)"
                + "\n    , CASE WHEN time = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE time END AS time"
                +
                "\n    , CASE WHEN time_ns = '24:00:00'::TIME_NS THEN '23:59:59.999999'::TIME_NS ELSE time_ns END AS time_ns"
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
                assertNotNull(rs);
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
            Future<QueryProgress> future = executorService.submit(() -> {
                try {
                    Thread.sleep(3000);
                    QueryProgress qp = stmt.getQueryProgress();
                    stmt.cancel();
                    return qp;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            });
            assertThrows(
                ()
                    -> stmt.executeQuery(
                        "WITH RECURSIVE cte AS NOT MATERIALIZED ("
                        +
                        "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 200000) "
                        + "SELECT avg(f) FROM cte"),
                SQLException.class);

            QueryProgress qpRunning = future.get();
            assertNotNull(qpRunning);
            assertTrue(qpRunning.getPercentage() > 0.09);
            assertTrue(qpRunning.getRowsProcessed() > 0);
            assertTrue(qpRunning.getTotalRowsToProcess() > 0);

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

    public static void test_auto_commit_option() throws Exception {
        Properties config = new Properties();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            assertTrue(conn.getAutoCommit());
        }

        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, true);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL, config).unwrap(DuckDBConnection.class)) {
            assertTrue(conn.getAutoCommit());

            try (Connection dup = conn.duplicate()) {
                assertTrue(dup.getAutoCommit());
            }
        }

        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, false);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL, config).unwrap(DuckDBConnection.class)) {
            assertFalse(conn.getAutoCommit());

            try (Connection dup = conn.duplicate()) {
                assertFalse(dup.getAutoCommit());
            }
        }

        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, "on");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            assertTrue(conn.getAutoCommit());
        }

        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, "off");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            assertFalse(conn.getAutoCommit());
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

    public static void test_pinned_db() throws Exception {
        Properties config = new Properties();
        config.put(DuckDBDriver.JDBC_PIN_DB, true);
        String memUrl = "jdbc:duckdb:memory:test1";

        try (Connection conn = DriverManager.getConnection(memUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        try (Connection conn = DriverManager.getConnection(memUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        try (Connection conn = DriverManager.getConnection(memUrl, config); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        try (Connection conn = DriverManager.getConnection(memUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE tab1");
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        try (Connection conn = DriverManager.getConnection(memUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE tab1");
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        assertThrows(
            () -> { DriverManager.getConnection(memUrl + ";allow_community_extensions=true;"); }, SQLException.class);

        assertTrue(DuckDBDriver.releaseDB(memUrl));
        assertFalse(DuckDBDriver.releaseDB(memUrl));

        try (Connection conn = DriverManager.getConnection(memUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        assertFalse(DuckDBDriver.releaseDB(memUrl));

        try (Connection conn = DriverManager.getConnection(memUrl + ";allow_community_extensions=true;");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
        }

        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 int)");
            assertFalse(DuckDBDriver.releaseDB(JDBC_URL));
        }

        // Leave DB pinned to check shutdown hook run
        DriverManager.getConnection(memUrl, config).close();
    }

    public static void test_driver_property_info() throws Exception {
        Driver driver = DriverManager.getDriver(JDBC_URL);
        DriverPropertyInfo[] dpis = driver.getPropertyInfo(JDBC_URL, null);
        for (DriverPropertyInfo dpi : dpis) {
            assertNotNull(dpi.name);
            //            assertNotNull(dpi.value);
            assertNotNull(dpi.description);
        }
        assertNotNull(dpis);
        assertTrue(dpis.length > 0);
    }

    public static void test_ignore_unsupported_options() throws Exception {
        assertThrows(() -> { DriverManager.getConnection("jdbc:duckdb:;foo=bar;"); }, SQLException.class);
        Properties config = new Properties();
        config.put("boo", "bar");
        config.put(JDBC_STREAM_RESULTS, true);
        DriverManager.getConnection("jdbc:duckdb:;foo=bar;jdbc_ignore_unsupported_options=yes;", config).close();
    }

    public static void test_extension_excel() throws Exception {
        // Check whether the Excel extension can be installed and loaded automatically
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT excel_text(1_234_567.897, 'h:mm AM/PM')")) {
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "9:31 PM");
            assertFalse(rs.next());
        }
    }

    public static void main(String[] args) throws Exception {
        String arg1 = args.length > 0 ? args[0] : "";
        final int statusCode;
        if (arg1.startsWith("Test")) {
            Class<?> clazz = Class.forName("org.duckdb." + arg1);
            statusCode = runTests(new String[0], clazz);
        } else {
            statusCode =
                runTests(args, TestDuckDBJDBC.class, TestAppender.class, TestAppenderCollection.class,
                         TestAppenderCollection2D.class, TestAppenderComposite.class, TestSingleValueAppender.class,
                         TestBatch.class, TestBindings.class, TestClosure.class, TestExtensionTypes.class,
                         TestMetadata.class, TestNoLib.class, /* TestSpatial.class, */ TestParameterMetadata.class,
                         TestPrepare.class, TestResults.class, TestSessionInit.class, TestTimestamp.class);
        }
        System.exit(statusCode);
    }
}

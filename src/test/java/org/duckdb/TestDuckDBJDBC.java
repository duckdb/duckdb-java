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
import java.nio.file.Paths;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.duckdb.test.TempDirectory;
import org.duckdb.udf.TableBindResult;
import org.duckdb.udf.TableFunctionDefinition;
import org.duckdb.udf.TableFunctionOptions;
import org.duckdb.udf.TableState;
import org.duckdb.udf.UdfLogicalType;
import org.duckdb.udf.UdfOptions;

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

    private static DuckDBColumnType[] scalarCoreTypes() {
        return new DuckDBColumnType[] {DuckDBColumnType.BOOLEAN, DuckDBColumnType.TINYINT, DuckDBColumnType.SMALLINT,
                                       DuckDBColumnType.INTEGER, DuckDBColumnType.BIGINT,  DuckDBColumnType.FLOAT,
                                       DuckDBColumnType.DOUBLE,  DuckDBColumnType.VARCHAR};
    }

    private static DuckDBColumnType[] scalarExtendedTypes() {
        return new DuckDBColumnType[] {
            DuckDBColumnType.DECIMAL,     DuckDBColumnType.BLOB,         DuckDBColumnType.DATE,
            DuckDBColumnType.TIME,        DuckDBColumnType.TIME_NS,      DuckDBColumnType.TIMESTAMP,
            DuckDBColumnType.TIMESTAMP_S, DuckDBColumnType.TIMESTAMP_MS, DuckDBColumnType.TIMESTAMP_NS};
    }

    private static DuckDBColumnType[] scalarUnsignedAndSpecialTypes() {
        return new DuckDBColumnType[] {DuckDBColumnType.UTINYINT,
                                       DuckDBColumnType.USMALLINT,
                                       DuckDBColumnType.UINTEGER,
                                       DuckDBColumnType.UBIGINT,
                                       DuckDBColumnType.HUGEINT,
                                       DuckDBColumnType.UHUGEINT,
                                       DuckDBColumnType.TIME_WITH_TIME_ZONE,
                                       DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
                                       DuckDBColumnType.UUID};
    }

    private static String nonNullLiteralForType(DuckDBColumnType type) {
        switch (type) {
        case BOOLEAN:
            return "TRUE::BOOLEAN";
        case TINYINT:
            return "7::TINYINT";
        case SMALLINT:
            return "32000::SMALLINT";
        case INTEGER:
            return "123456::INTEGER";
        case BIGINT:
            return "9876543210::BIGINT";
        case FLOAT:
            return "1.25::FLOAT";
        case DOUBLE:
            return "2.5::DOUBLE";
        case VARCHAR:
            return "'duck'::VARCHAR";
        default:
            throw new IllegalArgumentException("Unsupported test type: " + type);
        }
    }

    private static String sqlTypeNameForLiteral(DuckDBColumnType type) {
        switch (type) {
        case TIME_WITH_TIME_ZONE:
            return "TIME WITH TIME ZONE";
        case TIMESTAMP_WITH_TIME_ZONE:
            return "TIMESTAMP WITH TIME ZONE";
        default:
            return type.name();
        }
    }

    private static String nullLiteralForType(DuckDBColumnType type) {
        return "NULL::" + sqlTypeNameForLiteral(type);
    }

    private static String nonNullLiteralForExtendedType(DuckDBColumnType type) {
        switch (type) {
        case DECIMAL:
            return "42.75::DECIMAL(18,2)";
        case BLOB:
            return "'blob-extended'::BLOB";
        case DATE:
            return "DATE '2024-01-03'";
        case TIME:
            return "TIME '01:02:03.123456'";
        case TIME_NS:
            return "TIME_NS '01:02:03.123456789'";
        case TIMESTAMP:
            return "TIMESTAMP '2024-01-03 04:05:06.123456'";
        case TIMESTAMP_S:
            return "TIMESTAMP_S '2024-01-03 04:05:06'";
        case TIMESTAMP_MS:
            return "TIMESTAMP_MS '2024-01-03 04:05:06.123'";
        case TIMESTAMP_NS:
            return "TIMESTAMP_NS '2024-01-03 04:05:06.123456789'";
        default:
            throw new IllegalArgumentException("Unsupported extended test type: " + type);
        }
    }

    private static String nonNullLiteralForUnsignedAndSpecialType(DuckDBColumnType type) {
        switch (type) {
        case UTINYINT:
            return "250::UTINYINT";
        case USMALLINT:
            return "65000::USMALLINT";
        case UINTEGER:
            return "4000000000::UINTEGER";
        case UBIGINT:
            return "18446744073709551615::UBIGINT";
        case HUGEINT:
            return "170141183460469231731687303715884105727::HUGEINT";
        case UHUGEINT:
            return "340282366920938463463374607431768211455::UHUGEINT";
        case TIME_WITH_TIME_ZONE:
            return "'01:02:03+05:30'::TIME WITH TIME ZONE";
        case TIMESTAMP_WITH_TIME_ZONE:
            return "'2024-01-03 04:05:06+00'::TIMESTAMP WITH TIME ZONE";
        case UUID:
            return "'550e8400-e29b-41d4-a716-446655440000'::UUID";
        default:
            throw new IllegalArgumentException("Unsupported unsigned/special test type: " + type);
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

    public static void test_range_java_smoke() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("range_java", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 1024 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                    }
                    st[0] = current;
                    return produced;
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM range_java(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                }
                assertFalse(rs.next());
            }
        }
    }

    public static void test_range_java_streaming_large() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("range_java_large", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 256 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                    }
                    st[0] = current;
                    return produced;
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT count(*), sum(i) FROM range_java_large(10000)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 10000L);
                assertEquals(rs.getLong(2), 49995000L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_range_java_output_appender_api() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("range_java_appender", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 128 && current < end; produced++, current++) {
                        out.beginRow().append(current).endRow();
                    }
                    st[0] = current;
                    return out.getSize();
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM range_java_appender(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                }
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_bind_typed_parameters() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            Object[] observedParameters = new Object[3];
            boolean[] observedNullPath = new boolean[] {false};

            conn.registerTableFunction(
                "tf_bind_typed",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        if (parameters.length != 3) {
                            throw new IllegalStateException("Expected 3 bind parameters");
                        }
                        observedParameters[0] = parameters[0];
                        observedParameters[1] = parameters[1];
                        observedParameters[2] = parameters[2];

                        int start = parameters[0] == null ? 0 : ((Number) parameters[0]).intValue();
                        if (parameters[0] == null) {
                            observedNullPath[0] = true;
                        }
                        int delta = (int) Math.round(((Number) parameters[1]).doubleValue());
                        int labelLen = ((String) parameters[2]).length();
                        int end = start + delta + labelLen;
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER}, end);
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        int end = ((Number) bind.getBindState()).intValue();
                        return new TableState(new int[] {0, end});
                    }

                    @Override
                    public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] st = (int[]) state.getState();
                        int current = st[0];
                        int end = st[1];
                        int produced = 0;
                        for (; produced < 1024 && current < end; produced++, current++) {
                            out.setInt(0, produced, current);
                        }
                        st[0] = current;
                        return produced;
                    }
                },
                new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {
                    DuckDBColumnType.INTEGER, DuckDBColumnType.DOUBLE, DuckDBColumnType.VARCHAR}));

            try (ResultSet rs = stmt.executeQuery("SELECT count(*), sum(i) FROM tf_bind_typed(5, 2.0, 'abc')")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 10L);
                assertEquals(rs.getLong(2), 45L);
                assertFalse(rs.next());
            }

            assertTrue(observedParameters[0] instanceof Integer);
            assertTrue(observedParameters[1] instanceof Double);
            assertTrue(observedParameters[2] instanceof String);
            assertEquals(observedParameters[0], 5);
            assertEquals(observedParameters[1], 2.0d);
            assertEquals(observedParameters[2], "abc");

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tf_bind_typed(NULL::INTEGER, 2.0, 'xy')")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 4L);
                assertFalse(rs.next());
            }
            assertTrue(observedNullPath[0]);
        }
    }

    public static void test_table_function_bind_typed_parameters_extended_types() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            Object[] observedParameters = new Object[5];

            conn.registerTableFunction(
                "tf_bind_extended_types",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        if (parameters.length != 5) {
                            throw new IllegalStateException("Expected 5 bind parameters");
                        }
                        System.arraycopy(parameters, 0, observedParameters, 0, parameters.length);
                        int rows = (int) Math.round(((Number) parameters[0]).doubleValue());
                        rows = Math.max(rows, 0);
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER}, rows);
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        return new TableState(new int[] {0, ((Number) bind.getBindState()).intValue()});
                    }

                    @Override
                    public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] st = (int[]) state.getState();
                        int current = st[0];
                        int end = st[1];
                        int produced = 0;
                        for (; produced < 64 && current < end; produced++, current++) {
                            out.setInt(0, produced, current);
                        }
                        st[0] = current;
                        return produced;
                    }
                },
                new TableFunctionDefinition().withParameterTypes(
                    new DuckDBColumnType[] {DuckDBColumnType.DECIMAL, DuckDBColumnType.BLOB, DuckDBColumnType.DATE,
                                            DuckDBColumnType.TIME, DuckDBColumnType.TIMESTAMP}));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT count(*), sum(i) FROM tf_bind_extended_types("
                                       + "3.0::DECIMAL(18,2), 'blob-extended'::BLOB, DATE '2024-01-03', "
                                       + "TIME '01:02:03.123456', TIMESTAMP '2024-01-03 04:05:06.123456')")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 3L);
                assertEquals(rs.getLong(2), 3L);
                assertFalse(rs.next());
            }

            assertTrue(observedParameters[0] instanceof BigDecimal);
            assertEquals(((BigDecimal) observedParameters[0]).compareTo(new BigDecimal("3.00")), 0);
            assertTrue(observedParameters[1] instanceof byte[]);
            assertEquals((byte[]) observedParameters[1], "blob-extended".getBytes(StandardCharsets.UTF_8));
            assertTrue(observedParameters[2] instanceof LocalDate);
            assertTrue(observedParameters[3] instanceof LocalTime);
            assertTrue(observedParameters[4] instanceof LocalDateTime);
            assertEquals(observedParameters[2], LocalDate.of(2024, 1, 3));
            assertEquals(observedParameters[3], LocalTime.parse("01:02:03.123456"));
            assertEquals(observedParameters[4], LocalDateTime.parse("2024-01-03T04:05:06.123456"));
        }
    }

    public static void test_table_function_bind_temporal_and_uuid_objects() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            Object[] observedParameters = new Object[10];

            conn.registerTableFunction(
                "tf_bind_temporal_uuid_objects",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        if (parameters.length != observedParameters.length) {
                            throw new IllegalStateException("Expected 10 bind parameters");
                        }
                        System.arraycopy(parameters, 0, observedParameters, 0, parameters.length);
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER}, null);
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        return new TableState(new int[] {0});
                    }

                    @Override
                    public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] producedRows = (int[]) state.getState();
                        if (producedRows[0] > 0) {
                            return 0;
                        }
                        out.setInt(0, 0, 1);
                        producedRows[0] = 1;
                        return 1;
                    }
                },
                new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {
                    DuckDBColumnType.DATE, DuckDBColumnType.TIME, DuckDBColumnType.TIME_NS, DuckDBColumnType.TIMESTAMP,
                    DuckDBColumnType.TIMESTAMP_S, DuckDBColumnType.TIMESTAMP_MS, DuckDBColumnType.TIMESTAMP_NS,
                    DuckDBColumnType.TIME_WITH_TIME_ZONE, DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
                    DuckDBColumnType.UUID}));

            try (ResultSet rs = stmt.executeQuery("SELECT sum(i) FROM tf_bind_temporal_uuid_objects("
                                                  + "DATE '2024-01-03', "
                                                  + "TIME '01:02:03.123456', "
                                                  + "TIME_NS '01:02:03.123456789', "
                                                  + "TIMESTAMP '2024-01-03 04:05:06.123456', "
                                                  + "TIMESTAMP_S '2024-01-03 04:05:06', "
                                                  + "TIMESTAMP_MS '2024-01-03 04:05:06.123', "
                                                  + "TIMESTAMP_NS '2024-01-03 04:05:06.123456789', "
                                                  + "'01:02:03+05:30'::TIME WITH TIME ZONE, "
                                                  + "'2024-01-03 04:05:06+00'::TIMESTAMP WITH TIME ZONE, "
                                                  + "'550e8400-e29b-41d4-a716-446655440000'::UUID)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertFalse(rs.next());
            }

            assertTrue(observedParameters[0] instanceof LocalDate);
            assertTrue(observedParameters[1] instanceof LocalTime);
            assertTrue(observedParameters[2] instanceof LocalTime);
            assertTrue(observedParameters[3] instanceof LocalDateTime);
            assertTrue(observedParameters[4] instanceof LocalDateTime);
            assertTrue(observedParameters[5] instanceof LocalDateTime);
            assertTrue(observedParameters[6] instanceof LocalDateTime);
            assertTrue(observedParameters[7] instanceof OffsetTime);
            assertTrue(observedParameters[8] instanceof OffsetDateTime);
            assertTrue(observedParameters[9] instanceof UUID);

            assertEquals(observedParameters[0], LocalDate.of(2024, 1, 3));
            assertEquals(observedParameters[1], LocalTime.parse("01:02:03.123456"));
            assertEquals(observedParameters[2], LocalTime.parse("01:02:03.123456789"));
            assertEquals(observedParameters[3], LocalDateTime.parse("2024-01-03T04:05:06.123456"));
            assertEquals(observedParameters[4], LocalDateTime.parse("2024-01-03T04:05:06"));
            assertEquals(observedParameters[5], LocalDateTime.parse("2024-01-03T04:05:06.123"));
            assertEquals(observedParameters[6], LocalDateTime.parse("2024-01-03T04:05:06.123456789"));
            assertEquals(observedParameters[7], OffsetTime.parse("01:02:03+05:30"));
            assertEquals(observedParameters[8], OffsetDateTime.parse("2024-01-03T04:05:06Z"));
            assertEquals(observedParameters[9], UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT sum(i) FROM tf_bind_temporal_uuid_objects("
                                       + "NULL::DATE, NULL::TIME, NULL::TIME_NS, NULL::TIMESTAMP, NULL::TIMESTAMP_S, "
                                       + "NULL::TIMESTAMP_MS, NULL::TIMESTAMP_NS, NULL::TIME WITH TIME ZONE, "
                                       + "NULL::TIMESTAMP WITH TIME ZONE, NULL::UUID)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertFalse(rs.next());
            }
            for (Object observedParameter : observedParameters) {
                assertEquals(observedParameter, null);
            }
        }
    }

    public static void test_table_function_bind_decimal_parameter_exact_bigdecimal() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            Object[] observedParameters = new Object[1];

            conn.registerTableFunction("tf_bind_decimal_exact", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    if (parameters.length != 1) {
                        throw new IllegalStateException("Expected 1 bind parameter");
                    }
                    observedParameters[0] = parameters[0];
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               null);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] produced = (int[]) state.getState();
                    if (produced[0] > 0) {
                        return 0;
                    }
                    out.setInt(0, 0, 1);
                    produced[0] = 1;
                    return 1;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.DECIMAL}));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT sum(i) FROM tf_bind_decimal_exact(9007199254740.127::DECIMAL(18,3))")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertFalse(rs.next());
            }

            assertTrue(observedParameters[0] instanceof BigDecimal);
            assertEquals(((BigDecimal) observedParameters[0]).compareTo(new BigDecimal("9007199254740.127")), 0);
        }
    }

    public static void test_table_function_decimal_logical_output_boundaries_and_nulls() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_decimal_logical_boundaries", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"d4_1", "d9_4", "d18_6", "d30_10", "d38_10"},
                        new UdfLogicalType[] {UdfLogicalType.decimal(4, 1), UdfLogicalType.decimal(9, 4),
                                              UdfLogicalType.decimal(18, 6), UdfLogicalType.decimal(30, 10),
                                              UdfLogicalType.decimal(38, 10)},
                        null);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] producedRows = (int[]) state.getState();
                    if (producedRows[0] > 0) {
                        return 0;
                    }

                    out.setBigDecimal(0, 0, new BigDecimal("999.9"));
                    out.setBigDecimal(1, 0, new BigDecimal("99999.9999"));
                    out.setBigDecimal(2, 0, new BigDecimal("999999999999.999999"));
                    out.setBigDecimal(3, 0, new BigDecimal("99999999999999999999.9999999999"));
                    out.setBigDecimal(4, 0, new BigDecimal("9999999999999999999999999999.9999999999"));

                    out.setBigDecimal(0, 1, new BigDecimal("-999.9"));
                    out.setBigDecimal(1, 1, new BigDecimal("-99999.9999"));
                    out.setBigDecimal(2, 1, new BigDecimal("-999999999999.999999"));
                    out.setBigDecimal(3, 1, new BigDecimal("-99999999999999999999.9999999999"));
                    out.setBigDecimal(4, 1, new BigDecimal("-9999999999999999999999999999.9999999999"));

                    for (int col = 0; col < 5; col++) {
                        out.setNull(col, 2);
                    }

                    producedRows[0] = 3;
                    return 3;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[0]));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT CAST(d4_1 AS VARCHAR), CAST(d9_4 AS VARCHAR), CAST(d18_6 AS VARCHAR), "
                                       + "CAST(d30_10 AS VARCHAR), CAST(d38_10 AS VARCHAR) "
                                       + "FROM tf_decimal_logical_boundaries() ORDER BY d4_1 DESC NULLS LAST")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "999.9");
                assertEquals(rs.getString(2), "99999.9999");
                assertEquals(rs.getString(3), "999999999999.999999");
                assertEquals(rs.getString(4), "99999999999999999999.9999999999");
                assertEquals(rs.getString(5), "9999999999999999999999999999.9999999999");

                assertTrue(rs.next());
                assertEquals(rs.getString(1), "-999.9");
                assertEquals(rs.getString(2), "-99999.9999");
                assertEquals(rs.getString(3), "-999999999999.999999");
                assertEquals(rs.getString(4), "-99999999999999999999.9999999999");
                assertEquals(rs.getString(5), "-9999999999999999999999999999.9999999999");

                assertTrue(rs.next());
                assertEquals(rs.getObject(1), null);
                assertEquals(rs.getObject(2), null);
                assertEquals(rs.getObject(3), null);
                assertEquals(rs.getObject(4), null);
                assertEquals(rs.getObject(5), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_bind_typed_parameters_complex() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            Object[] observedParameters = new Object[4];

            conn.registerTableFunction(
                "tf_bind_complex",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        if (parameters.length != 4) {
                            throw new IllegalStateException("Expected 4 bind parameters");
                        }
                        System.arraycopy(parameters, 0, observedParameters, 0, parameters.length);

                        List<?> listParam = (List<?>) parameters[0];
                        Map<?, ?> mapParam = (Map<?, ?>) parameters[1];
                        Map<?, ?> structParam = (Map<?, ?>) parameters[2];
                        String enumParam = (String) parameters[3];

                        int rowCount = listParam.size() + mapParam.size() + ((Number) structParam.get("id")).intValue();
                        if ("medium".equals(enumParam)) {
                            rowCount += 1;
                        }
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER}, rowCount);
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        int end = ((Number) bind.getBindState()).intValue();
                        return new TableState(new int[] {0, end});
                    }

                    @Override
                    public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] st = (int[]) state.getState();
                        int current = st[0];
                        int end = st[1];
                        int produced = 0;
                        for (; produced < 128 && current < end; produced++, current++) {
                            out.setInt(0, produced, current);
                        }
                        st[0] = current;
                        return produced;
                    }
                },
                new TableFunctionDefinition().withParameterTypes(new UdfLogicalType[] {
                    UdfLogicalType.list(UdfLogicalType.of(DuckDBColumnType.INTEGER)),
                    UdfLogicalType.map(UdfLogicalType.of(DuckDBColumnType.VARCHAR),
                                       UdfLogicalType.of(DuckDBColumnType.INTEGER)),
                    UdfLogicalType.struct(new String[] {"id", "txt"},
                                          new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER),
                                                                UdfLogicalType.of(DuckDBColumnType.VARCHAR)}),
                    UdfLogicalType.enumeration("small", "medium", "large")}));

            try (ResultSet rs = stmt.executeQuery("SELECT count(*), sum(i) FROM tf_bind_complex("
                                                  + "[10,20,30], map(['k1','k2'], [100,200]), "
                                                  + "{'id':4, 'txt':'duck'}, "
                                                  + "'medium'::ENUM('small','medium','large'))")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 10L);
                assertEquals(rs.getLong(2), 45L);
                assertFalse(rs.next());
            }

            assertTrue(observedParameters[0] instanceof List);
            assertTrue(observedParameters[1] instanceof Map);
            assertTrue(observedParameters[2] instanceof Map);
            assertTrue(observedParameters[3] instanceof String);

            List<?> observedList = (List<?>) observedParameters[0];
            assertEquals(observedList.size(), 3);
            assertEquals(((Number) observedList.get(0)).intValue(), 10);
            assertEquals(((Number) observedList.get(1)).intValue(), 20);
            assertEquals(((Number) observedList.get(2)).intValue(), 30);

            Map<?, ?> observedMap = (Map<?, ?>) observedParameters[1];
            assertEquals(observedMap.size(), 2);
            assertEquals(((Number) observedMap.get("k1")).intValue(), 100);
            assertEquals(((Number) observedMap.get("k2")).intValue(), 200);

            Map<?, ?> observedStruct = (Map<?, ?>) observedParameters[2];
            assertEquals(((Number) observedStruct.get("id")).intValue(), 4);
            assertEquals(observedStruct.get("txt"), "duck");
            assertEquals(observedParameters[3], "medium");
        }
    }

    public static void test_table_function_bind_parameter_type_validation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(() -> {
                conn.registerTableFunction(
                    "tf_bad_param",
                    new org.duckdb.udf.TableFunction() {
                        @Override
                        public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                            return new TableBindResult(new String[] {"i"},
                                                       new DuckDBColumnType[] {DuckDBColumnType.INTEGER});
                        }

                        @Override
                        public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                            return new TableState(new int[] {0, 0});
                        }

                        @Override
                        public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                            return 0;
                        }
                    },
                    new TableFunctionDefinition().withParameterTypes(
                        new DuckDBColumnType[] {DuckDBColumnType.INTERVAL}));
            }, SQLFeatureNotSupportedException.class);

            assertThrows(() -> {
                conn.registerTableFunction(
                    "tf_bad_param_logical",
                    new org.duckdb.udf.TableFunction() {
                        @Override
                        public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                            return new TableBindResult(new String[] {"i"},
                                                       new DuckDBColumnType[] {DuckDBColumnType.INTEGER});
                        }

                        @Override
                        public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                            return new TableState(new int[] {0, 0});
                        }

                        @Override
                        public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                            return 0;
                        }
                    },
                    new TableFunctionDefinition().withParameterTypes(
                        new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTERVAL)}));
            }, SQLException.class);
        }
    }

    public static void test_table_function_typed_outputs_core_types() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_core_out", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    int end = ((Number) parameters[0]).intValue();
                    return new TableBindResult(
                        new String[] {"b", "t8", "s16", "i32", "i64", "f32", "f64", "txt"},
                        new DuckDBColumnType[] {DuckDBColumnType.BOOLEAN, DuckDBColumnType.TINYINT,
                                                DuckDBColumnType.SMALLINT, DuckDBColumnType.INTEGER,
                                                DuckDBColumnType.BIGINT, DuckDBColumnType.FLOAT,
                                                DuckDBColumnType.DOUBLE, DuckDBColumnType.VARCHAR},
                        end);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 128 && current < end; produced++, current++) {
                        out.setBoolean(0, produced, current % 2 == 0);
                        out.setInt(1, produced, current - 50);
                        out.setInt(2, produced, 1000 + current);
                        out.setInt(3, produced, current * 10);
                        out.setLong(4, produced, 1_000_000_000_000L + current);
                        out.setFloat(5, produced, current + 0.5f);
                        if (current % 2 == 0) {
                            out.setDouble(6, produced, current + 0.25d);
                        } else {
                            out.setNull(6, produced);
                        }
                        if (current % 3 == 0) {
                            out.setNull(7, produced);
                        } else {
                            out.setString(7, produced, "v" + current);
                        }
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tf_core_out(6) ORDER BY i32")) {
                for (int i = 0; i < 6; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean(1), i % 2 == 0);
                    assertEquals(rs.getInt(2), i - 50);
                    assertEquals(rs.getInt(3), 1000 + i);
                    assertEquals(rs.getInt(4), i * 10);
                    assertEquals(rs.getLong(5), 1_000_000_000_000L + i);
                    assertEquals(rs.getFloat(6), i + 0.5f, 0.0001f);
                    if (i % 2 == 0) {
                        assertEquals(rs.getDouble(7), i + 0.25d, 0.0000001d);
                    } else {
                        assertEquals(rs.getObject(7), null);
                    }
                    if (i % 3 == 0) {
                        assertEquals(rs.getString(8), null);
                    } else {
                        assertEquals(rs.getString(8), "v" + i);
                    }
                }
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_typed_outputs_extended_types() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_extended_out", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    int end = ((Number) parameters[0]).intValue();
                    return new TableBindResult(new String[] {"id", "dec", "blob", "d", "t", "ts"},
                                               new DuckDBColumnType[] {DuckDBColumnType.INTEGER,
                                                                       DuckDBColumnType.DECIMAL, DuckDBColumnType.BLOB,
                                                                       DuckDBColumnType.DATE, DuckDBColumnType.TIME,
                                                                       DuckDBColumnType.TIMESTAMP},
                                               end);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    LocalDate baseDate = LocalDate.of(2024, 1, 3);
                    LocalTime baseTime = LocalTime.of(1, 1, 1);
                    LocalDateTime baseTimestamp = LocalDateTime.of(2024, 1, 3, 4, 5, 6);
                    for (; produced < 64 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                        out.setBigDecimal(1, produced, BigDecimal.valueOf(current).add(BigDecimal.valueOf(0.25d)));
                        out.setBytes(2, produced, ("b" + current).getBytes(StandardCharsets.UTF_8));
                        out.setLocalDate(3, produced, baseDate.plusDays(current));
                        out.setLocalTime(4, produced, baseTime.plusSeconds(current));
                        out.setLocalDateTime(5, produced, baseTimestamp.plusSeconds(current));
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            try (ResultSet rs = stmt.executeQuery("SELECT id, CAST(dec AS DOUBLE), blob, CAST(d AS VARCHAR), "
                                                  + "CAST(t AS VARCHAR), CAST(ts AS VARCHAR) "
                                                  + "FROM tf_extended_out(3) ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 0);
                assertEquals(rs.getDouble(2), 0.25d, 0.000001d);
                assertEquals(rs.getBytes(3), "b0".getBytes(StandardCharsets.UTF_8));
                assertEquals(rs.getString(4), "2024-01-03");
                assertEquals(rs.getString(5), "01:01:01");
                assertEquals(rs.getString(6), "2024-01-03 04:05:06");

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertEquals(rs.getDouble(2), 1.25d, 0.000001d);
                assertEquals(rs.getBytes(3), "b1".getBytes(StandardCharsets.UTF_8));
                assertEquals(rs.getString(4), "2024-01-04");
                assertEquals(rs.getString(5), "01:01:02");
                assertEquals(rs.getString(6), "2024-01-03 04:05:07");

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 2);
                assertEquals(rs.getDouble(2), 2.25d, 0.000001d);
                assertEquals(rs.getBytes(3), "b2".getBytes(StandardCharsets.UTF_8));
                assertEquals(rs.getString(4), "2024-01-05");
                assertEquals(rs.getString(5), "01:01:03");
                assertEquals(rs.getString(6), "2024-01-03 04:05:08");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_output_appender_java_object_methods() throws Exception {
        final UUID expectedUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_out_java_objects", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    int end = ((Number) parameters[0]).intValue();
                    return new TableBindResult(
                        new String[] {"dec", "d", "t", "ts", "t_tz", "ts_tz", "uuid_v"},
                        new DuckDBColumnType[] {DuckDBColumnType.DECIMAL, DuckDBColumnType.DATE, DuckDBColumnType.TIME,
                                                DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIME_WITH_TIME_ZONE,
                                                DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE, DuckDBColumnType.UUID},
                        end);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    if (st[0] >= st[1]) {
                        return 0;
                    }
                    out.setBigDecimal(0, 0, new BigDecimal("12.345"));
                    out.setLocalDate(1, 0, LocalDate.of(2024, 1, 3));
                    out.setLocalTime(2, 0, LocalTime.of(1, 2, 3));
                    out.setDate(3, 0, new java.util.Date(1_704_254_706_000L)); // 2024-01-03 04:05:06 UTC
                    out.setOffsetTime(4, 0, OffsetTime.parse("01:02:03+02:00"));
                    out.setOffsetDateTime(5, 0, OffsetDateTime.parse("2024-01-03T04:05:06+02:00"));
                    out.setUUID(6, 0, expectedUuid);
                    st[0]++;
                    return 1;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            try (ResultSet rs = stmt.executeQuery("SELECT CAST(dec AS DOUBLE), CAST(d AS VARCHAR), CAST(t AS VARCHAR), "
                                                  + "CAST(ts AS VARCHAR), CAST(uuid_v AS VARCHAR), "
                                                  + "t_tz IS NOT NULL, ts_tz IS NOT NULL "
                                                  + "FROM tf_out_java_objects(1)")) {
                assertTrue(rs.next());
                assertEquals(rs.getDouble(1), 12.345d, 0.000001d);
                assertEquals(rs.getString(2), "2024-01-03");
                assertEquals(rs.getString(3), "01:02:03");
                assertEquals(rs.getString(4), "2024-01-03 04:05:06");
                assertEquals(rs.getString(5), expectedUuid.toString());
                assertEquals(rs.getBoolean(6), true);
                assertEquals(rs.getBoolean(7), true);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_output_appender_java_append_overloads() throws Exception {
        final UUID expectedUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_out_java_append_overloads", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"dec", "d", "t", "ts", "uuid_v"},
                        new DuckDBColumnType[] {DuckDBColumnType.DECIMAL, DuckDBColumnType.DATE, DuckDBColumnType.TIME,
                                                DuckDBColumnType.TIMESTAMP, DuckDBColumnType.UUID},
                        null);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] producedRows = (int[]) state.getState();
                    if (producedRows[0] > 0) {
                        return 0;
                    }

                    out.beginRow()
                        .append(new BigDecimal("9.875"))
                        .append(LocalDate.of(2025, 2, 14))
                        .append(LocalTime.of(12, 34, 56))
                        .append(LocalDateTime.of(2025, 2, 14, 12, 34, 56))
                        .append(expectedUuid)
                        .endRow();

                    out.beginRow().appendNull().appendNull().appendNull().appendNull().appendNull().endRow();

                    producedRows[0] = 2;
                    return 2;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[0]));

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT CAST(dec AS DOUBLE), CAST(d AS VARCHAR), CAST(t AS VARCHAR), CAST(ts AS VARCHAR), "
                     + "CAST(uuid_v AS VARCHAR) FROM tf_out_java_append_overloads() ORDER BY d NULLS LAST")) {
                assertTrue(rs.next());
                assertEquals(rs.getDouble(1), 9.875d, 0.000001d);
                assertEquals(rs.getString(2), "2025-02-14");
                assertEquals(rs.getString(3), "12:34:56");
                assertEquals(rs.getString(4), "2025-02-14 12:34:56");
                assertEquals(rs.getString(5), expectedUuid.toString());

                assertTrue(rs.next());
                assertEquals(rs.getObject(1), null);
                assertEquals(rs.getObject(2), null);
                assertEquals(rs.getObject(3), null);
                assertEquals(rs.getObject(4), null);
                assertEquals(rs.getObject(5), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_output_appender_decimal_exact_bigdecimal_paths() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_out_decimal_exact", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"dec"}, new DuckDBColumnType[] {DuckDBColumnType.DECIMAL},
                                               null);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] producedRows = (int[]) state.getState();
                    if (producedRows[0] > 0) {
                        return 0;
                    }

                    out.beginRow().append(new BigDecimal("9007199254740.127")).endRow();
                    out.setBigDecimal(0, 1, new BigDecimal("-9007199254740.127"));

                    producedRows[0] = 2;
                    return 2;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[0]));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT CAST(dec AS VARCHAR) FROM tf_out_decimal_exact() ORDER BY dec DESC")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "9007199254740.127");
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "-9007199254740.127");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_output_appender_java_object_type_mismatch() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_out_java_object_type_mismatch", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER});
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(null);
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    out.setLocalDate(0, 0, LocalDate.of(2024, 1, 1));
                    return 1;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[0]));

            assertThrows(
                () -> { stmt.executeQuery("SELECT * FROM tf_out_java_object_type_mismatch()"); }, SQLException.class);
        }
    }

    public static void test_table_function_typed_outputs_unsigned_and_special_roundtrip_and_nulls() throws Exception {
        final DuckDBColumnType[] types = scalarUnsignedAndSpecialTypes();
        final String[] columnNames =
            new String[] {"u8", "u16", "u32", "u64", "i128", "u128", "timetz", "tstz", "uuid_v"};

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_unsigned_special_out", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(columnNames, types, parameters.clone());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new Object[] {bind.getBindState(), Boolean.FALSE});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    Object[] stateValues = (Object[]) state.getState();
                    if ((Boolean) stateValues[1]) {
                        return 0;
                    }

                    Object[] parameters = (Object[]) stateValues[0];
                    for (int i = 0; i < types.length; i++) {
                        Object parameter = parameters[i];
                        if (parameter == null) {
                            out.setNull(i, 0);
                            continue;
                        }

                        switch (types[i]) {
                        case UTINYINT:
                        case USMALLINT:
                            out.setInt(i, 0, ((Number) parameter).intValue());
                            break;
                        case UINTEGER:
                        case UBIGINT:
                            out.setLong(i, 0, ((Number) parameter).longValue());
                            break;
                        case TIME_WITH_TIME_ZONE:
                            out.setOffsetTime(i, 0, (OffsetTime) parameter);
                            break;
                        case TIMESTAMP_WITH_TIME_ZONE:
                            out.setOffsetDateTime(i, 0, (OffsetDateTime) parameter);
                            break;
                        case HUGEINT:
                        case UHUGEINT:
                            out.setBytes(i, 0, (byte[]) parameter);
                            break;
                        case UUID:
                            out.setUUID(i, 0, (UUID) parameter);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected unsigned/special table type: " + types[i]);
                        }
                    }

                    stateValues[1] = Boolean.TRUE;
                    return 1;
                }
            }, new TableFunctionDefinition().withParameterTypes(types));

            StringBuilder nonNullArgs = new StringBuilder();
            StringBuilder nonNullChecks = new StringBuilder();
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    nonNullArgs.append(", ");
                    nonNullChecks.append(", ");
                }
                String literal = nonNullLiteralForUnsignedAndSpecialType(types[i]);
                nonNullArgs.append(literal);
                nonNullChecks.append(columnNames[i]).append(" = ").append(literal);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT " + nonNullChecks + " FROM tf_unsigned_special_out(" +
                                                  nonNullArgs + ")")) {
                assertTrue(rs.next());
                for (int i = 0; i < types.length; i++) {
                    assertEquals(rs.getBoolean(i + 1), true);
                }
                assertFalse(rs.next());
            }

            StringBuilder nullArgs = new StringBuilder();
            StringBuilder nullChecks = new StringBuilder();
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    nullArgs.append(", ");
                    nullChecks.append(", ");
                }
                nullArgs.append(nullLiteralForType(types[i]));
                nullChecks.append(columnNames[i]).append(" IS NULL");
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT " + nullChecks + " FROM tf_unsigned_special_out(" + nullArgs + ")")) {
                assertTrue(rs.next());
                for (int i = 0; i < types.length; i++) {
                    assertEquals(rs.getBoolean(i + 1), true);
                }
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_nested_and_enum_outputs() throws Exception {
        final UdfLogicalType listOfInt = UdfLogicalType.list(UdfLogicalType.of(DuckDBColumnType.INTEGER));
        final UdfLogicalType arrayOfVarchar = UdfLogicalType.array(UdfLogicalType.of(DuckDBColumnType.VARCHAR), 2);
        final UdfLogicalType mapVarcharInt = UdfLogicalType.map(UdfLogicalType.of(DuckDBColumnType.VARCHAR),
                                                                UdfLogicalType.of(DuckDBColumnType.INTEGER));
        final UdfLogicalType structType = UdfLogicalType.struct(
            new String[] {"id", "txt"}, new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER),
                                                              UdfLogicalType.of(DuckDBColumnType.VARCHAR)});
        final UdfLogicalType unionType = UdfLogicalType.unionType(
            new String[] {"num", "txt"}, new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER),
                                                               UdfLogicalType.of(DuckDBColumnType.VARCHAR)});
        final UdfLogicalType enumType = UdfLogicalType.enumeration("small", "medium", "large");

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_nested_out", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    int rowCount = ((Number) parameters[0]).intValue();
                    return new TableBindResult(new String[] {"lst_i", "arr_txt", "kv", "s", "u", "en"},
                                               new UdfLogicalType[] {listOfInt, arrayOfVarchar, mapVarcharInt,
                                                                     structType, unionType, enumType},
                                               rowCount);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 64 && current < end; produced++, current++) {
                        Map<String, Integer> mapValue = new HashMap<>();
                        mapValue.put("k", 10 + current * 10);
                        if (current % 2 == 0) {
                            out.beginRow()
                                .append(Arrays.asList(current + 1, current + 2, current + 3))
                                .append(new String[] {"a" + current, "b" + current})
                                .append(mapValue)
                                .append(Arrays.asList(current, "row" + current))
                                .append(new AbstractMap.SimpleEntry<String, Object>("num", 100 + current))
                                .append("medium")
                                .endRow();
                        } else {
                            out.beginRow()
                                .append(Arrays.asList(current + 1, null, current + 3))
                                .append(new String[] {"a" + current, "b" + current})
                                .append(mapValue)
                                .append(Arrays.asList(current, "row" + current))
                                .append(new AbstractMap.SimpleEntry<String, Object>("txt", "u" + current))
                                .append("small")
                                .endRow();
                        }
                    }
                    st[0] = current;
                    return out.getSize();
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            try (ResultSet rs = stmt.executeQuery("SELECT list_extract(lst_i, 1), list_extract(lst_i, 2), "
                                                  + "array_extract(arr_txt, 2), list_extract(map_extract(kv, 'k'), 1), "
                                                  + "s.id, s.txt, union_tag(u), u.num, u.txt, CAST(en AS VARCHAR) "
                                                  + "FROM tf_nested_out(2) ORDER BY s.id")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertEquals(rs.getInt(2), 2);
                assertEquals(rs.getString(3), "b0");
                assertEquals(rs.getInt(4), 10);
                assertEquals(rs.getInt(5), 0);
                assertEquals(rs.getString(6), "row0");
                assertEquals(rs.getString(7), "num");
                assertEquals(rs.getInt(8), 100);
                assertEquals(rs.getObject(9), null);
                assertEquals(rs.getString(10), "medium");

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 2);
                assertEquals(rs.getObject(2), null);
                assertEquals(rs.getString(3), "b1");
                assertEquals(rs.getInt(4), 20);
                assertEquals(rs.getInt(5), 1);
                assertEquals(rs.getString(6), "row1");
                assertEquals(rs.getString(7), "txt");
                assertEquals(rs.getObject(8), null);
                assertEquals(rs.getString(9), "u1");
                assertEquals(rs.getString(10), "small");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_nested_projection_pushdown() throws Exception {
        final UdfLogicalType structType = UdfLogicalType.struct(
            new String[] {"id", "txt"}, new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER),
                                                              UdfLogicalType.of(DuckDBColumnType.VARCHAR)});
        final UdfLogicalType enumType = UdfLogicalType.enumeration("small", "large");

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            int[] materializedBySourceColumn = new int[] {0, 0, 0};
            int[][] observedProjectedColumns = new int[2][];

            conn.registerTableFunction(
                "tf_nested_projected",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        int end = ((Number) parameters[0]).intValue();
                        return new TableBindResult(
                            new String[] {"id", "nested", "en"},
                            new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER), structType, enumType},
                            end);
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        int end = ((Number) bind.getBindState()).intValue();
                        int[] projectedColumns = new int[ctx.getColumnCount()];
                        for (int i = 0; i < projectedColumns.length; i++) {
                            projectedColumns[i] = ctx.getColumnIndex(i);
                        }
                        if (observedProjectedColumns[0] == null) {
                            observedProjectedColumns[0] = projectedColumns.clone();
                        } else {
                            observedProjectedColumns[1] = projectedColumns.clone();
                        }
                        return new TableState(new Object[] {0, end, projectedColumns});
                    }

                    @Override
                    public int produce(TableState state, UdfOutputAppender out) {
                        Object[] st = (Object[]) state.getState();
                        int current = (int) st[0];
                        int end = (int) st[1];
                        int[] projectedColumns = (int[]) st[2];
                        int produced = 0;
                        for (; produced < 128 && current < end; produced++, current++) {
                            out.beginRow();
                            for (int projectedCol = 0; projectedCol < projectedColumns.length; projectedCol++) {
                                int sourceColumn = projectedColumns[projectedCol];
                                materializedBySourceColumn[sourceColumn]++;
                                if (sourceColumn == 0) {
                                    out.append(current);
                                } else if (sourceColumn == 1) {
                                    out.append(Arrays.asList(current, "p" + current));
                                } else {
                                    out.append(current % 2 == 0 ? "small" : "large");
                                }
                            }
                            out.endRow();
                        }
                        st[0] = current;
                        return out.getSize();
                    }
                },
                new TableFunctionDefinition()
                    .withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER})
                    .withProjectionPushdown(true));

            try (ResultSet rs = stmt.executeQuery("SELECT en FROM tf_nested_projected(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), i % 2 == 0 ? "small" : "large");
                }
                assertFalse(rs.next());
            }
            assertEquals(materializedBySourceColumn[0], 0);
            assertEquals(materializedBySourceColumn[1], 0);
            assertEquals(materializedBySourceColumn[2], 5);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT nested.id, nested.txt FROM tf_nested_projected(3) ORDER BY 1")) {
                for (int i = 0; i < 3; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getString(2), "p" + i);
                }
                assertFalse(rs.next());
            }
            assertEquals(materializedBySourceColumn[0], 0);
            assertEquals(materializedBySourceColumn[1], 3);
            assertEquals(materializedBySourceColumn[2], 5);
            assertNotNull(observedProjectedColumns[0]);
            assertNotNull(observedProjectedColumns[1]);
            assertEquals(observedProjectedColumns[0].length, 1);
            assertEquals(observedProjectedColumns[1].length, 1);
            assertEquals(observedProjectedColumns[0][0], 2);
            assertEquals(observedProjectedColumns[1][0], 1);
        }
    }

    public static void test_table_function_typed_outputs_streaming_chunks() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_core_stream", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    int end = ((Number) parameters[0]).intValue();
                    return new TableBindResult(new String[] {"i", "d", "txt"},
                                               new DuckDBColumnType[] {DuckDBColumnType.INTEGER,
                                                                       DuckDBColumnType.DOUBLE,
                                                                       DuckDBColumnType.VARCHAR},
                                               end);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 37 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                        out.setDouble(1, produced, current * 1.5d);
                        out.setString(2, produced, "x" + current);
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT count(*), sum(i), sum(d), count(txt) FROM tf_core_stream(5000)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 5000L);
                assertEquals(rs.getLong(2), 12_497_500L);
                assertEquals(rs.getDouble(3), 18_746_250d, 0.000001d);
                assertEquals(rs.getLong(4), 5000L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_range_java_error_propagation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("range_java_error", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               null);
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) throws Exception {
                    throw new Exception("range_java_error");
                }
            });

            assertThrows(() -> { stmt.executeQuery("SELECT * FROM range_java_error(1)"); }, SQLException.class);
        }
    }

    public static void test_table_function_projection_pushdown() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            int[] materializedBySourceColumn = new int[] {0, 0, 0};
            conn.registerTableFunction("tf_projected", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"col1", "col2", "col3"},
                                               new DuckDBColumnType[] {DuckDBColumnType.INTEGER,
                                                                       DuckDBColumnType.INTEGER,
                                                                       DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    int[] projectedColumns = new int[ctx.getColumnCount()];
                    for (int i = 0; i < projectedColumns.length; i++) {
                        projectedColumns[i] = ctx.getColumnIndex(i);
                    }
                    return new TableState(new Object[] {0, end, projectedColumns});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    Object[] st = (Object[]) state.getState();
                    int current = (int) st[0];
                    int end = (int) st[1];
                    int[] projectedColumns = (int[]) st[2];
                    int produced = 0;
                    for (; produced < 1024 && current < end; produced++, current++) {
                        for (int projectedCol = 0; projectedCol < projectedColumns.length; projectedCol++) {
                            int sourceColumn = projectedColumns[projectedCol];
                            materializedBySourceColumn[sourceColumn]++;
                            if (sourceColumn == 0) {
                                out.setInt(projectedCol, produced, current);
                            } else if (sourceColumn == 1) {
                                out.setInt(projectedCol, produced, current * 10);
                            } else {
                                out.setInt(projectedCol, produced, current * 100);
                            }
                        }
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withProjectionPushdown(true));

            try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM tf_projected(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                }
                assertFalse(rs.next());
            }
            assertEquals(materializedBySourceColumn[0], 5);
            assertEquals(materializedBySourceColumn[1], 0);
            assertEquals(materializedBySourceColumn[2], 0);
        }
    }

    public static void test_table_function_projection_pushdown_mixed_schema() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            int[] materializedBySourceColumn = new int[] {0, 0, 0, 0, 0, 0};
            int[][] observedProjectedColumns = new int[1][];

            conn.registerTableFunction("tf_projected_mixed", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"col_int", "col_txt", "col_dbl", "col_bool", "col_i64", "col_f32"},
                        new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.VARCHAR,
                                                DuckDBColumnType.DOUBLE, DuckDBColumnType.BOOLEAN,
                                                DuckDBColumnType.BIGINT, DuckDBColumnType.FLOAT},
                        ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    int[] projectedColumns = new int[ctx.getColumnCount()];
                    for (int i = 0; i < projectedColumns.length; i++) {
                        projectedColumns[i] = ctx.getColumnIndex(i);
                    }
                    observedProjectedColumns[0] = projectedColumns.clone();
                    return new TableState(new Object[] {0, end, projectedColumns});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    Object[] st = (Object[]) state.getState();
                    int current = (int) st[0];
                    int end = (int) st[1];
                    int[] projectedColumns = (int[]) st[2];
                    int produced = 0;
                    for (; produced < 256 && current < end; produced++, current++) {
                        for (int projectedCol = 0; projectedCol < projectedColumns.length; projectedCol++) {
                            int sourceColumn = projectedColumns[projectedCol];
                            materializedBySourceColumn[sourceColumn]++;
                            if (sourceColumn == 0) {
                                out.setInt(projectedCol, produced, current);
                            } else if (sourceColumn == 1) {
                                out.setString(projectedCol, produced, "s" + current);
                            } else if (sourceColumn == 2) {
                                out.setDouble(projectedCol, produced, current + 0.5d);
                            } else if (sourceColumn == 3) {
                                out.setBoolean(projectedCol, produced, current % 2 == 0);
                            } else if (sourceColumn == 4) {
                                out.setLong(projectedCol, produced, 1_000_000_000L + current);
                            } else {
                                out.setFloat(projectedCol, produced, current + 0.25f);
                            }
                        }
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withProjectionPushdown(true));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col_i64, col_txt FROM tf_projected_mixed(5) ORDER BY col_i64")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 1_000_000_000L + i);
                    assertEquals(rs.getString(2), "s" + i);
                }
                assertFalse(rs.next());
            }

            assertNotNull(observedProjectedColumns[0]);
            assertEquals(observedProjectedColumns[0].length, 2);
            int[] sortedProjected = observedProjectedColumns[0].clone();
            Arrays.sort(sortedProjected);
            assertTrue(Arrays.equals(sortedProjected, new int[] {1, 4}));
            assertEquals(materializedBySourceColumn[0], 0);
            assertEquals(materializedBySourceColumn[1], 5);
            assertEquals(materializedBySourceColumn[2], 0);
            assertEquals(materializedBySourceColumn[3], 0);
            assertEquals(materializedBySourceColumn[4], 5);
            assertEquals(materializedBySourceColumn[5], 0);

            observedProjectedColumns[0] = null;
            try (ResultSet rs = stmt.executeQuery("SELECT col_bool FROM tf_projected_mixed(5)")) {
                for (int i = 0; i < 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean(1), i % 2 == 0);
                }
                assertFalse(rs.next());
            }

            assertNotNull(observedProjectedColumns[0]);
            assertEquals(observedProjectedColumns[0].length, 1);
            assertEquals(observedProjectedColumns[0][0], 3);
            assertEquals(materializedBySourceColumn[0], 0);
            assertEquals(materializedBySourceColumn[1], 5);
            assertEquals(materializedBySourceColumn[2], 0);
            assertEquals(materializedBySourceColumn[3], 5);
            assertEquals(materializedBySourceColumn[4], 5);
            assertEquals(materializedBySourceColumn[5], 0);
        }
    }

    public static void test_table_function_thread_options_smoke() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            try (Statement setup = conn.createStatement()) {
                setup.execute("PRAGMA threads=4");
            }

            conn.registerTableFunction(
                "tf_threadsafe",
                new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                                   ((Number) parameters[0]).intValue());
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        int end = ((Number) bind.getBindState()).intValue();
                        return new TableState(new int[] {0, end});
                    }

                    @Override
                    public synchronized int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] st = (int[]) state.getState();
                        int current = st[0];
                        int end = st[1];
                        int produced = 0;
                        for (; produced < 1024 && current < end; produced++, current++) {
                            out.setInt(0, produced, current);
                        }
                        st[0] = current;
                        return produced;
                    }
                },
                new TableFunctionDefinition().withProjectionPushdown(true),
                new TableFunctionOptions().threadSafe(true).maxThreads(4));

            conn.registerTableFunction("tf_singlethread", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 1024 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionOptions().threadSafe(false).maxThreads(8));

            for (int i = 0; i < 20; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT sum(i) FROM tf_threadsafe(100000)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 4999950000L);
                }
                try (ResultSet rs = stmt.executeQuery("SELECT sum(i) FROM tf_singlethread(100000)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 4999950000L);
                }
            }
        }
    }

    public static void test_table_function_thread_options_mixed_typed_outputs() throws Exception {
        final int rowCount = 100000;
        final long expectedSumInt = ((long) rowCount * (rowCount - 1)) / 2;
        final double expectedSumDouble = expectedSumInt * 0.5d;
        final long expectedTrueCount = rowCount / 2;

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            try (Statement setup = conn.createStatement()) {
                setup.execute("PRAGMA threads=4");
            }

            conn.registerTableFunction("tf_threadsafe_mixed", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"i", "d", "b", "txt"},
                        new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.DOUBLE,
                                                DuckDBColumnType.BOOLEAN, DuckDBColumnType.VARCHAR},
                        ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public synchronized int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 1024 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                        out.setDouble(1, produced, current * 0.5d);
                        out.setBoolean(2, produced, current % 2 == 1);
                        out.setString(3, produced, "x");
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionOptions().threadSafe(true).maxThreads(4));

            conn.registerTableFunction("tf_singlethread_mixed", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"i", "d", "b", "txt"},
                        new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.DOUBLE,
                                                DuckDBColumnType.BOOLEAN, DuckDBColumnType.VARCHAR},
                        ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    int end = st[1];
                    int produced = 0;
                    for (; produced < 1024 && current < end; produced++, current++) {
                        out.setInt(0, produced, current);
                        out.setDouble(1, produced, current * 0.5d);
                        out.setBoolean(2, produced, current % 2 == 1);
                        out.setString(3, produced, "x");
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionOptions().threadSafe(false).maxThreads(8));

            for (int i = 0; i < 20; i++) {
                try (ResultSet rs =
                         stmt.executeQuery("SELECT sum(i), sum(d), sum(CASE WHEN b THEN 1 ELSE 0 END), count(txt) "
                                           + "FROM tf_threadsafe_mixed(" + rowCount + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), expectedSumInt);
                    assertEquals(rs.getDouble(2), expectedSumDouble, 0.0001d);
                    assertEquals(rs.getLong(3), expectedTrueCount);
                    assertEquals(rs.getLong(4), (long) rowCount);
                    assertFalse(rs.next());
                }
                try (ResultSet rs =
                         stmt.executeQuery("SELECT sum(i), sum(d), sum(CASE WHEN b THEN 1 ELSE 0 END), count(txt) "
                                           + "FROM tf_singlethread_mixed(" + rowCount + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), expectedSumInt);
                    assertEquals(rs.getDouble(2), expectedSumDouble, 0.0001d);
                    assertEquals(rs.getLong(3), expectedTrueCount);
                    assertEquals(rs.getLong(4), (long) rowCount);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_table_function_mixed_typed_outputs_exception_propagation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_mixed_error", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(
                        new String[] {"i", "txt"},
                        new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.VARCHAR},
                        ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    int end = ((Number) bind.getBindState()).intValue();
                    return new TableState(new int[] {0, end});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) throws Exception {
                    int[] st = (int[]) state.getState();
                    int current = st[0];
                    if (current >= 10) {
                        throw new Exception("tf_mixed_error");
                    }
                    int produced = 0;
                    for (; produced < 10 && current < st[1]; produced++, current++) {
                        out.setInt(0, produced, current);
                        out.setString(1, produced, "e" + current);
                    }
                    st[0] = current;
                    return produced;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            assertThrows(() -> { stmt.executeQuery("SELECT * FROM tf_mixed_error(100)"); }, SQLException.class);
        }
    }

    public static void test_table_function_init_exception_message_propagation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_init_error", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) throws Exception {
                    throw new Exception("tf_init_error");
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                    return 0;
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT * FROM tf_init_error(3)"); }, SQLException.class);
            assertTrue(message != null && message.contains("tf_init_error"));
        }
    }

    public static void test_table_function_main_exception_message_propagation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerTableFunction("tf_main_error", new org.duckdb.udf.TableFunction() {
                @Override
                public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                    return new TableBindResult(new String[] {"i"}, new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                               ((Number) parameters[0]).intValue());
                }

                @Override
                public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                    return new TableState(new int[] {0, ((Number) bind.getBindState()).intValue()});
                }

                @Override
                public int produce(TableState state, org.duckdb.UdfOutputAppender out) throws Exception {
                    throw new Exception("tf_main_error");
                }
            }, new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.INTEGER}));

            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT * FROM tf_main_error(3)"); }, SQLException.class);
            assertTrue(message != null && message.contains("tf_main_error"));
        }
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    public static void test_udf_lifecycle_hardening_repetition() throws Exception {
        long baselineUsedHeap = -1;
        final int iterations = 250;
        for (int i = 0; i < iterations; i++) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {
                conn.registerScalarUdf("life_scalar", (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        out.setInt(row, args[0].getInt(row) + 1);
                    }
                });
                conn.registerTableFunction("life_table", new org.duckdb.udf.TableFunction() {
                    @Override
                    public TableBindResult bind(org.duckdb.udf.BindContext ctx, Object[] parameters) {
                        return new TableBindResult(new String[] {"i"},
                                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                                   ((Number) parameters[0]).intValue());
                    }

                    @Override
                    public TableState init(org.duckdb.udf.InitContext ctx, TableBindResult bind) {
                        int end = ((Number) bind.getBindState()).intValue();
                        return new TableState(new int[] {0, end});
                    }

                    @Override
                    public int produce(TableState state, org.duckdb.UdfOutputAppender out) {
                        int[] st = (int[]) state.getState();
                        int current = st[0];
                        int end = st[1];
                        int produced = 0;
                        for (; produced < 1024 && current < end; produced++, current++) {
                            out.setInt(0, produced, current);
                        }
                        st[0] = current;
                        return produced;
                    }
                });

                try (ResultSet rs = stmt.executeQuery("SELECT sum(life_scalar(i::INTEGER)) FROM range(1000) t(i)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 500500L);
                }
                try (ResultSet rs = stmt.executeQuery("SELECT sum(i) FROM life_table(1000)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 499500L);
                }
            }
            if (i % 50 == 0) {
                System.gc();
            }
            if (i == 25) {
                System.gc();
                Thread.sleep(20);
                baselineUsedHeap = usedHeapBytes();
            }
        }

        System.gc();
        Thread.sleep(50);
        long finalUsedHeap = usedHeapBytes();
        assertTrue(baselineUsedHeap > 0);
        // Heuristic guardrail: allow the JVM heap to fluctuate, but not grow without bound.
        assertTrue(finalUsedHeap <= baselineUsedHeap + (64L * 1024L * 1024L));
    }

    public static void test_java_scalar_udf_add_one() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add_one", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    if (args[0].isNull(row)) {
                        out.setNull(row);
                    } else {
                        out.setInt(row, args[0].getInt(row) + 1);
                    }
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add_one(i::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 500500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_output_writer_api() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add_one_writer", new org.duckdb.udf.ScalarUdf() {
                @Override
                public void apply(org.duckdb.udf.UdfContext ctx, UdfReader[] args, UdfScalarWriter out, int rowCount) {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row)) {
                            out.setNull(row);
                        } else {
                            out.setInt(row, args[0].getInt(row) + 1);
                        }
                    }
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add_one_writer(i::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 500500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_output_writer_object_methods() throws Exception {
        UdfLogicalType decimal18_3 = UdfLogicalType.decimal(18, 3);
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("writer_obj_decimal", new UdfLogicalType[] {decimal18_3}, decimal18_3,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setObject(row, args[0].isNull(row) ? null : args[0].getBigDecimal(row));
                                       }
                                   });
            conn.registerScalarUdf("writer_obj_date", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.DATE)},
                                   UdfLogicalType.of(DuckDBColumnType.DATE), (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setLocalDate(row,
                                                            args[0].isNull(row) ? null : args[0].getLocalDate(row));
                                       }
                                   });
            conn.registerScalarUdf("writer_obj_time", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.TIME)},
                                   UdfLogicalType.of(DuckDBColumnType.TIME), (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setLocalTime(row,
                                                            args[0].isNull(row) ? null : args[0].getLocalTime(row));
                                       }
                                   });
            conn.registerScalarUdf(
                "writer_obj_timetz", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.TIME_WITH_TIME_ZONE)},
                UdfLogicalType.of(DuckDBColumnType.TIME_WITH_TIME_ZONE), (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        out.setOffsetTime(row, args[0].isNull(row) ? null : args[0].getOffsetTime(row));
                    }
                });
            conn.registerScalarUdf(
                "writer_obj_ts", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.TIMESTAMP)},
                UdfLogicalType.of(DuckDBColumnType.TIMESTAMP), (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        out.setLocalDateTime(row, args[0].isNull(row) ? null : args[0].getLocalDateTime(row));
                    }
                });
            conn.registerScalarUdf(
                "writer_obj_tstz", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)},
                UdfLogicalType.of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE), (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        out.setOffsetDateTime(row, args[0].isNull(row) ? null : args[0].getOffsetDateTime(row));
                    }
                });
            conn.registerScalarUdf("writer_obj_uuid", new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.UUID)},
                                   UdfLogicalType.of(DuckDBColumnType.UUID), (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setUUID(row, args[0].isNull(row) ? null : args[0].getUUID(row));
                                       }
                                   });

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT "
                    + "writer_obj_decimal(123.450::DECIMAL(18,3)) = 123.450::DECIMAL(18,3), "
                    + "writer_obj_decimal(NULL::DECIMAL(18,3)) IS NULL, "
                    + "writer_obj_date(DATE '2024-01-03') = DATE '2024-01-03', "
                    + "writer_obj_time(TIME '01:02:03.123456') = TIME '01:02:03.123456', "
                    + "writer_obj_timetz(TIME WITH TIME ZONE '01:02:03+05:30') = TIME WITH TIME ZONE '01:02:03+05:30', "
                    + "writer_obj_ts(TIMESTAMP '2024-01-03 04:05:06.123456') = TIMESTAMP '2024-01-03 04:05:06.123456', "
                    + "writer_obj_tstz(TIMESTAMP WITH TIME ZONE '2024-01-03 04:05:06+00') = TIMESTAMP WITH TIME ZONE "
                    + "'2024-01-03 04:05:06+00', "
                    + "writer_obj_uuid('" + uuid + "'::UUID) = '" + uuid + "'::UUID")) {
                assertTrue(rs.next());
                for (int col = 1; col <= 8; col++) {
                    assertEquals(rs.getBoolean(col), true);
                }
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_reader_api() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add_one_reader", new org.duckdb.udf.ScalarUdf() {
                @Override
                public void apply(org.duckdb.udf.UdfContext ctx, UdfReader[] args, UdfScalarWriter out, int rowCount) {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row)) {
                            out.setNull(row);
                        } else {
                            out.setInt(row, args[0].getInt(row) + 1);
                        }
                    }
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add_one_reader(i::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 500500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_reader_object_accessors() throws Exception {
        UdfLogicalType decimal18_3 = UdfLogicalType.decimal(18, 3);
        UUID expectedUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        LocalDate expectedDate = LocalDate.of(2024, 1, 3);
        LocalTime expectedTime = LocalTime.of(1, 2, 3, 123456000);
        OffsetTime expectedOffsetTime = OffsetTime.parse("01:02:03+05:30");
        LocalDateTime expectedTimestamp = LocalDateTime.of(2024, 1, 3, 4, 5, 6, 123456000);
        Instant expectedTzTimestampInstant = Instant.parse("2024-01-03T04:05:06Z");

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf(
                "reader_objects_ok",
                new UdfLogicalType[] {decimal18_3, UdfLogicalType.of(DuckDBColumnType.DATE),
                                      UdfLogicalType.of(DuckDBColumnType.TIME),
                                      UdfLogicalType.of(DuckDBColumnType.TIME_WITH_TIME_ZONE),
                                      UdfLogicalType.of(DuckDBColumnType.TIMESTAMP),
                                      UdfLogicalType.of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE),
                                      UdfLogicalType.of(DuckDBColumnType.UUID)},
                UdfLogicalType.of(DuckDBColumnType.BOOLEAN), (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row) || args[1].isNull(row) || args[2].isNull(row) || args[3].isNull(row) ||
                            args[4].isNull(row) || args[5].isNull(row) || args[6].isNull(row)) {
                            out.setNull(row);
                            continue;
                        }

                        boolean ok = args[0].getBigDecimal(row).compareTo(new BigDecimal("123.450")) == 0;
                        ok = ok && expectedDate.equals(args[1].getLocalDate(row));
                        java.util.Date dateValue = args[1].getDate(row);
                        ok = ok && dateValue instanceof java.sql.Date &&
                             expectedDate.equals(((java.sql.Date) dateValue).toLocalDate());
                        ok = ok && expectedTime.equals(args[2].getLocalTime(row));
                        ok = ok && expectedOffsetTime.equals(args[3].getOffsetTime(row));
                        ok = ok && expectedTimestamp.equals(args[4].getLocalDateTime(row));
                        OffsetDateTime offsetDateTime = args[5].getOffsetDateTime(row);
                        ok = ok && offsetDateTime != null &&
                             expectedTzTimestampInstant.equals(offsetDateTime.toInstant());
                        ok = ok && expectedUuid.equals(args[6].getUUID(row));

                        out.setBoolean(row, ok);
                    }
                });

            try (ResultSet rs = stmt.executeQuery("SELECT reader_objects_ok(123.450::DECIMAL(18,3), DATE '2024-01-03', "
                                                  + "TIME '01:02:03.123456', '01:02:03+05:30'::TIME WITH TIME ZONE, "
                                                  + "TIMESTAMP '2024-01-03 04:05:06.123456', "
                                                  + "'2024-01-03 04:05:06+00'::TIMESTAMP WITH TIME ZONE, "
                                                  + "'550e8400-e29b-41d4-a716-446655440000'::UUID), "
                                                  + "reader_objects_ok(NULL::DECIMAL(18,3), DATE '2024-01-03', "
                                                  + "TIME '01:02:03.123456', '01:02:03+05:30'::TIME WITH TIME ZONE, "
                                                  + "TIMESTAMP '2024-01-03 04:05:06.123456', "
                                                  + "'2024-01-03 04:05:06+00'::TIMESTAMP WITH TIME ZONE, "
                                                  + "'550e8400-e29b-41d4-a716-446655440000'::UUID) IS NULL")) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), true);
                assertEquals(rs.getBoolean(2), true);
                assertFalse(rs.next());
            }

            conn.registerScalarUdf("reader_object_type_error", new DuckDBColumnType[] {DuckDBColumnType.INTEGER},
                                   DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           args[0].getUUID(row);
                                           out.setInt(row, 0);
                                       }
                                   });

            assertThrows(() -> {
                try (ResultSet rs = stmt.executeQuery("SELECT reader_object_type_error(1)")) {
                    rs.next();
                }
            }, SQLException.class);
        }
    }

    public static void test_java_scalar_udf_logical_type_registration() throws Exception {
        UdfLogicalType decimal18_2 = UdfLogicalType.decimal(18, 2);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add_one_logical",
                                   new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.INTEGER)},
                                   UdfLogicalType.of(DuckDBColumnType.INTEGER), (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setInt(row, args[0].getInt(row) + 1);
                                           }
                                       }
                                   });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add_one_logical(i::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 500500L);
                assertFalse(rs.next());
            }

            conn.registerScalarUdf("dec_identity_logical", new UdfLogicalType[] {decimal18_2}, decimal18_2,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setBigDecimal(row, args[0].getBigDecimal(row));
                                           }
                                       }
                                   });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT dec_identity_logical(42.75::DECIMAL(18,2)) = 42.75::DECIMAL(18,2)")) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), true);
                assertFalse(rs.next());
            }

            assertThrows(
                ()
                    -> conn.registerScalarUdf(
                        "bad_logical_arg",
                        new UdfLogicalType[] {UdfLogicalType.list(UdfLogicalType.of(DuckDBColumnType.INTEGER))},
                        UdfLogicalType.of(DuckDBColumnType.INTEGER), (ctx, args, out, rowCount) -> {}),
                SQLFeatureNotSupportedException.class);
        }
    }

    public static void test_udf_logical_type_decimal_factory_validation() throws Exception {
        UdfLogicalType decimal = UdfLogicalType.decimal(18, 2);
        assertEquals(decimal.getType(), DuckDBColumnType.DECIMAL);
        assertEquals(decimal.getDecimalWidth(), 18);
        assertEquals(decimal.getDecimalScale(), 2);

        assertThrows(() -> { UdfLogicalType.decimal(0, 0); }, IllegalArgumentException.class);
        assertThrows(() -> { UdfLogicalType.decimal(39, 0); }, IllegalArgumentException.class);
        assertThrows(() -> { UdfLogicalType.decimal(18, -1); }, IllegalArgumentException.class);
        assertThrows(() -> { UdfLogicalType.decimal(18, 19); }, IllegalArgumentException.class);
    }

    public static void test_java_scalar_udf_decimal_exact_width_scale_and_overflow() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            int[] widths = new int[] {4, 9, 18, 30};
            int[] scales = new int[] {1, 4, 6, 10};
            String[] literals =
                new String[] {"-99.9", "12345.6789", "123456789012.123456", "12345678901234567890.1234567890"};

            for (int i = 0; i < widths.length; i++) {
                int width = widths[i];
                int scale = scales[i];
                UdfLogicalType decimalType = UdfLogicalType.decimal(width, scale);
                String fnName = "f_decimal_exact_" + width + "_" + scale;
                String typedLiteral = literals[i] + "::DECIMAL(" + width + "," + scale + ")";

                conn.registerScalarUdf(fnName, new UdfLogicalType[] {decimalType}, decimalType,
                                       (ctx, args, out, rowCount) -> {
                                           for (int row = 0; row < rowCount; row++) {
                                               if (args[0].isNull(row)) {
                                                   out.setNull(row);
                                               } else {
                                                   out.setBigDecimal(row, args[0].getBigDecimal(row));
                                               }
                                           }
                                       });

                try (ResultSet rs = stmt.executeQuery("SELECT CAST(" + fnName + "(" + typedLiteral + ") AS VARCHAR), "
                                                      + "CAST(" + typedLiteral + " AS VARCHAR), " + fnName +
                                                      "(NULL::DECIMAL(" + width + "," + scale + ")) IS NULL")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), rs.getString(2));
                    assertEquals(rs.getBoolean(3), true);
                    assertFalse(rs.next());
                }
            }

            UdfLogicalType decimal4_1 = UdfLogicalType.decimal(4, 1);
            conn.registerScalarUdf("f_decimal_overflow_4_1", new UdfLogicalType[] {decimal4_1}, decimal4_1,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setBigDecimal(row, new BigDecimal("1000.0"));
                                           }
                                       }
                                   });

            assertThrows(() -> {
                try (ResultSet rs = stmt.executeQuery("SELECT f_decimal_overflow_4_1(1.0::DECIMAL(4,1))")) {
                    rs.next();
                }
            }, SQLException.class);
        }
    }

    public static void test_java_scalar_udf_decimal_boundary_values_per_width_scale() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            int[] widths = new int[] {4, 9, 18, 30, 38};
            int[] scales = new int[] {1, 4, 6, 10, 10};
            String[] maxValues =
                new String[] {"999.9", "99999.9999", "999999999999.999999", "99999999999999999999.9999999999",
                              "9999999999999999999999999999.9999999999"};
            String[] minValues =
                new String[] {"-999.9", "-99999.9999", "-999999999999.999999", "-99999999999999999999.9999999999",
                              "-9999999999999999999999999999.9999999999"};

            for (int i = 0; i < widths.length; i++) {
                int width = widths[i];
                int scale = scales[i];
                UdfLogicalType decimalType = UdfLogicalType.decimal(width, scale);
                String fnName = "f_decimal_boundaries_" + width + "_" + scale;
                String maxTyped = maxValues[i] + "::DECIMAL(" + width + "," + scale + ")";
                String minTyped = minValues[i] + "::DECIMAL(" + width + "," + scale + ")";

                conn.registerScalarUdf(fnName, new UdfLogicalType[] {decimalType}, decimalType,
                                       (ctx, args, out, rowCount) -> {
                                           for (int row = 0; row < rowCount; row++) {
                                               if (args[0].isNull(row)) {
                                                   out.setNull(row);
                                               } else {
                                                   out.setBigDecimal(row, args[0].getBigDecimal(row));
                                               }
                                           }
                                       });

                try (ResultSet rs = stmt.executeQuery("SELECT CAST(" + fnName + "(" + maxTyped + ") AS VARCHAR), "
                                                      + "CAST(" + maxTyped + " AS VARCHAR), CAST(" + fnName + "(" +
                                                      minTyped + ") AS VARCHAR), CAST(" + minTyped + " AS VARCHAR)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), rs.getString(2));
                    assertEquals(rs.getString(3), rs.getString(4));
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_reader_object_accessors_null_special_handling() throws Exception {
        UdfLogicalType decimal18_3 = UdfLogicalType.decimal(18, 3);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("reader_objects_nulls",
                                   new UdfLogicalType[] {decimal18_3, UdfLogicalType.of(DuckDBColumnType.DATE),
                                                         UdfLogicalType.of(DuckDBColumnType.TIME),
                                                         UdfLogicalType.of(DuckDBColumnType.TIME_WITH_TIME_ZONE),
                                                         UdfLogicalType.of(DuckDBColumnType.TIMESTAMP),
                                                         UdfLogicalType.of(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE),
                                                         UdfLogicalType.of(DuckDBColumnType.UUID)},
                                   UdfLogicalType.of(DuckDBColumnType.BOOLEAN), (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           boolean ok = args[0].getBigDecimal(row) == null;
                                           ok = ok && args[1].getDate(row) == null;
                                           ok = ok && args[1].getLocalDate(row) == null;
                                           ok = ok && args[2].getLocalTime(row) == null;
                                           ok = ok && args[3].getOffsetTime(row) == null;
                                           ok = ok && args[4].getLocalDateTime(row) == null;
                                           ok = ok && args[5].getOffsetDateTime(row) == null;
                                           ok = ok && args[6].getUUID(row) == null;
                                           out.setBoolean(row, ok);
                                       }
                                   }, new UdfOptions().nullSpecialHandling(true));

            try (ResultSet rs = stmt.executeQuery("SELECT reader_objects_nulls(NULL::DECIMAL(18,3), NULL::DATE, "
                                                  + "NULL::TIME, NULL::TIME WITH TIME ZONE, NULL::TIMESTAMP, "
                                                  + "NULL::TIMESTAMP WITH TIME ZONE, NULL::UUID)")) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), true);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_null_default_handling() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_default_null", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, 5);
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT f_default_null(NULL::INTEGER)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_null_special_handling() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_special_null", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    if (!args[0].isNull(row)) {
                        throw new IllegalStateException("Expected NULL input row");
                    }
                    out.setInt(row, 5);
                }
            }, new UdfOptions().nullSpecialHandling(true));

            try (ResultSet rs = stmt.executeQuery("SELECT f_special_null(NULL::INTEGER)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 5);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_exception_abort() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_throw_abort",
                                   (ctx, args, out, rowCount) -> { throw new RuntimeException("kaboom"); });

            assertThrows(() -> {
                stmt.executeQuery("SELECT f_throw_abort(i::INTEGER) FROM range(3) t(i)");
            }, SQLException.class);
        }
    }

    public static void test_java_scalar_udf_exception_return_null() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_throw_null", (ctx, args, out, rowCount) -> {
                throw new RuntimeException("kaboom");
            }, new UdfOptions().returnNullOnException(true));

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) total, count(f_throw_null(i::INTEGER)) non_null FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1000L);
                assertEquals(rs.getLong(2), 0L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_deterministic_caches_constant_input() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_rand_det", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, ThreadLocalRandom.current().nextInt());
                }
            });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT count(DISTINCT f_rand_det(1::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_volatile_varies_per_row() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("f_rand_vol", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, ThreadLocalRandom.current().nextInt());
                }
            }, new UdfOptions().deterministic(false));

            try (ResultSet rs =
                     stmt.executeQuery("SELECT count(DISTINCT f_rand_vol(1::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertTrue(rs.getLong(1) > 1L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_add2() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add2", new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER},
                                   DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row) || args[1].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setInt(row, args[0].getInt(row) + args[1].getInt(row));
                                           }
                                       }
                                   });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add2(i::INTEGER, 10::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 509500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_mul3() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf(
                "mul3",
                new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER},
                DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row) || args[1].isNull(row) || args[2].isNull(row)) {
                            out.setNull(row);
                        } else {
                            out.setInt(row, args[0].getInt(row) * args[1].getInt(row) * args[2].getInt(row));
                        }
                    }
                });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT sum(mul3(i::INTEGER, 2::INTEGER, 3::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 2997000L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_registration_overloads_column_types() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("plus1_overload", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + 1);
                                       }
                                   });

            conn.registerScalarUdf("sum2_overload", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                   DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + args[1].getInt(row));
                                       }
                                   });

            conn.registerScalarUdf("sum3_overload", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                   DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row,
                                                      args[0].getInt(row) + args[1].getInt(row) + args[2].getInt(row));
                                       }
                                   });

            conn.registerScalarUdf("sum4_overload", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                   DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + args[1].getInt(row) +
                                                               args[2].getInt(row) + args[3].getInt(row));
                                       }
                                   });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT plus1_overload(41::INTEGER), sum2_overload(1::INTEGER,2::INTEGER), "
                                       + "sum3_overload(1::INTEGER,2::INTEGER,3::INTEGER), "
                                       + "sum4_overload(1::INTEGER,2::INTEGER,3::INTEGER,4::INTEGER)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getInt(2), 3);
                assertEquals(rs.getInt(3), 6);
                assertEquals(rs.getInt(4), 10);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_registration_overloads_logical_types() throws Exception {
        UdfLogicalType integerType = UdfLogicalType.of(DuckDBColumnType.INTEGER);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("logical_sum2_overload", integerType, integerType, integerType,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + args[1].getInt(row));
                                       }
                                   });

            conn.registerScalarUdf("logical_sum3_overload", integerType, integerType, integerType, integerType,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row,
                                                      args[0].getInt(row) + args[1].getInt(row) + args[2].getInt(row));
                                       }
                                   });

            conn.registerScalarUdf("logical_sum4_overload", integerType, integerType, integerType, integerType,
                                   integerType, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + args[1].getInt(row) +
                                                               args[2].getInt(row) + args[3].getInt(row));
                                       }
                                   });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT logical_sum2_overload(1::INTEGER,2::INTEGER), "
                                       + "logical_sum3_overload(1::INTEGER,2::INTEGER,3::INTEGER), "
                                       + "logical_sum4_overload(1::INTEGER,2::INTEGER,3::INTEGER,4::INTEGER)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 3);
                assertEquals(rs.getInt(2), 6);
                assertEquals(rs.getInt(3), 10);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_arity_validation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add2_arity_check",
                                   new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER},
                                   DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setInt(row, args[0].getInt(row) + args[1].getInt(row));
                                       }
                                   });

            assertThrows(() -> { stmt.executeQuery("SELECT add2_arity_check(1::INTEGER)"); }, SQLException.class);
        }
    }

    public static void test_java_scalar_udf_zero_arguments_registration() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("zero_const", DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                assertEquals(args.length, 0);
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, 42);
                }
            });

            conn.registerScalarUdf("zero_throw_null",
                                   UdfLogicalType.of(DuckDBColumnType.INTEGER), (ctx, args, out, rowCount) -> {
                                       throw new RuntimeException("kaboom");
                                   }, new UdfOptions().returnNullOnException(true));

            try (ResultSet rs = stmt.executeQuery("SELECT sum(zero_const()) FROM range(100)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 4200L);
                assertFalse(rs.next());
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT count(*) total, count(zero_throw_null()) non_null FROM range(100)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 100L);
                assertEquals(rs.getLong(2), 0L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_varargs_registration() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdfVarArgs("sum_varargs", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                          (ctx, args, out, rowCount) -> {
                                              for (int row = 0; row < rowCount; row++) {
                                                  int sum = 0;
                                                  boolean anyNull = false;
                                                  for (UdfReader arg : args) {
                                                      if (arg.isNull(row)) {
                                                          anyNull = true;
                                                          break;
                                                      }
                                                      sum += arg.getInt(row);
                                                  }
                                                  if (anyNull) {
                                                      out.setNull(row);
                                                  } else {
                                                      out.setInt(row, sum);
                                                  }
                                              }
                                          });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(sum_varargs()) FROM range(100)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 0L);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT sum(sum_varargs(i::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 499500L);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT sum(sum_varargs(i::INTEGER, 1::INTEGER, 2::INTEGER)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 502500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_varargs_registration_validation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(()
                             -> conn.registerScalarUdf(
                                 "bad_varargs",
                                 new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER},
                                 DuckDBColumnType.INTEGER,
                                 (ctx, args, out, rowCount)
                                     -> {
                                     for (int row = 0; row < rowCount; row++) {
                                         out.setInt(row, 0);
                                     }
                                 },
                                 new UdfOptions().varArgs(true)),
                         SQLException.class);
        }
    }

    public static void test_java_scalar_udf_java_class_type_mapper_registration() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("class_plus1", Integer.class, Integer.class, (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, args[0].getInt(row) + 1);
                }
            });

            conn.registerScalarUdf("class_concat", new Class<?>[] {String.class, String.class}, String.class,
                                   (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           out.setString(row, args[0].getString(row) + args[1].getString(row));
                                       }
                                   });

            conn.registerScalarUdf("class_zero", Integer.class, (ctx, args, out, rowCount) -> {
                assertEquals(args.length, 0);
                for (int row = 0; row < rowCount; row++) {
                    out.setInt(row, 7);
                }
            });

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT class_plus1(41::INTEGER), class_concat('a','b'), sum(class_zero()) FROM range(10)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(2), "ab");
                assertEquals(rs.getLong(3), 70L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_java_class_type_mapper_decimal_requires_explicit_logical_type()
        throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(()
                             -> conn.registerScalarUdf("class_decimal_forbidden", BigDecimal.class, BigDecimal.class,
                                                       (ctx, args, out, rowCount) -> {
                                                           for (int row = 0; row < rowCount; row++) {
                                                               out.setBigDecimal(row, BigDecimal.ONE);
                                                           }
                                                       }),
                         SQLException.class);
        }
    }

    public static void test_java_scalar_udf_ergonomic_registration_preserves_options_semantics() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("class_null_special_opt", Integer.class,
                                   Integer.class, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row)) {
                                               out.setInt(row, 99);
                                           } else {
                                               out.setInt(row, args[0].getInt(row));
                                           }
                                       }
                                   }, new UdfOptions().nullSpecialHandling(true));

            conn.registerScalarUdfVarArgs("varargs_throw_opt", DuckDBColumnType.INTEGER,
                                          DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                                              throw new RuntimeException("kaboom");
                                          }, new UdfOptions().returnNullOnException(true).deterministic(false));

            try (ResultSet rs = stmt.executeQuery("SELECT class_null_special_opt(NULL::INTEGER)")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 99);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) total, count(varargs_throw_opt(i::INTEGER)) non_null FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1000L);
                assertEquals(rs.getLong(2), 0L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_arity_above_four() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf(
                "sum5",
                new DuckDBColumnType[] {DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                        DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER},
                DuckDBColumnType.INTEGER, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row) || args[1].isNull(row) || args[2].isNull(row) || args[3].isNull(row) ||
                            args[4].isNull(row)) {
                            out.setNull(row);
                        } else {
                            out.setInt(row, args[0].getInt(row) + args[1].getInt(row) + args[2].getInt(row) +
                                                args[3].getInt(row) + args[4].getInt(row));
                        }
                    }
                });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(sum5(i::INTEGER, 1, 2, 3, 4)) FROM range(1000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 509500L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_core_type_registration_surface() throws Exception {
        DuckDBColumnType[] unsupportedTypes = new DuckDBColumnType[] {
            DuckDBColumnType.INTERVAL, DuckDBColumnType.LIST,  DuckDBColumnType.ARRAY, DuckDBColumnType.STRUCT,
            DuckDBColumnType.MAP,      DuckDBColumnType.UNION, DuckDBColumnType.ENUM};

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType supportedType : scalarCoreTypes()) {
                String fnName = "f_supported_" + supportedType.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {supportedType}, supportedType,
                                       (ctx, args, out, rowCount) -> {
                                           for (int row = 0; row < rowCount; row++) {
                                               out.setNull(row);
                                           }
                                       });
                try (ResultSet rs =
                         stmt.executeQuery("SELECT " + fnName + "(" + nonNullLiteralForType(supportedType) + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1), null);
                    assertFalse(rs.next());
                }
            }

            for (DuckDBColumnType unsupportedType : unsupportedTypes) {
                String fnName = "f_unsupported_" + unsupportedType.name().toLowerCase();
                assertThrows(() -> {
                    conn.registerScalarUdf(fnName, new DuckDBColumnType[] {unsupportedType}, unsupportedType,
                                           (ctx, args, out, rowCount) -> {});
                }, SQLFeatureNotSupportedException.class);
            }
        }
    }

    public static void test_java_scalar_udf_extended_type_registration_surface() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType supportedType : scalarExtendedTypes()) {
                String fnName = "f_supported_ext_" + supportedType.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {supportedType}, supportedType,
                                       (ctx, args, out, rowCount) -> {
                                           for (int row = 0; row < rowCount; row++) {
                                               out.setNull(row);
                                           }
                                       });
                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" +
                                                      nonNullLiteralForExtendedType(supportedType) + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1), null);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_unsigned_and_special_type_registration_surface() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType supportedType : scalarUnsignedAndSpecialTypes()) {
                String fnName = "f_supported_unsigned_special_" + supportedType.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {supportedType}, supportedType,
                                       (ctx, args, out, rowCount) -> {
                                           for (int row = 0; row < rowCount; row++) {
                                               out.setNull(row);
                                           }
                                       });
                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" +
                                                      nonNullLiteralForUnsignedAndSpecialType(supportedType) + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1), null);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_extended_roundtrip_and_nulls() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarExtendedTypes()) {
                String fnName = "f_identity_ext_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row)) {
                            out.setNull(row);
                            continue;
                        }
                        switch (type) {
                        case DECIMAL:
                            out.setBigDecimal(row, args[0].getBigDecimal(row));
                            break;
                        case BLOB:
                            out.setBytes(row, args[0].getBytes(row));
                            break;
                        case DATE:
                            out.setInt(row, args[0].getInt(row));
                            break;
                        case TIME:
                        case TIME_NS:
                        case TIMESTAMP:
                        case TIMESTAMP_S:
                        case TIMESTAMP_MS:
                        case TIMESTAMP_NS:
                            out.setLong(row, args[0].getLong(row));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected extended type: " + type);
                        }
                    }
                });

                String literal = nonNullLiteralForExtendedType(type);
                String nullLiteral = nullLiteralForType(type);
                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" + literal + ") = " + literal + ", " +
                                                      fnName + "(" + nullLiteral + ") IS NULL")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean(1), true);
                    assertEquals(rs.getBoolean(2), true);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_unsigned_and_special_roundtrip_and_nulls() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarUnsignedAndSpecialTypes()) {
                String fnName = "f_identity_unsigned_special_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row)) {
                            out.setNull(row);
                            continue;
                        }
                        switch (type) {
                        case UTINYINT:
                        case USMALLINT:
                            out.setInt(row, args[0].getInt(row));
                            break;
                        case UINTEGER:
                        case UBIGINT:
                        case TIME_WITH_TIME_ZONE:
                        case TIMESTAMP_WITH_TIME_ZONE:
                            out.setLong(row, args[0].getLong(row));
                            break;
                        case HUGEINT:
                        case UHUGEINT:
                        case UUID:
                            out.setBytes(row, args[0].getBytes(row));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected unsigned/special type: " + type);
                        }
                    }
                });

                String literal = nonNullLiteralForUnsignedAndSpecialType(type);
                String nullLiteral = nullLiteralForType(type);
                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" + literal + ") = " + literal + ", " +
                                                      fnName + "(" + nullLiteral + ") IS NULL")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean(1), true);
                    assertEquals(rs.getBoolean(2), true);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_core_roundtrip_and_nulls() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarCoreTypes()) {
                String fnName = "f_identity_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (args[0].isNull(row)) {
                            out.setNull(row);
                            continue;
                        }
                        switch (type) {
                        case BOOLEAN:
                            out.setBoolean(row, args[0].getBoolean(row));
                            break;
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                            out.setInt(row, args[0].getInt(row));
                            break;
                        case BIGINT:
                            out.setLong(row, args[0].getLong(row));
                            break;
                        case FLOAT:
                            out.setFloat(row, args[0].getFloat(row));
                            break;
                        case DOUBLE:
                            out.setDouble(row, args[0].getDouble(row));
                            break;
                        case VARCHAR:
                            out.setString(row, args[0].getString(row));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected core type: " + type);
                        }
                    }
                });

                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" + nonNullLiteralForType(type) + "), " +
                                                      fnName + "(" + nullLiteralForType(type) + ")")) {
                    assertTrue(rs.next());
                    switch (type) {
                    case BOOLEAN:
                        assertEquals(rs.getBoolean(1), true);
                        break;
                    case TINYINT:
                        assertEquals(rs.getInt(1), 7);
                        break;
                    case SMALLINT:
                        assertEquals(rs.getInt(1), 32000);
                        break;
                    case INTEGER:
                        assertEquals(rs.getInt(1), 123456);
                        break;
                    case BIGINT:
                        assertEquals(rs.getLong(1), 9876543210L);
                        break;
                    case FLOAT:
                        assertEquals(rs.getFloat(1), 1.25f, 0.0001f);
                        break;
                    case DOUBLE:
                        assertEquals(rs.getDouble(1), 2.5d, 0.0000001d);
                        break;
                    case VARCHAR:
                        assertEquals(rs.getString(1), "duck");
                        break;
                    default:
                        throw new IllegalStateException("Unexpected core type: " + type);
                    }
                    assertEquals(rs.getObject(2), null);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_core_null_special_handling() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarCoreTypes()) {
                String fnName = "f_special_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type, (ctx, args, out, rowCount) -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (!args[0].isNull(row)) {
                            throw new IllegalStateException("Expected NULL input row");
                        }
                        switch (type) {
                        case BOOLEAN:
                            out.setBoolean(row, true);
                            break;
                        case TINYINT:
                            out.setInt(row, 12);
                            break;
                        case SMALLINT:
                            out.setInt(row, 1234);
                            break;
                        case INTEGER:
                            out.setInt(row, 123456);
                            break;
                        case BIGINT:
                            out.setLong(row, 123456789L);
                            break;
                        case FLOAT:
                            out.setFloat(row, 9.25f);
                            break;
                        case DOUBLE:
                            out.setDouble(row, 19.5d);
                            break;
                        case VARCHAR:
                            out.setString(row, "null-special");
                            break;
                        default:
                            throw new IllegalStateException("Unexpected core type: " + type);
                        }
                    }
                }, new UdfOptions().nullSpecialHandling(true));

                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" + nullLiteralForType(type) + ")")) {
                    assertTrue(rs.next());
                    switch (type) {
                    case BOOLEAN:
                        assertEquals(rs.getBoolean(1), true);
                        break;
                    case TINYINT:
                        assertEquals(rs.getInt(1), 12);
                        break;
                    case SMALLINT:
                        assertEquals(rs.getInt(1), 1234);
                        break;
                    case INTEGER:
                        assertEquals(rs.getInt(1), 123456);
                        break;
                    case BIGINT:
                        assertEquals(rs.getLong(1), 123456789L);
                        break;
                    case FLOAT:
                        assertEquals(rs.getFloat(1), 9.25f, 0.0001f);
                        break;
                    case DOUBLE:
                        assertEquals(rs.getDouble(1), 19.5d, 0.0000001d);
                        break;
                    case VARCHAR:
                        assertEquals(rs.getString(1), "null-special");
                        break;
                    default:
                        throw new IllegalStateException("Unexpected core type: " + type);
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_core_exception_return_null() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarCoreTypes()) {
                String fnName = "f_throw_null_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type, (ctx, args, out, rowCount) -> {
                    throw new RuntimeException("kaboom");
                }, new UdfOptions().returnNullOnException(true));

                try (ResultSet rs = stmt.executeQuery("SELECT " + fnName + "(" + nonNullLiteralForType(type) + ")")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1), null);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_java_scalar_udf_core_exception_abort() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType type : scalarCoreTypes()) {
                String fnName = "f_throw_abort_" + type.name().toLowerCase();
                conn.registerScalarUdf(fnName, new DuckDBColumnType[] {type}, type,
                                       (ctx, args, out, rowCount) -> { throw new RuntimeException("kaboom"); });
                assertThrows(() -> {
                    stmt.executeQuery("SELECT " + fnName + "(" + nonNullLiteralForType(type) + ")");
                }, SQLException.class);
            }
        }
    }

    public static void test_java_scalar_udf_reverse_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("reverse_java", new DuckDBColumnType[] {DuckDBColumnType.VARCHAR},
                                   DuckDBColumnType.VARCHAR, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setString(
                                                   row, new StringBuilder(args[0].getString(row)).reverse().toString());
                                           }
                                       }
                                   });

            try (ResultSet rs =
                     stmt.executeQuery("SELECT reverse_java('abcd'), reverse_java('cafe'), reverse_java('hello')")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "dcba");
                assertEquals(rs.getString(2), "efac");
                assertEquals(rs.getString(3), "olleh");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_concat_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("concat_java",
                                   new DuckDBColumnType[] {DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR},
                                   DuckDBColumnType.VARCHAR, (ctx, args, out, rowCount) -> {
                                       for (int row = 0; row < rowCount; row++) {
                                           if (args[0].isNull(row) || args[1].isNull(row)) {
                                               out.setNull(row);
                                           } else {
                                               out.setString(row, args[0].getString(row) + args[1].getString(row));
                                           }
                                       }
                                   });

            try (ResultSet rs = stmt.executeQuery("SELECT concat_java('Hello ', 'world')")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "Hello world");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_java_scalar_udf_add_one_10m_benchmark() throws Exception {
        if (!Boolean.getBoolean("duckdb.udf.benchmark")) {
            return;
        }
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarUdf("add_one", (ctx, args, out, rowCount) -> {
                for (int row = 0; row < rowCount; row++) {
                    if (args[0].isNull(row)) {
                        out.setNull(row);
                    } else {
                        out.setInt(row, args[0].getInt(row) + 1);
                    }
                }
            });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(add_one(i::INTEGER)) FROM range(10000000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 50000005000000L);
                assertFalse(rs.next());
            }
        }
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
            + ",'enum18' ,'enum19' ,'enum20' ,'enum21' ,'enum22' ,'enum23' ,'enum24' ,'enum25' ,'enum26' ,'enum27' "
            + ",'enum28'"
            + ",'enum29' ,'enum30' ,'enum31' ,'enum32' ,'enum33' ,'enum34' ,'enum35' ,'enum36' ,'enum37' ,'enum38' "
            + ",'enum39'"
            + ",'enum40' ,'enum41' ,'enum42' ,'enum43' ,'enum44' ,'enum45' ,'enum46' ,'enum47' ,'enum48' ,'enum49' "
            + ",'enum50'"
            + ",'enum51' ,'enum52' ,'enum53' ,'enum54' ,'enum55' ,'enum56' ,'enum57' ,'enum58' ,'enum59' ,'enum60' "
            + ",'enum61'"
            + ",'enum62' ,'enum63' ,'enum64' ,'enum65' ,'enum66' ,'enum67' ,'enum68' ,'enum69' ,'enum70' ,'enum71' "
            + ",'enum72'"
            + ",'enum73' ,'enum74' ,'enum75' ,'enum76' ,'enum77' ,'enum78' ,'enum79' ,'enum80' ,'enum81' ,'enum82' "
            + ",'enum83'"
            + ",'enum84' ,'enum85' ,'enum86' ,'enum87' ,'enum88' ,'enum89' ,'enum90' ,'enum91' ,'enum92' ,'enum93' "
            + ",'enum94'"
            +
            ",'enum95' ,'enum96' ,'enum97' ,'enum98' ,'enum99' ,'enum100' ,'enum101' ,'enum102' ,'enum103' ,'enum104' "
            + ",'enum105' ,'enum106' ,'enum107' ,'enum108' ,'enum109' ,'enum110' ,'enum111' ,'enum112' ,'enum113' "
            + ",'enum114'"
            + ",'enum115' ,'enum116' ,'enum117' ,'enum118' ,'enum119' ,'enum120' ,'enum121' ,'enum122' ,'enum123' "
            + ",'enum124'"
            + ",'enum125' ,'enum126' ,'enum127' ,'enum128' ,'enum129' ,'enum130' ,'enum131' ,'enum132' ,'enum133' "
            + ",'enum134'"
            + ",'enum135' ,'enum136' ,'enum137' ,'enum138' ,'enum139' ,'enum140' ,'enum141' ,'enum142' ,'enum143' "
            + ",'enum144'"
            + ",'enum145' ,'enum146' ,'enum147' ,'enum148' ,'enum149' ,'enum150' ,'enum151' ,'enum152' ,'enum153' "
            + ",'enum154'"
            + ",'enum155' ,'enum156' ,'enum157' ,'enum158' ,'enum159' ,'enum160' ,'enum161' ,'enum162' ,'enum163' "
            + ",'enum164'"
            + ",'enum165' ,'enum166' ,'enum167' ,'enum168' ,'enum169' ,'enum170' ,'enum171' ,'enum172' ,'enum173' "
            + ",'enum174'"
            + ",'enum175' ,'enum176' ,'enum177' ,'enum178' ,'enum179' ,'enum180' ,'enum181' ,'enum182' ,'enum183' "
            + ",'enum184'"
            + ",'enum185' ,'enum186' ,'enum187' ,'enum188' ,'enum189' ,'enum190' ,'enum191' ,'enum192' ,'enum193' "
            + ",'enum194'"
            + ",'enum195' ,'enum196' ,'enum197' ,'enum198' ,'enum199' ,'enum200' ,'enum201' ,'enum202' ,'enum203' "
            + ",'enum204'"
            + ",'enum205' ,'enum206' ,'enum207' ,'enum208' ,'enum209' ,'enum210' ,'enum211' ,'enum212' ,'enum213' "
            + ",'enum214'"
            + ",'enum215' ,'enum216' ,'enum217' ,'enum218' ,'enum219' ,'enum220' ,'enum221' ,'enum222' ,'enum223' "
            + ",'enum224'"
            + ",'enum225' ,'enum226' ,'enum227' ,'enum228' ,'enum229' ,'enum230' ,'enum231' ,'enum232' ,'enum233' "
            + ",'enum234'"
            + ",'enum235' ,'enum236' ,'enum237' ,'enum238' ,'enum239' ,'enum240' ,'enum241' ,'enum242' ,'enum243' "
            + ",'enum244'"
            + ",'enum245' ,'enum246' ,'enum247' ,'enum248' ,'enum249' ,'enum250' ,'enum251' ,'enum252' ,'enum253' "
            + ",'enum254'"
            + ",'enum255' ,'enum256' ,'enum257' ,'enum258' ,'enum259' ,'enum260' ,'enum261' ,'enum262' ,'enum263' "
            + ",'enum264'"
            + ",'enum265' ,'enum266' ,'enum267' ,'enum268' ,'enum269' ,'enum270' ,'enum271' ,'enum272' ,'enum273' "
            + ",'enum274'"
            + ",'enum275' ,'enum276' ,'enum277' ,'enum278' ,'enum279' ,'enum280' ,'enum281' ,'enum282' ,'enum283' "
            + ",'enum284'"
            + ",'enum285' ,'enum286' ,'enum287' ,'enum288' ,'enum289' ,'enum290' ,'enum291' ,'enum292' ,'enum293' "
            + ",'enum294'"
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
        Properties prop1 = new Properties();
        prop1.put(DuckDBDriver.DUCKDB_READONLY_PROPERTY, "true");
        Properties prop2 = new Properties();
        prop2.put(DuckDBDriver.DUCKDB_READONLY_PROPERTY, true);
        Properties prop3 = new Properties();
        prop3.put(DuckDBDriver.DUCKDB_ACCESS_MODE_PROPERTY, DuckDBDriver.DUCKDB_ACCESS_MODE_READ_ONLY);
        Properties prop4 = new Properties();
        prop3.put(DuckDBDriver.DUCKDB_READONLY_PROPERTY, true);
        prop4.put(DuckDBDriver.DUCKDB_ACCESS_MODE_PROPERTY, DuckDBDriver.DUCKDB_ACCESS_MODE_READ_ONLY);
        List<Properties> propList = Arrays.asList(prop1, prop2, prop3, prop4);

        for (Properties config : propList) {
            try (TempDirectory dir = new TempDirectory()) {
                Path database_file = dir.path().resolve(Paths.get("duckcb_jdbc_test_read_only.db"));
                String jdbc_url = JDBC_URL + database_file;
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
                     Statement stmt1 = conn.createStatement();
                     ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
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
                try (Connection conn_ro1 = DriverManager.getConnection(jdbc_url, config);
                     Connection conn_ro2 = DriverManager.getConnection(jdbc_url, config)) {

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
        }
    }

    public static void test_read_only_discrepancy() throws Exception {
        Properties config = new Properties();
        config.put(DuckDBDriver.DUCKDB_READONLY_PROPERTY, true);
        config.put(DuckDBDriver.DUCKDB_ACCESS_MODE_PROPERTY, DuckDBDriver.DUCKDB_ACCESS_MODE_READ_WRITE);
        assertThrows(() -> DriverManager.getConnection(JDBC_URL, config), SQLException.class);
        assertThrows(()
                         -> DriverManager.getConnection(JDBC_URL + ";duckdb.read_only=false;access_mode=READ_ONLY;"),
                     SQLException.class);
    }

    public static void test_temporal_types() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT '2019-11-26 21:11:00'::timestamp ts, '2019-11-26'::date dt, "
                                         + "interval '5 days' iv, '21:11:00'::time te");
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
        info.put("custom_profiling_settings", "{}");

        String message = assertThrows(() -> DriverManager.getConnection(JDBC_URL, info), SQLException.class);

        assertTrue(message.contains("Could not set option \"custom_profiling_settings\" as a global option"));
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
            "bignum", asList("-17976931348623157081452742373170435679807056752584499659891747680315726078002"
                                 + "853876058955863276687817154045895351438246423432132688946418276846754670353751"
                                 + "698604991057655128207624549009038932894407586850845513394230458323690322294816"
                                 + "5808559332123348274797826204144723168738177180919299881250404026184124858368",
                             "179769313486231570814527423731704356798070567525844996598917476803157260780028"
                                 + "538760589558632766878171540458953514382464234321326889464182768467546703537516"
                                 + "986049910576551282076245490090389328944075868508455133942304583236903222948165"
                                 + "808559332123348274797826204144723168738177180919299881250404026184124858368",
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
            String sql = "select * EXCLUDE(time, time_ns, time_tz)"
                         +
                         "\n    , CASE WHEN time = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE time END AS time"
                         + "\n    , CASE WHEN time_ns = '24:00:00'::TIME_NS THEN '23:59:59.999999'::TIME_NS ELSE "
                         + "time_ns END AS time_ns"
                         + "\n    , CASE WHEN time_tz = '24:00:00-15:59:59'::TIMETZ THEN "
                         + "'23:59:59.999999-15:59:59'::TIMETZ ELSE time_tz END AS time_tz"
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
            assertThrows(()
                             -> stmt.executeQuery("WITH RECURSIVE cte AS NOT MATERIALIZED ("
                                                  + "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, "
                                                  + "cte.p + cte.f from cte WHERE cte.i < 200000) "
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
            statusCode = runTests(args, TestDuckDBJDBC.class, TestAppender.class, TestAppenderCollection.class,
                                  TestAppenderCollection2D.class, TestAppenderComposite.class,
                                  TestSingleValueAppender.class, TestBatch.class, TestBindings.class, TestClosure.class,
                                  TestExtensionTypes.class, TestMetadata.class, TestNoLib.class, /* TestSpatial.class,*/
                                  TestParameterMetadata.class, TestPrepare.class, TestResults.class,
                                  TestSessionInit.class, TestTimestamp.class, TestVariant.class);
        }
        System.exit(statusCode);
    }
}

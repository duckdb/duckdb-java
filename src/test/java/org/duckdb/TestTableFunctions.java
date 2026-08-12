package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.duckdb.DuckDBFunctions.FunctionException;

public class TestTableFunctions {

    private static final AtomicInteger LIFECYCLE_FUNCTION_ID = new AtomicInteger();

    @SuppressWarnings("try")
    private static final class TrackingState implements DuckDBTableFunctionState {
        private final boolean throwOnClose;
        private final AtomicInteger closeCalls = new AtomicInteger();

        TrackingState(boolean throwOnClose) {
            this.throwOnClose = throwOnClose;
        }

        @Override
        public void close() throws Exception {
            closeCalls.incrementAndGet();
            if (throwOnClose) {
                throw new Exception("table function state close failure");
            }
        }
    }

    private static final class LifecycleTracker {
        final boolean failInApply;
        final boolean parallel;
        final long rows;
        final boolean throwOnClose;
        final AtomicLong remaining = new AtomicLong();
        final ConcurrentLinkedQueue<TrackingState> bindStates = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<TrackingState> globalStates = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<TrackingState> localStates = new ConcurrentLinkedQueue<>();

        LifecycleTracker(long rows, boolean failInApply, boolean throwOnClose, boolean parallel) {
            this.rows = rows;
            this.failInApply = failInApply;
            this.throwOnClose = throwOnClose;
            this.parallel = parallel;
        }
    }

    private static final class PlainAutoCloseableState implements AutoCloseable {
        final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }

    private static String registerLifecycleTableFunction(Connection conn, final LifecycleTracker tracker)
        throws Exception {
        String name = "java_table_function_state_" + LIFECYCLE_FUNCTION_ID.incrementAndGet();
        DuckDBFunctions.tableFunction()
            .withName(name)
            .withFunction(new DuckDBTableFunction<TrackingState, TrackingState, TrackingState>() {
                @Override
                public TrackingState bind(DuckDBTableFunctionBindInfo info) throws Exception {
                    info.addResultColumn("value", Long.TYPE);
                    TrackingState state = new TrackingState(tracker.throwOnClose);
                    tracker.bindStates.add(state);
                    return state;
                }

                @Override
                public TrackingState init(DuckDBTableFunctionInitInfo info) throws Exception {
                    info.setMaxThreads(tracker.parallel ? Runtime.getRuntime().availableProcessors() : 1);
                    tracker.remaining.set(tracker.rows);
                    TrackingState state = new TrackingState(tracker.throwOnClose);
                    tracker.globalStates.add(state);
                    return state;
                }

                @Override
                public TrackingState localInit(DuckDBTableFunctionInitInfo info) throws Exception {
                    TrackingState state = new TrackingState(tracker.throwOnClose);
                    tracker.localStates.add(state);
                    return state;
                }

                @Override
                public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                    if (tracker.failInApply) {
                        throw new Exception("table function apply failure");
                    }
                    long written = 0;
                    while (written < output.capacity()) {
                        long remaining = tracker.remaining.getAndDecrement();
                        if (remaining <= 0) {
                            break;
                        }
                        output.vector(0).setLong(written++, remaining);
                    }
                    return written;
                }
            })
            .register(conn);
        return name;
    }

    private static void assertLifecycleStatesClosed(LifecycleTracker tracker) throws Exception {
        assertTrue(!tracker.bindStates.isEmpty());
        assertTrue(!tracker.globalStates.isEmpty());
        assertTrue(!tracker.localStates.isEmpty());
        for (TrackingState state : tracker.bindStates) {
            assertEquals(state.closeCalls.get(), 1);
        }
        for (TrackingState state : tracker.globalStates) {
            assertEquals(state.closeCalls.get(), 1);
        }
        for (TrackingState state : tracker.localStates) {
            assertEquals(state.closeCalls.get(), 1);
        }
    }

    public static void test_table_function_basic() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName("java_table_basic")
                .withParameter(int.class)
                .withNamedParameter("param1", String.class)
                .withFunction(new DuckDBTableFunction<Integer, AtomicBoolean, Object>() {
                    @Override
                    public Integer bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("col1", Integer.TYPE).addResultColumn("col2", String.class);
                        DuckDBValue param = info.getParameter(0);
                        assertThrows(param::getBoolean, FunctionException.class);
                        DuckDBValue namedParam = info.getNamedParameter("param1");
                        assertEquals(namedParam.getString(), "foobar");
                        return param.getInt();
                    }

                    @Override
                    public AtomicBoolean init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(1);
                        return new AtomicBoolean(false);
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        Integer bindData = info.getBindData();
                        AtomicBoolean done = info.getInitData();
                        if (done.get()) {
                            return 0;
                        }
                        output.vector(0).setInt(0, bindData);
                        output.vector(1).setString(0, "foo");
                        output.vector(0).setNull(1);
                        output.vector(1).setString(1, "bar");
                        done.set(true);
                        return 2;
                    }
                })
                .register(conn);
            try (ResultSet rs = stmt.executeQuery("FROM java_table_basic(42, param1='foobar')")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(2), "foo");
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertEquals(rs.getString(2), "bar");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_long_result() throws Exception {
        long count = (1 << 16) + 7;
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName("java_table_long_result")
                .withFunction(new DuckDBTableFunction<Object, AtomicLong, Object>() {
                    @Override
                    public Object bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("col1", Long.TYPE).addResultColumn("col2", String.class);
                        return null;
                    }

                    @Override
                    public AtomicLong init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(1);
                        return new AtomicLong(count);
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        AtomicLong remainingHolder = info.getInitData();
                        long remaining = remainingHolder.get();
                        if (remaining <= 0) {
                            return 0;
                        }

                        DuckDBWritableVector vec1 = output.vector(0);
                        DuckDBWritableVector vec2 = output.vector(1);

                        long limit = Math.min(remaining, output.capacity());
                        long row = 0;
                        for (; row < limit; row++) {
                            long val = remaining - row;
                            vec1.setLong(row, val);
                            vec2.setString(row, val + "foo");
                        }
                        remainingHolder.set(remaining - row);
                        return row;
                    }
                })
                .register(conn);
            try (ResultSet rs = stmt.executeQuery("FROM java_table_long_result()")) {
                long fetched = 0;
                while (rs.next()) {
                    long val = count - fetched;
                    assertEquals(rs.getLong(1), val);
                    assertEquals(rs.getString(2), val + "foo");
                    fetched++;
                }
                assertEquals(fetched, count);
            }
        }
    }

    public static void test_table_function_projection_pushdown() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName("java_table_projection_pushdown")
                .withProjectionPushdown()
                .withFunction(new DuckDBTableFunction<String, AtomicBoolean, Object>() {
                    @Override
                    public String bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("col1", Integer.TYPE).addResultColumn("col2", String.class);
                        return "foobar";
                    }

                    @Override
                    public AtomicBoolean init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(1);
                        assertEquals(info.getColumnCount(), (long) 2);
                        assertEquals(info.getColumnIndex(0), (long) 0);
                        return new AtomicBoolean(false);
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        String bindData = info.getBindData();
                        assertEquals(bindData, "foobar");
                        AtomicBoolean done = info.getInitData();
                        if (done.get()) {
                            return 0;
                        }
                        output.vector(0).setInt(0, 41);
                        output.vector(1).setString(0, "foo");
                        output.vector(0).setInt(1, 42);
                        output.vector(1).setString(1, "bar");
                        done.set(true);
                        return 2;
                    }
                })
                .register(conn);
            try (ResultSet rs = stmt.executeQuery("FROM java_table_projection_pushdown()")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertEquals(rs.getString(2), "foo");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(2), "bar");
                assertFalse(rs.next());
            }
        }
    }

    static class ParametersTestBindData {
        final boolean boolParam;
        final byte byteParam;
        final short uint8Param;
        final short shortParam;
        final int uint16Param;
        final int intParam;
        final long uint32Param;
        final long longParam;
        final BigInteger uint64Param;
        final BigInteger hugeIntParam;
        final BigInteger uhugeIntParam;
        final float floatParam;
        final double doubleParam;
        final BigDecimal decimalParam;
        final LocalDate localDateParam;
        final LocalDateTime localDateTimeParam;
        final String stringParam;

        ParametersTestBindData(boolean boolParam, byte byteParam, short uint8Param, short shortParam, int uint16Param,
                               int intParam, long uint32Param, long longParam, BigInteger uint64Param,
                               BigInteger hugeIntParam, BigInteger uhugeIntParam, float floatParam, double doubleParam,
                               BigDecimal decimalParam, LocalDate localDateParam, LocalDateTime localDateTimeParam,
                               String stringParam) {
            this.boolParam = boolParam;
            this.byteParam = byteParam;
            this.uint8Param = uint8Param;
            this.shortParam = shortParam;
            this.uint16Param = uint16Param;
            this.intParam = intParam;
            this.uint32Param = uint32Param;
            this.longParam = longParam;
            this.uint64Param = uint64Param;
            this.hugeIntParam = hugeIntParam;
            this.uhugeIntParam = uhugeIntParam;
            this.floatParam = floatParam;
            this.doubleParam = doubleParam;
            this.decimalParam = decimalParam;
            this.localDateParam = localDateParam;
            this.localDateTimeParam = localDateTimeParam;
            this.stringParam = stringParam;
        }
    }

    public static void test_table_function_parameter_types() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName("java_table_parameter_types")
                .withParameters(DuckDBColumnType.BOOLEAN, DuckDBColumnType.TINYINT, DuckDBColumnType.UTINYINT,
                                DuckDBColumnType.SMALLINT, DuckDBColumnType.USMALLINT, DuckDBColumnType.INTEGER,
                                DuckDBColumnType.UINTEGER, DuckDBColumnType.BIGINT, DuckDBColumnType.UBIGINT,
                                DuckDBColumnType.HUGEINT, DuckDBColumnType.UHUGEINT, DuckDBColumnType.FLOAT,
                                DuckDBColumnType.DOUBLE, DuckDBColumnType.DECIMAL, DuckDBColumnType.DATE,
                                DuckDBColumnType.TIMESTAMP, DuckDBColumnType.VARCHAR)
                .withFunction(new DuckDBTableFunction<ParametersTestBindData, AtomicBoolean, Object>() {
                    @Override
                    public ParametersTestBindData bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("par0", DuckDBColumnType.BOOLEAN)
                            .addResultColumn("par1", DuckDBColumnType.TINYINT)
                            .addResultColumn("par2", DuckDBColumnType.UTINYINT)
                            .addResultColumn("par3", DuckDBColumnType.SMALLINT)
                            .addResultColumn("par4", DuckDBColumnType.USMALLINT)
                            .addResultColumn("par5", DuckDBColumnType.INTEGER)
                            .addResultColumn("par6", DuckDBColumnType.UINTEGER)
                            .addResultColumn("par7", DuckDBColumnType.BIGINT)
                            .addResultColumn("par8", DuckDBColumnType.UBIGINT)
                            .addResultColumn("par9", DuckDBColumnType.HUGEINT)
                            .addResultColumn("par10", DuckDBColumnType.UHUGEINT)
                            .addResultColumn("par11", DuckDBColumnType.FLOAT)
                            .addResultColumn("par12", DuckDBColumnType.DOUBLE)
                            .addResultColumn("par13", DuckDBColumnType.DECIMAL)
                            .addResultColumn("par14", DuckDBColumnType.DATE)
                            .addResultColumn("par15", DuckDBColumnType.TIMESTAMP)
                            .addResultColumn("par16", DuckDBColumnType.VARCHAR);
                        return new ParametersTestBindData(
                            info.getParameter(0).getBoolean(), info.getParameter(1).getByte(),
                            info.getParameter(2).getUint8(), info.getParameter(3).getShort(),
                            info.getParameter(4).getUint16(), info.getParameter(5).getInt(),
                            info.getParameter(6).getUint32(), info.getParameter(7).getLong(),
                            info.getParameter(8).getUint64(), info.getParameter(9).getHugeInt(),
                            info.getParameter(10).getUHugeInt(), info.getParameter(11).getFloat(),
                            info.getParameter(12).getDouble(), info.getParameter(13).getBigDecimal(),
                            info.getParameter(14).getLocalDate(), info.getParameter(15).getLocalDateTime(),
                            info.getParameter(16).getString());
                    }

                    @Override
                    public AtomicBoolean init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(1);
                        return new AtomicBoolean(false);
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        ParametersTestBindData bindData = info.getBindData();
                        AtomicBoolean done = info.getInitData();
                        if (done.get()) {
                            return 0;
                        }

                        output.vector(0).setBoolean(0, bindData.boolParam);
                        output.vector(1).setByte(0, bindData.byteParam);
                        output.vector(2).setUint8(0, bindData.uint8Param);
                        output.vector(3).setShort(0, bindData.shortParam);
                        output.vector(4).setUint16(0, bindData.uint16Param);
                        output.vector(5).setInt(0, bindData.intParam);
                        output.vector(6).setUint32(0, bindData.uint32Param);
                        output.vector(7).setLong(0, bindData.longParam);
                        output.vector(8).setUint64(0, bindData.uint64Param);
                        output.vector(9).setHugeInt(0, bindData.hugeIntParam);
                        output.vector(10).setUHugeInt(0, bindData.uhugeIntParam);
                        output.vector(11).setFloat(0, bindData.floatParam);
                        output.vector(12).setDouble(0, bindData.doubleParam);
                        output.vector(13).setBigDecimal(0, bindData.decimalParam);
                        output.vector(14).setDate(0, bindData.localDateParam);
                        output.vector(15).setTimestamp(0, bindData.localDateTimeParam);
                        output.vector(16).setString(0, bindData.stringParam);

                        done.set(true);
                        return 1;
                    }
                })
                .register(conn);
            try (DuckDBResultSet rs = (DuckDBResultSet) stmt.executeQuery("FROM java_table_parameter_types("
                                                                          + "TRUE::BOOLEAN,"
                                                                          + "41::TINYINT,"
                                                                          + "42::UTINYINT,"
                                                                          + "43::SMALLINT,"
                                                                          + "44::USMALLINT,"
                                                                          + "45::INTEGER,"
                                                                          + "46::UINTEGER,"
                                                                          + "47::BIGINT,"
                                                                          + "48::UBIGINT,"
                                                                          + "49::HUGEINT,"
                                                                          + "50::UHUGEINT,"
                                                                          + "51::FLOAT,"
                                                                          + "52::DOUBLE,"
                                                                          + "53::DECIMAL,"
                                                                          + "'2020-12-31'::DATE,"
                                                                          + "'2020-12-31 23:58:59'::TIMESTAMP,"
                                                                          + "'foobar'::VARCHAR"
                                                                          + ")")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertEquals(rs.getByte(2), (byte) 41);
                assertEquals(rs.getShort(3), (short) 42);
                assertEquals(rs.getShort(4), (short) 43);
                assertEquals(rs.getInt(5), 44);
                assertEquals(rs.getInt(6), 45);
                assertEquals(rs.getLong(7), (long) 46);
                assertEquals(rs.getLong(8), (long) 47);
                assertEquals(rs.getHugeint(9), BigInteger.valueOf(48));
                assertEquals(rs.getHugeint(10), BigInteger.valueOf(49));
                assertEquals(rs.getHugeint(11), BigInteger.valueOf(50));
                assertEquals(rs.getFloat(12), (float) 51);
                assertEquals(rs.getDouble(13), (double) 52);
                assertEquals(rs.getBigDecimal(14), BigDecimal.valueOf(53).setScale(18));
                assertEquals(rs.getObject(15, LocalDate.class), LocalDate.of(2020, 12, 31));
                assertEquals(rs.getObject(16, LocalDateTime.class), LocalDateTime.of(2020, 12, 31, 23, 58, 59));
                assertEquals(rs.getString(17), "foobar");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_multiple_threads() throws Exception {
        long count = (1 << 16) + 7;
        AtomicInteger threadsUsed = new AtomicInteger(0);
        try (Connection conn = DriverManager.getConnection(JDBC_URL + ";preserve_insertion_order=FALSE;");
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName("java_table_long_result")
                .withParameter(long.class)
                .withFunction(new DuckDBTableFunction<Long, AtomicLong, Object>() {
                    @Override
                    public Long bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("col1", Long.TYPE).addResultColumn("col2", String.class);
                        return info.getParameter(0).getLong();
                    }

                    @Override
                    public AtomicLong init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(Runtime.getRuntime().availableProcessors());
                        return new AtomicLong(count);
                    }

                    @Override
                    public Object localInit(DuckDBTableFunctionInitInfo info) throws Exception {
                        threadsUsed.incrementAndGet();
                        return null;
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        AtomicLong remaining = info.getInitData();

                        DuckDBWritableVector vec1 = output.vector(0);
                        DuckDBWritableVector vec2 = output.vector(1);

                        long val = info.getBindData();
                        long limit = output.capacity();
                        long row = 0;
                        for (; row < limit && remaining.decrementAndGet() >= 0; row++) {
                            vec1.setLong(row, val);
                            vec2.setString(row, val + "foo");
                        }
                        return row;
                    }
                })
                .register(conn);
            try (ResultSet rs = stmt.executeQuery("FROM java_table_long_result(42)")) {
                long fetched = 0;
                while (rs.next()) {
                    long val = 42;
                    assertEquals(rs.getLong(1), val);
                    assertEquals(rs.getString(2), val + "foo");
                    fetched++;
                }
                assertEquals(fetched, count);
            }
            assertEquals(threadsUsed.get(), Runtime.getRuntime().availableProcessors());
        }
    }

    public static void test_table_function_state_limit_cleanup() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(1 << 16, false, false, false);
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + functionName + "() LIMIT 1");
        assertTrue(rs.next());
        rs.close();
        rs.close();
        stmt.close();
        stmt.close();
        assertTrue(tracker.remaining.get() > 0);
        assertLifecycleStatesClosed(tracker);
        conn.close();
        conn.close();
        assertLifecycleStatesClosed(tracker);
    }

    public static void test_table_function_state_normal_completion() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(17, false, false, false);
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
            long rows = 0;
            while (rs.next()) {
                rows++;
            }
            assertEquals(rows, 17L);
        }
        stmt.close();
        assertLifecycleStatesClosed(tracker);
        conn.close();
    }

    public static void test_table_function_state_apply_error_cleanup() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(17, true, false, false);
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        assertThrows(() -> {
            try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
                rs.next();
            }
        }, java.sql.SQLException.class);
        stmt.close();
        assertLifecycleStatesClosed(tracker);
        conn.close();
    }

    public static void test_table_function_state_connection_cleanup() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(1 << 16, false, false, false);
        Connection conn = DriverManager.getConnection(JDBC_URL + ";jdbc_stream_results=true");
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + functionName + "() LIMIT 1");
        assertTrue(rs.next());
        conn.close();
        assertTrue(tracker.remaining.get() > 0);
        assertLifecycleStatesClosed(tracker);
    }

    public static void test_table_function_state_close_exception_suppressed() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(1, false, true, false);
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
        stmt.close();

        try (Statement subsequent = conn.createStatement(); ResultSet rs = subsequent.executeQuery("SELECT 42")) {
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 42);
            assertFalse(rs.next());
        }
        conn.close();
        assertLifecycleStatesClosed(tracker);
    }

    public static void test_table_function_state_requires_marker() throws Exception {
        PlainAutoCloseableState bindState = new PlainAutoCloseableState();
        PlainAutoCloseableState globalState = new PlainAutoCloseableState();
        PlainAutoCloseableState localState = new PlainAutoCloseableState();
        String functionName = "java_table_function_plain_state_" + LIFECYCLE_FUNCTION_ID.incrementAndGet();
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.tableFunction()
                .withName(functionName)
                .withFunction(new DuckDBTableFunction<PlainAutoCloseableState, PlainAutoCloseableState,
                                                      PlainAutoCloseableState>() {
                    @Override
                    public PlainAutoCloseableState bind(DuckDBTableFunctionBindInfo info) throws Exception {
                        info.addResultColumn("value", Integer.TYPE);
                        return bindState;
                    }

                    @Override
                    public PlainAutoCloseableState init(DuckDBTableFunctionInitInfo info) throws Exception {
                        info.setMaxThreads(1);
                        return globalState;
                    }

                    @Override
                    public PlainAutoCloseableState localInit(DuckDBTableFunctionInitInfo info) throws Exception {
                        return localState;
                    }

                    @Override
                    public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                        return 0;
                    }
                })
                .register(conn);
            try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
                assertFalse(rs.next());
            }
        }
        assertEquals(bindState.closeCalls.get(), 0);
        assertEquals(globalState.closeCalls.get(), 0);
        assertEquals(localState.closeCalls.get(), 0);
    }

    public static void test_table_function_state_alias_closes_per_holder() throws Exception {
        TrackingState alias = new TrackingState(false);
        String functionName = "java_table_function_alias_state_" + LIFECYCLE_FUNCTION_ID.incrementAndGet();
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        DuckDBFunctions.tableFunction()
            .withName(functionName)
            .withFunction(new DuckDBTableFunction<TrackingState, TrackingState, Object>() {
                @Override
                public TrackingState bind(DuckDBTableFunctionBindInfo info) throws Exception {
                    info.addResultColumn("value", Integer.TYPE);
                    return alias;
                }

                @Override
                public TrackingState init(DuckDBTableFunctionInitInfo info) throws Exception {
                    info.setMaxThreads(1);
                    return alias;
                }

                @Override
                public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                    return 0;
                }
            })
            .register(conn);
        try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
            assertFalse(rs.next());
        }
        stmt.close();
        conn.close();
        assertEquals(alias.closeCalls.get(), 2);
    }

    public static void test_table_function_state_null_and_common() throws Exception {
        String nullFunctionName = "java_table_function_null_state_" + LIFECYCLE_FUNCTION_ID.incrementAndGet();
        String commonFunctionName = "java_table_function_common_state_" + LIFECYCLE_FUNCTION_ID.incrementAndGet();
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            registerObjectStateTableFunction(conn, nullFunctionName, null, null, null);
            registerObjectStateTableFunction(conn, commonFunctionName, new Object(), new Object(), new Object());
            try (ResultSet rs = stmt.executeQuery("FROM " + nullFunctionName + "()")) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("FROM " + commonFunctionName + "()")) {
                assertFalse(rs.next());
            }
        }
    }

    public static void test_table_function_state_streaming_result_close() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(1 << 16, false, false, false);
        Connection conn = DriverManager.getConnection(JDBC_URL + ";jdbc_stream_results=true");
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + functionName + "() LIMIT 1")) {
            assertTrue(rs.next());
        }
        stmt.close();
        conn.close();
        assertTrue(tracker.remaining.get() > 0);
        assertLifecycleStatesClosed(tracker);
    }

    public static void test_table_function_state_repeated_cleanup() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(17, false, false, false);
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            String functionName = registerLifecycleTableFunction(conn, tracker);
            for (int execution = 0; execution < 2; execution++) {
                try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
                    while (rs.next()) {
                        // Consume the result so every execution reaches normal exhaustion.
                    }
                }
            }
            assertLifecycleStatesClosed(tracker);
        }
    }

    public static void test_table_function_state_local_parallel_cleanup() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker(1 << 16, false, false, true);
        Connection conn = DriverManager.getConnection(JDBC_URL + ";preserve_insertion_order=false");
        Statement stmt = conn.createStatement();
        String functionName = registerLifecycleTableFunction(conn, tracker);
        try (ResultSet rs = stmt.executeQuery("FROM " + functionName + "()")) {
            while (rs.next()) {
                // Consume all rows to let every worker finish.
            }
        }
        stmt.close();
        assertLifecycleStatesClosed(tracker);
        conn.close();
    }

    private static void registerObjectStateTableFunction(Connection conn, String functionName, Object bindState,
                                                         Object globalState, Object localState) throws Exception {
        DuckDBFunctions.tableFunction()
            .withName(functionName)
            .withFunction(new DuckDBTableFunction<Object, Object, Object>() {
                @Override
                public Object bind(DuckDBTableFunctionBindInfo info) throws Exception {
                    info.addResultColumn("value", Integer.TYPE);
                    return bindState;
                }

                @Override
                public Object init(DuckDBTableFunctionInitInfo info) throws Exception {
                    info.setMaxThreads(1);
                    return globalState;
                }

                @Override
                public Object localInit(DuckDBTableFunctionInitInfo info) throws Exception {
                    return localState;
                }

                @Override
                public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                    return 0;
                }
            })
            .register(conn);
    }
}

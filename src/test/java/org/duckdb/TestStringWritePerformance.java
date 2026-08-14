package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.test.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance evidence for the VARCHAR write path of Java table functions.
 *
 * Scenarios isolate the cell-write cost of DuckDBWritableVector by emitting a
 * single column and aggregating it with COUNT(col), so no hash table or complex
 * operator sits between the table function and the measurement:
 *
 * - double_cell:        setDouble per cell (baseline of the numeric write path)
 * - varchar_repeat_cell: setString per cell, 12 distinct month-like values
 *   (matches the GROUP BY key shape reported in the field analysis)
 * - varchar_unique_cell: setString per cell, 2048 distinct values
 * - varchar_repeat_batch / varchar_unique_batch: same payloads written with the
 *   setStrings batch API (candidate fix)
 * - varchar_repeat_utf8_batch / varchar_unique_utf8_batch: pre-encoded UTF-8
 *   byte arrays written with the UTF-8 batch API
 *
 * Run with `make stress` (after `make release`). This benchmark is intentionally
 * excluded from the regular test suite.
 *
 * Tunables: -Dduckdb.perf.rows (default 2000000), -Dduckdb.perf.samples (default 5).
 * The stress target enables -Dduckdb.perf.assert=true; assertions require at least
 * one million rows to avoid measuring setup and JIT warmup noise.
 */
public class TestStringWritePerformance {

    private static final long DEFAULT_ROWS = 2_000_000;
    private static final long MIN_ASSERT_ROWS = 1_000_000;
    private static final int SAMPLES = Integer.getInteger("duckdb.perf.samples", 5);
    private static final boolean ASSERT = Boolean.getBoolean("duckdb.perf.assert");

    private static final String[] MONTHS = {"January", "February", "March",     "April",   "May",      "June",
                                            "July",    "August",   "September", "October", "November", "December"};

    private static String[] uniqueChunkValues(long capacity) {
        String[] values = new String[Math.toIntExact(capacity)];
        for (int i = 0; i < values.length; i++) {
            values[i] = "value_" + (i % 10) + "_" + i + "_abcdefghijklmnopqrstuvwxyz";
        }
        return values;
    }

    private static String[] repeatChunkValues(long capacity) {
        String[] values = new String[Math.toIntExact(capacity)];
        for (int i = 0; i < values.length; i++) {
            values[i] = MONTHS[i % MONTHS.length];
        }
        return values;
    }

    private static byte[][] encodeUtf8(String[] values) {
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            encoded[i] = values[i].getBytes(UTF_8);
        }
        return encoded;
    }

    enum Mode {
        DOUBLE_CELL,
        VARCHAR_REPEAT_CELL,
        VARCHAR_UNIQUE_CELL,
        VARCHAR_REPEAT_BATCH,
        VARCHAR_UNIQUE_BATCH,
        VARCHAR_REPEAT_UTF8_BATCH,
        VARCHAR_UNIQUE_UTF8_BATCH;
    }

    private static void registerFunction(Connection conn, final String name, final Mode mode) throws Exception {
        DuckDBFunctions.tableFunction()
            .withName(name)
            .withParameter(long.class)
            .withFunction(new DuckDBTableFunction<Long, Object[], Object>() {
                @Override
                public Long bind(DuckDBTableFunctionBindInfo info) throws Exception {
                    info.addResultColumn("col", mode == Mode.DOUBLE_CELL ? Double.TYPE : String.class);
                    return info.getParameter(0).getLong();
                }

                @Override
                public Object[] init(DuckDBTableFunctionInitInfo info) throws Exception {
                    info.setMaxThreads(1);
                    return new Object[] {new AtomicLong(info.getBindData()), null};
                }

                @Override
                public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output) throws Exception {
                    Object[] state = info.getInitData();
                    AtomicLong progress = (AtomicLong) state[0];
                    String[] uniqueValues = null;
                    byte[][] utf8Values = null;
                    if ((mode == Mode.VARCHAR_UNIQUE_CELL || mode == Mode.VARCHAR_UNIQUE_BATCH) && state[1] == null) {
                        uniqueValues = uniqueChunkValues(output.capacity());
                        state[1] = uniqueValues;
                    }
                    if (mode == Mode.VARCHAR_UNIQUE_CELL || mode == Mode.VARCHAR_UNIQUE_BATCH) {
                        uniqueValues = (String[]) state[1];
                    }
                    if ((mode == Mode.VARCHAR_REPEAT_UTF8_BATCH || mode == Mode.VARCHAR_UNIQUE_UTF8_BATCH) &&
                        state[1] == null) {
                        String[] values = mode == Mode.VARCHAR_REPEAT_UTF8_BATCH ? repeatChunkValues(output.capacity())
                                                                                 : uniqueChunkValues(output.capacity());
                        state[1] = encodeUtf8(values);
                    }
                    if (mode == Mode.VARCHAR_REPEAT_UTF8_BATCH || mode == Mode.VARCHAR_UNIQUE_UTF8_BATCH) {
                        utf8Values = (byte[][]) state[1];
                    }
                    long remaining = progress.get();
                    if (remaining <= 0) {
                        return 0;
                    }
                    long limit = Math.min(remaining, output.capacity());
                    DuckDBWritableVector vec = output.vector(0);
                    switch (mode) {
                    case DOUBLE_CELL:
                        for (long row = 0; row < limit; row++) {
                            vec.setDouble(row, 1.5d);
                        }
                        break;
                    case VARCHAR_REPEAT_CELL:
                        for (long row = 0; row < limit; row++) {
                            vec.setString(row, MONTHS[(int) (row % MONTHS.length)]);
                        }
                        break;
                    case VARCHAR_UNIQUE_CELL: {
                        for (long row = 0; row < limit; row++) {
                            vec.setString(row, uniqueValues[(int) row]);
                        }
                        break;
                    }
                    case VARCHAR_REPEAT_BATCH: {
                        String[] batch = repeatChunkValues(limit);
                        vec.setStrings(0, batch);
                        break;
                    }
                    case VARCHAR_UNIQUE_BATCH: {
                        if (limit == output.capacity()) {
                            vec.setStrings(0, uniqueValues);
                        } else {
                            String[] batch = new String[Math.toIntExact(limit)];
                            System.arraycopy(uniqueValues, 0, batch, 0, Math.toIntExact(limit));
                            vec.setStrings(0, batch);
                        }
                        break;
                    }
                    case VARCHAR_REPEAT_UTF8_BATCH:
                    case VARCHAR_UNIQUE_UTF8_BATCH: {
                        byte[][] batch =
                            limit == output.capacity() ? utf8Values : Arrays.copyOf(utf8Values, Math.toIntExact(limit));
                        vec.setStringUtf8Batch(0, batch);
                        break;
                    }
                    default:
                        throw new IllegalStateException("unknown mode " + mode);
                    }
                    progress.set(remaining - limit);
                    return limit;
                }
            })
            .register(conn);
    }

    public static void test_string_write_performance() throws Exception {
        long rows = Long.getLong("duckdb.perf.rows", DEFAULT_ROWS);
        if (rows <= 0) {
            throw new IllegalArgumentException("duckdb.perf.rows must be positive");
        }
        if (SAMPLES <= 0) {
            throw new IllegalArgumentException("duckdb.perf.samples must be positive");
        }
        if (ASSERT && rows < MIN_ASSERT_ROWS) {
            throw new IllegalArgumentException("duckdb.perf.assert requires at least " + MIN_ASSERT_ROWS + " rows");
        }
        try (Connection conn = DriverManager.getConnection(TestDuckDBJDBC.JDBC_URL);
             Statement stmt = conn.createStatement()) {
            registerFunction(conn, "perf_double_cell", Mode.DOUBLE_CELL);
            registerFunction(conn, "perf_varchar_repeat_cell", Mode.VARCHAR_REPEAT_CELL);
            registerFunction(conn, "perf_varchar_unique_cell", Mode.VARCHAR_UNIQUE_CELL);
            registerFunction(conn, "perf_varchar_repeat_batch", Mode.VARCHAR_REPEAT_BATCH);
            registerFunction(conn, "perf_varchar_unique_batch", Mode.VARCHAR_UNIQUE_BATCH);
            registerFunction(conn, "perf_varchar_repeat_utf8_batch", Mode.VARCHAR_REPEAT_UTF8_BATCH);
            registerFunction(conn, "perf_varchar_unique_utf8_batch", Mode.VARCHAR_UNIQUE_UTF8_BATCH);

            double doubleRate = measure(stmt, "perf_double_cell", rows);
            double repeatCellRate = measure(stmt, "perf_varchar_repeat_cell", rows);
            double uniqueCellRate = measure(stmt, "perf_varchar_unique_cell", rows);
            double repeatBatchRate = measure(stmt, "perf_varchar_repeat_batch", rows);
            double uniqueBatchRate = measure(stmt, "perf_varchar_unique_batch", rows);
            double repeatUtf8BatchRate = measure(stmt, "perf_varchar_repeat_utf8_batch", rows);
            double uniqueUtf8BatchRate = measure(stmt, "perf_varchar_unique_utf8_batch", rows);

            System.out.println("[perf] rows=" + rows + " samples=" + SAMPLES);
            report("double_cell", doubleRate, rows);
            report("varchar_repeat_cell", repeatCellRate, rows);
            report("varchar_unique_cell", uniqueCellRate, rows);
            report("varchar_repeat_batch", repeatBatchRate, rows);
            report("varchar_unique_batch", uniqueBatchRate, rows);
            report("varchar_repeat_utf8_batch", repeatUtf8BatchRate, rows);
            report("varchar_unique_utf8_batch", uniqueUtf8BatchRate, rows);
            System.out.println("[perf] slowdown repeat_cell vs double: " +
                               String.format("%.1fx", doubleRate / repeatCellRate));
            System.out.println("[perf] slowdown unique_cell vs double: " +
                               String.format("%.1fx", doubleRate / uniqueCellRate));
            System.out.println("[perf] speedup batch vs cell (repeat): " +
                               String.format("%.1fx", repeatBatchRate / repeatCellRate));
            System.out.println("[perf] speedup batch vs cell (unique): " +
                               String.format("%.1fx", uniqueBatchRate / uniqueCellRate));
            System.out.println("[perf] batch remaining gap vs double (repeat): " +
                               String.format("%.1fx", doubleRate / repeatBatchRate));
            System.out.println("[perf] speedup UTF-8 byte batch vs String batch (repeat): " +
                               String.format("%.1fx", repeatUtf8BatchRate / repeatBatchRate));
            System.out.println("[perf] speedup UTF-8 byte batch vs String batch (unique): " +
                               String.format("%.1fx", uniqueUtf8BatchRate / uniqueBatchRate));
            if (ASSERT) {
                assertTrue(repeatBatchRate > repeatCellRate, "batch (repeat) should beat per-cell setString");
                assertTrue(uniqueBatchRate > uniqueCellRate, "batch (unique) should beat per-cell setString");
                assertTrue(repeatUtf8BatchRate > repeatCellRate,
                           "UTF-8 byte batch (repeat) should beat per-cell setString");
                assertTrue(uniqueUtf8BatchRate > uniqueCellRate,
                           "UTF-8 byte batch (unique) should beat per-cell setString");
            }
        }
    }

    private static void report(String scenario, double rowsPerSec, long rows) {
        System.out.println("[perf] " + String.format("%-24s", scenario) +
                           " rate=" + String.format("%,12.0f", rowsPerSec) + " rows/s (" +
                           String.format("%.1f", 1e9d / rowsPerSec) + " ns/cell)");
    }

    private static double measure(Statement stmt, String function, long rows) throws Exception {
        String sql = "SELECT COUNT(col) FROM " + function + "(" + rows + ")";
        runOnce(stmt, sql, rows);
        double[] samples = new double[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) {
            samples[i] = runOnce(stmt, sql, rows);
        }
        Arrays.sort(samples);
        return samples[SAMPLES / 2];
    }

    private static double runOnce(Statement stmt, String sql, long expectedRows) throws Exception {
        long start = System.nanoTime();
        long count;
        try (ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException("no result for " + sql);
            }
            count = rs.getLong(1);
        }
        long elapsed = System.nanoTime() - start;
        if (count != expectedRows) {
            throw new IllegalStateException("expected " + expectedRows + " rows, got " + count);
        }
        return (double) expectedRows / (elapsed / 1e9d);
    }

    private TestStringWritePerformance() {
    }
}

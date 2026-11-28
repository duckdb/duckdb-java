package org.duckdb;

import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DECIMAL values wider than 38 digits, the unscaled value of such a decimal is stored as a BIGNUM blob.
 */
public class TestWideDecimal {

    private static final int[] WIDTHS = new int[] {39, 76, 255, 300};

    private static String digits(int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append((char) ('1' + (i % 9)));
        }
        return sb.toString();
    }

    // a value that uses all the digits of DECIMAL(width, scale)
    private static BigDecimal fullWidthValue(int width, int scale) {
        return new BigDecimal(new BigInteger(digits(width)), scale);
    }

    public static void test_wide_decimal_bignum_codec() throws Exception {
        assertEquals(DuckDBBignum.fromBigInteger(BigInteger.ZERO), new byte[] {(byte) 0x80, 0x00, 0x01, 0x00});
        assertEquals(DuckDBBignum.fromBigInteger(BigInteger.ONE), new byte[] {(byte) 0x80, 0x00, 0x01, 0x01});
        assertEquals(DuckDBBignum.fromBigInteger(BigInteger.ONE.negate()),
                     new byte[] {0x7f, (byte) 0xff, (byte) 0xfe, (byte) 0xfe});
        assertEquals(DuckDBBignum.fromBigInteger(BigInteger.valueOf(256)),
                     new byte[] {(byte) 0x80, 0x00, 0x02, 0x01, 0x00});
        BigInteger[] values = new BigInteger[] {BigInteger.ZERO,
                                                BigInteger.ONE,
                                                BigInteger.valueOf(127),
                                                BigInteger.valueOf(128),
                                                BigInteger.valueOf(255),
                                                BigInteger.valueOf(256),
                                                BigInteger.valueOf(Long.MAX_VALUE),
                                                BigInteger.ONE.shiftLeft(64),
                                                BigInteger.ONE.shiftLeft(127),
                                                BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE),
                                                BigInteger.TEN.pow(38),
                                                BigInteger.TEN.pow(300),
                                                new BigInteger(digits(1000))};
        for (BigInteger value : values) {
            assertEquals(DuckDBBignum.toBigInteger(DuckDBBignum.fromBigInteger(value)), value);
            assertEquals(DuckDBBignum.toBigInteger(DuckDBBignum.fromBigInteger(value.negate())), value.negate());
        }
        assertThrows(() -> {
            DuckDBBignum.toBigInteger(new byte[] {(byte) 0x80, 0x00, 0x00});
        }, IllegalArgumentException.class);
    }

    public static void test_wide_decimal_bind_fetch() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            for (int width : WIDTHS) {
                for (int scale : new int[] {0, 7}) {
                    BigDecimal positive = fullWidthValue(width, scale);
                    BigDecimal negative = positive.negate();
                    BigDecimal small = new BigDecimal("-42").setScale(scale);
                    BigDecimal zero = BigDecimal.ZERO.setScale(scale);
                    String typeName = "DECIMAL(" + width + "," + scale + ")";

                    stmt.execute("CREATE OR REPLACE TABLE wide (id INTEGER, d " + typeName + ")");
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO wide VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setBigDecimal(2, positive);
                        ps.execute();
                        ps.setInt(1, 2);
                        ps.setObject(2, negative);
                        ps.execute();
                        ps.setInt(1, 3);
                        ps.setObject(2, small, Types.DECIMAL);
                        ps.execute();
                        ps.setInt(1, 4);
                        ps.setBigDecimal(2, zero);
                        ps.execute();
                        ps.setInt(1, 5);
                        ps.setBigDecimal(2, null);
                        ps.execute();
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT d FROM wide ORDER BY id")) {
                        ResultSetMetaData meta = rs.getMetaData();
                        assertEquals(meta.getColumnType(1), Types.DECIMAL);
                        assertEquals(meta.getColumnTypeName(1), typeName);
                        assertEquals(meta.getColumnClassName(1), BigDecimal.class.getName());
                        assertEquals(meta.getPrecision(1), width);
                        assertEquals(meta.getScale(1), scale);

                        for (BigDecimal expected : new BigDecimal[] {positive, negative, small, zero}) {
                            assertTrue(rs.next());
                            assertEquals(rs.getBigDecimal(1), expected);
                            assertEquals(rs.getObject(1), expected);
                            assertEquals(rs.getObject(1, BigDecimal.class), expected);
                            assertEquals(rs.getString(1), expected.toString());
                            assertFalse(rs.wasNull());
                        }
                        assertEquals(rs.getLong(1), 0L);
                        assertEquals(rs.getDouble(1), 0.0, 1e-9);

                        assertTrue(rs.next());
                        assertNull(rs.getBigDecimal(1));
                        assertTrue(rs.wasNull());
                        assertNull(rs.getObject(1));
                        assertNull(rs.getString(1));
                        assertFalse(rs.next());
                    }

                    // the engine sees the same exact values
                    try (ResultSet rs = stmt.executeQuery("SELECT d::VARCHAR FROM wide ORDER BY id")) {
                        assertTrue(rs.next());
                        assertEquals(rs.getString(1), positive.toPlainString());
                        assertTrue(rs.next());
                        assertEquals(rs.getString(1), negative.toPlainString());
                        assertTrue(rs.next());
                        assertEquals(rs.getString(1), small.toPlainString());
                    }

                    // predicates on the bound parameter
                    try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM wide WHERE d = ?")) {
                        ps.setBigDecimal(1, negative);
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next());
                            assertEquals(rs.getInt(1), 2);
                            assertFalse(rs.next());
                        }
                    }
                }
            }
        }
    }

    public static void test_wide_decimal_bind_untyped_parameter() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement ps = conn.prepareStatement("SELECT ?, typeof(?)")) {
            BigDecimal value = new BigDecimal("-" + digits(47) + ".125");
            ps.setBigDecimal(1, value);
            ps.setBigDecimal(2, value);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), value);
                assertEquals(rs.getString(2), "DECIMAL(50,3)");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_wide_decimal_bind_max_decimal_width_38() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("SET max_decimal_width = 38");
            BigDecimal value = new BigDecimal("-" + digits(47) + ".125");
            // the parameter is bound as the exact value, never as NULL
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?, typeof(?)")) {
                ps.setBigDecimal(1, value);
                ps.setBigDecimal(2, value);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal(1), value);
                    assertEquals(rs.getString(2), "DECIMAL(50,3)");
                    assertFalse(rs.next());
                }
            }
            // a value that does not fit the column is rejected
            stmt.execute("CREATE TABLE narrow (d DECIMAL(38,3))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO narrow VALUES (?)")) {
                ps.setBigDecimal(1, new BigDecimal("1.5"));
                ps.execute();
                ps.setBigDecimal(1, value);
                assertThrows(ps::execute, SQLException.class);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT d FROM narrow")) {
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), new BigDecimal("1.500"));
                assertFalse(rs.next());
            }
        }
    }

    public static void test_wide_decimal_arithmetic_default_session() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            String max = "999999999999999999999999999999999999.99";
            try (ResultSet rs = stmt.executeQuery("SELECT CAST('" + max + "' AS DECIMAL(38,2)) + CAST('" + max +
                                                  "' AS DECIMAL(38,2)) AS s")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(meta.getColumnType(1), Types.DECIMAL);
                assertEquals(meta.getColumnTypeName(1), "DECIMAL(39,2)");
                assertEquals(meta.getPrecision(1), 39);
                assertEquals(meta.getScale(1), 2);
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), new BigDecimal("1999999999999999999999999999999999999.98"));
                assertEquals(rs.getObject(1), new BigDecimal("1999999999999999999999999999999999999.98"));
                assertFalse(rs.next());
            }

            // a literal of more than 38 digits is a decimal
            String literal = digits(45) + ".5";
            try (ResultSet rs = stmt.executeQuery("SELECT " + literal)) {
                assertEquals(rs.getMetaData().getColumnTypeName(1), "DECIMAL(46,1)");
                assertEquals(rs.getMetaData().getColumnType(1), Types.DECIMAL);
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), new BigDecimal(literal));
            }
        }
    }

    public static void test_wide_decimal_numeric_getters() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CAST('-1234.5' AS DECIMAL(50,1)), CAST(NULL AS DECIMAL(50,1))")) {
            assertTrue(rs.next());
            assertEquals(rs.getDouble(1), -1234.5, 1e-9);
            assertEquals(rs.getFloat(1), -1234.5f);
            assertEquals(rs.getLong(1), -1234L);
            assertEquals(rs.getInt(1), -1234);
            assertEquals(rs.getShort(1), (short) -1234);
            assertEquals(rs.getString(1), "-1234.5");
            assertEquals(rs.getLong(2), 0L);
            assertTrue(rs.wasNull());
            assertNull(rs.getBigDecimal(2));
        }
    }

    public static void test_wide_decimal_nested() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            BigDecimal value = fullWidthValue(60, 2);
            String cast = "CAST('" + value.toPlainString() + "' AS DECIMAL(60,2))";
            try (ResultSet rs =
                     stmt.executeQuery("SELECT [" + cast + ", NULL, -" + cast + "], {'a': " + cast + ", 'b': 1}")) {
                assertTrue(rs.next());
                Object[] list = (Object[]) rs.getArray(1).getArray();
                assertEquals(list.length, 3);
                assertEquals(list[0], value);
                assertNull(list[1]);
                assertEquals(list[2], value.negate());
                Object[] attrs = ((Struct) rs.getObject(2)).getAttributes();
                assertEquals(attrs[0], value);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_wide_decimal_parameter_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wide (d DECIMAL(300,25))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO wide VALUES (?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(meta.getParameterType(1), Types.DECIMAL);
                assertEquals(meta.getPrecision(1), 300);
                assertEquals(meta.getScale(1), 25);
                assertEquals(meta.getParameterClassName(1), BigDecimal.class.getName());
            }
        }
    }

    public static void test_wide_decimal_appender() throws Exception {
        for (boolean inlined : new boolean[] {true, false}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE wide (id INTEGER, a DECIMAL(39,2), b DECIMAL(76,0), c DECIMAL(255,7), "
                             + "d DECIMAL(300,10))");
                BigDecimal a = fullWidthValue(39, 2);
                BigDecimal b = fullWidthValue(76, 0);
                BigDecimal c = fullWidthValue(255, 7);
                BigDecimal d = fullWidthValue(300, 10);
                // short values are stored inline in the vector
                BigDecimal smallA = new BigDecimal("-1.25");
                BigDecimal smallB = new BigDecimal("255");
                BigDecimal smallC = BigDecimal.ZERO.setScale(7);
                BigDecimal smallD = new BigDecimal("-0.0000000001");

                try (DuckDBAppender appender = conn.createAppender("wide")) {
                    appender.setWriteInlinedStrings(inlined);
                    appender.beginRow().append(1).append(a).append(b).append(c).append(d).endRow();
                    appender.beginRow()
                        .append(2)
                        .append(a.negate())
                        .append(b.negate())
                        .append(c.negate())
                        .append(d.negate())
                        .endRow();
                    appender.beginRow().append(3).append(smallA).append(smallB).append(smallC).append(smallD).endRow();
                    appender.beginRow()
                        .append(4)
                        .append((BigDecimal) null)
                        .appendNull()
                        .append((BigDecimal) null)
                        .appendNull()
                        .endRow();
                    // wrong scale and too many digits are rejected
                    appender.beginRow().append(5);
                    assertThrows(() -> { appender.append(new BigDecimal("1.5")); }, SQLException.class);
                    assertThrows(() -> { appender.append(fullWidthValue(40, 2)); }, SQLException.class);
                    appender.append(smallA).append(smallB).append(smallC).append(smallD).endRow();
                    assertEquals(appender.flush(), 5L);
                }

                try (ResultSet rs = stmt.executeQuery("SELECT a, b, c, d FROM wide ORDER BY id")) {
                    BigDecimal[][] expected = new BigDecimal[][] {{a, b, c, d},
                                                                  {a.negate(), b.negate(), c.negate(), d.negate()},
                                                                  {smallA, smallB, smallC, smallD},
                                                                  {null, null, null, null},
                                                                  {smallA, smallB, smallC, smallD}};
                    for (BigDecimal[] row : expected) {
                        assertTrue(rs.next());
                        for (int i = 0; i < row.length; i++) {
                            assertEquals(rs.getBigDecimal(i + 1), row[i]);
                        }
                    }
                    assertFalse(rs.next());
                }
                // the engine sees the same exact values
                try (ResultSet rs = stmt.executeQuery("SELECT d::VARCHAR FROM wide WHERE id <= 3 ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), d.toPlainString());
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), d.negate().toPlainString());
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), smallD.toPlainString());
                }
            }
        }
    }

    public static void test_wide_decimal_appender_nested() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wide (id INTEGER, s STRUCT(d DECIMAL(50,3), i INTEGER))");
            BigDecimal value = fullWidthValue(50, 3).negate();
            try (DuckDBAppender appender = conn.createAppender("wide")) {
                appender.beginRow().append(1).beginStruct().append(value).append(42).endStruct().endRow();
                appender.flush();
            }
            try (ResultSet rs = stmt.executeQuery("SELECT s.d, s.i FROM wide")) {
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), value);
                assertEquals(rs.getInt(2), 42);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_wide_decimal_single_value_appender() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wide (id INTEGER, a DECIMAL(39,2), b DECIMAL(300,10))");
            BigDecimal a = fullWidthValue(39, 2).negate();
            BigDecimal b = fullWidthValue(300, 10);
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "wide")) {
                appender.beginRow();
                appender.append(1);
                appender.appendBigDecimal(a);
                appender.appendBigDecimal(b);
                appender.endRow();
                appender.flush();
            }
            try (ResultSet rs = stmt.executeQuery("SELECT a, b FROM wide")) {
                assertTrue(rs.next());
                assertEquals(rs.getBigDecimal(1), a);
                assertEquals(rs.getBigDecimal(2), b);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_wide_decimal_scalar_function() throws Exception {
        for (int width : WIDTHS) {
            int scale = 3;
            BigDecimal value = fullWidthValue(width - 1, scale);
            BigDecimal increment = new BigDecimal("0.001");
            String typeName = "DECIMAL(" + width + "," + scale + ")";
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {
                DuckDBFunctions.scalarFunction()
                    .withName("java_wide_negate")
                    .withParameter(DuckDBLogicalType.decimal(width, scale))
                    .withReturnType(DuckDBLogicalType.decimal(width, scale))
                    .withVectorizedFunction((input, output) -> {
                        DuckDBReadableVector in = input.vector(0);
                        input.stream().forEach(row -> {
                            BigDecimal dec = in.getBigDecimal(row);
                            if (dec == null) {
                                output.setNull(row);
                            } else {
                                output.setBigDecimal(row, dec.negate().add(increment));
                            }
                        });
                    })
                    .register(conn);
                try (ResultSet rs =
                         stmt.executeQuery("SELECT java_wide_negate(v), typeof(java_wide_negate(v)) FROM (VALUES "
                                           + "(CAST('" + value.toPlainString() + "' AS " + typeName + ")), "
                                           + "(NULL), "
                                           + "(CAST('-" + value.toPlainString() + "' AS " + typeName + ")), "
                                           + "(CAST('0.001' AS " + typeName + "))) t(v)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal(1), value.negate().add(increment));
                    assertEquals(rs.getString(2), typeName);
                    assertTrue(rs.next());
                    assertNull(rs.getBigDecimal(1));
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal(1), value.add(increment));
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal(1), BigDecimal.ZERO.setScale(scale));
                    assertFalse(rs.next());
                }

                // values that do not fit the return type are rejected
                DuckDBFunctions.scalarFunction()
                    .withName("java_wide_overflow")
                    .withParameter(int.class)
                    .withReturnType(DuckDBLogicalType.decimal(width, scale))
                    .withVectorizedFunction((input, output) -> {
                        input.stream().forEach(row -> output.setBigDecimal(row, fullWidthValue(width + 1, scale)));
                    })
                    .register(conn);
                assertThrows(() -> { stmt.executeQuery("SELECT java_wide_overflow(1)").close(); }, SQLException.class);
            }
        }
    }

    public static void test_wide_decimal_table_function_parameter() throws Exception {
        // 60 digits: beyond 128 bits, 300 digits: the width is beyond 255, the last value fits in 128 bits
        String[] typeNames = new String[] {"DECIMAL(60,2)", "DECIMAL(300,2)", "DECIMAL(60,2)"};
        BigDecimal[] values =
            new BigDecimal[] {fullWidthValue(60, 2).negate(), fullWidthValue(300, 2), new BigDecimal("-12345.67")};
        for (int i = 0; i < values.length; i++) {
            String typeName = typeNames[i];
            BigDecimal value = values[i];
            int width = Integer.parseInt(typeName.substring("DECIMAL(".length(), typeName.indexOf(',')));
            AtomicReference<BigDecimal> seen = new AtomicReference<>();
            try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
                 DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(width, 2)) {
                DuckDBFunctions.tableFunction()
                    .withName("java_wide_param")
                    .withParameter(decimalType)
                    .withFunction(new DuckDBTableFunction<BigDecimal, AtomicBoolean, Object>() {
                        @Override
                        public BigDecimal bind(DuckDBTableFunctionBindInfo info) throws Exception {
                            info.addResultColumn("col1", decimalType);
                            BigDecimal param = info.getParameter(0).getBigDecimal();
                            seen.set(param);
                            return param;
                        }

                        @Override
                        public AtomicBoolean init(DuckDBTableFunctionInitInfo info) throws Exception {
                            return new AtomicBoolean(false);
                        }

                        @Override
                        public long apply(DuckDBTableFunctionCallInfo info, DuckDBDataChunkWriter output)
                            throws Exception {
                            AtomicBoolean done = info.getInitData();
                            if (done.getAndSet(true)) {
                                return 0;
                            }
                            BigDecimal bindData = info.getBindData();
                            output.vector(0).setBigDecimal(0, bindData);
                            output.vector(0).setNull(1);
                            return 2;
                        }
                    })
                    .register(conn);
                try (ResultSet rs = stmt.executeQuery("FROM java_wide_param(CAST('" + value.toPlainString() + "' AS " +
                                                      typeName + "))")) {
                    assertEquals(seen.get(), value);
                    assertEquals(rs.getMetaData().getColumnTypeName(1), typeName);
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal(1), value);
                    assertTrue(rs.next());
                    assertNull(rs.getBigDecimal(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_wide_decimal_logical_type() throws Exception {
        int[][] params = new int[][] {{39, 0}, {76, 76}, {255, 10}, {256, 1}, {300, 299}, {1000000, 5}};
        for (int[] param : params) {
            try (DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(param[0], param[1])) {
                ByteBuffer decimalRef = decimalType.logicalTypeRef();
                assertEquals(duckdb_get_type_id(decimalRef), DUCKDB_TYPE_DECIMAL.typeId);
                assertEquals(duckdb_decimal_width(decimalRef), param[0]);
                assertEquals(duckdb_decimal_scale(decimalRef), param[1]);
                assertEquals(duckdb_decimal_internal_type(decimalRef), DUCKDB_TYPE_VARINT.typeId);
            }
        }
        assertThrows(() -> { DuckDBLogicalType.decimal(0, 0); }, SQLException.class);
        assertThrows(() -> { DuckDBLogicalType.decimal(300, 301); }, SQLException.class);
    }

    public static void test_wide_decimal_type_info() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            for (int maxWidth : new int[] {1000000, 38, 100000}) {
                if (maxWidth != 1000000) {
                    stmt.execute("SET max_decimal_width = " + maxWidth);
                }
                int count = 0;
                try (ResultSet rs = conn.getMetaData().getTypeInfo()) {
                    while (rs.next()) {
                        if ("DECIMAL".equals(rs.getString("TYPE_NAME"))) {
                            assertEquals(rs.getInt("PRECISION"), maxWidth);
                            assertEquals(rs.getInt("MAXIMUM_SCALE"), Math.min(maxWidth, Short.MAX_VALUE));
                            count++;
                        } else {
                            assertEquals(rs.getInt("PRECISION"), 0);
                        }
                    }
                }
                assertEquals(count, 2);
            }
        }
    }
}

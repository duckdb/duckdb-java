package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

public class TestNumeric {

    public static void test_bigdecimal() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE q (id DECIMAL(3,0), dec16 DECIMAL(4,1), dec32 DECIMAL(9,4), dec64 DECIMAL(18,7), dec128 DECIMAL(38,10))");

            try (PreparedStatement ps1 =
                     conn.prepareStatement("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (?, ?, ?, ?, ?)")) {
                ps1.setObject(1, new BigDecimal("1"));
                ps1.setObject(2, new BigDecimal("999.9"));
                ps1.setObject(3, new BigDecimal("99999.9999"));
                ps1.setObject(4, new BigDecimal("99999999999.9999999"));
                ps1.setObject(5, new BigDecimal("9999999999999999999999999999.9999999999"));
                ps1.execute();

                ps1.clearParameters();
                ps1.setBigDecimal(1, new BigDecimal("2"));
                ps1.setBigDecimal(2, new BigDecimal("-999.9"));
                ps1.setBigDecimal(3, new BigDecimal("-99999.9999"));
                ps1.setBigDecimal(4, new BigDecimal("-99999999999.9999999"));
                ps1.setBigDecimal(5, new BigDecimal("-9999999999999999999999999999.9999999999"));
                ps1.execute();

                ps1.clearParameters();
                ps1.setObject(1, new BigDecimal("3"), Types.DECIMAL);
                ps1.setObject(2, new BigDecimal("-5"), Types.DECIMAL);
                ps1.setObject(3, new BigDecimal("-999"), Types.DECIMAL);
                ps1.setObject(4, new BigDecimal("-88888888"), Types.DECIMAL);
                ps1.setObject(5, new BigDecimal("-123456789654321"), Types.DECIMAL);
                ps1.execute();
            }

            stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (4, -0, -0, -0, -0)");
            stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (5, 0, 0, 0, 18446744073709551615)");
            stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (6, 0, 0, 0, 18446744073709551616)");
            stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (7, 0, 0, 0, -18446744073709551615)");
            stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (8, 0, 0, 0, -18446744073709551616)");

            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM q ORDER BY id")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        assertEquals(rs.getBigDecimal(1), rs.getObject(1, BigDecimal.class));
                        assertEquals(rs.getBigDecimal(2), rs.getObject(2, BigDecimal.class));
                        assertEquals(rs.getBigDecimal(3), rs.getObject(3, BigDecimal.class));
                        assertEquals(rs.getBigDecimal(4), rs.getObject(4, BigDecimal.class));
                        assertEquals(rs.getBigDecimal(5), rs.getObject(5, BigDecimal.class));
                    }
                }

                try (ResultSet rs2 = ps.executeQuery()) {
                    DuckDBResultSetMetaData meta = rs2.getMetaData().unwrap(DuckDBResultSetMetaData.class);
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("1"));
                    assertEquals(rs2.getBigDecimal(2), new BigDecimal("999.9"));
                    assertEquals(rs2.getBigDecimal(3), new BigDecimal("99999.9999"));
                    assertEquals(rs2.getBigDecimal(4), new BigDecimal("99999999999.9999999"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("9999999999999999999999999999.9999999999"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("2"));
                    assertEquals(rs2.getBigDecimal(2), new BigDecimal("-999.9"));
                    assertEquals(rs2.getBigDecimal(3), new BigDecimal("-99999.9999"));
                    assertEquals(rs2.getBigDecimal(4), new BigDecimal("-99999999999.9999999"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("-9999999999999999999999999999.9999999999"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("3"));
                    assertEquals(rs2.getBigDecimal(2), new BigDecimal("-5.0"));
                    assertEquals(rs2.getBigDecimal(3), new BigDecimal("-999.0000"));
                    assertEquals(rs2.getBigDecimal(4), new BigDecimal("-88888888.0000000"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("-123456789654321.0000000000"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("4"));
                    assertEquals(rs2.getBigDecimal(2), new BigDecimal("-0.0"));
                    assertEquals(rs2.getBigDecimal(3), new BigDecimal("-0.0000"));
                    assertEquals(rs2.getBigDecimal(4), new BigDecimal("-0.0000000"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("-0.0000000000"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("5"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("18446744073709551615.0000000000"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("6"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("18446744073709551616.0000000000"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("7"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("-18446744073709551615.0000000000"));
                    rs2.next();
                    assertEquals(rs2.getBigDecimal(1), new BigDecimal("8"));
                    assertEquals(rs2.getBigDecimal(5), new BigDecimal("-18446744073709551616.0000000000"));

                    // Metadata tests
                    assertEquals(Types.DECIMAL, meta.type_to_int(DuckDBColumnType.DECIMAL));
                    assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(1)));
                    assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(2)));
                    assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(3)));
                    assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(4)));

                    assertEquals(3, meta.getPrecision(1));
                    assertEquals(0, meta.getScale(1));
                    assertEquals(4, meta.getPrecision(2));
                    assertEquals(1, meta.getScale(2));
                    assertEquals(9, meta.getPrecision(3));
                    assertEquals(4, meta.getScale(3));
                    assertEquals(18, meta.getPrecision(4));
                    assertEquals(7, meta.getScale(4));
                    assertEquals(38, meta.getPrecision(5));
                    assertEquals(10, meta.getScale(5));
                }
            }
        }
    }

    public static void test_lots_of_decimals() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {

            try (Statement stmt = conn.createStatement()) {
                // Create the table
                stmt.execute(
                    "CREATE TABLE q (id DECIMAL(4,0),dec32 DECIMAL(9,4),dec64 DECIMAL(18,7),dec128 DECIMAL(38,10))");
                stmt.close();
            }

            // Create the INSERT prepared statement we will use
            try (PreparedStatement ps1 =
                     conn.prepareStatement("INSERT INTO q (id, dec32, dec64, dec128) VALUES (?, ?, ?, ?)")) {

                // Create the Java decimals we will be inserting
                BigDecimal id_org = new BigDecimal("1");
                BigDecimal dec32_org = new BigDecimal("99999.9999");
                BigDecimal dec64_org = new BigDecimal("99999999999.9999999");
                BigDecimal dec128_org = new BigDecimal("9999999999999999999999999999.9999999999");

                // Insert the initial values
                ps1.setObject(1, id_org);
                ps1.setObject(2, dec32_org);
                ps1.setObject(3, dec64_org);
                ps1.setObject(4, dec128_org);
                // This does not have a result set
                assertFalse(ps1.execute());

                // Create the SELECT prepared statement we will use
                try (PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM q WHERE id = ?")) {
                    BigDecimal multiplicant = new BigDecimal("0.987");

                    BigDecimal dec32;
                    BigDecimal dec64;
                    BigDecimal dec128;

                    for (int i = 2; i < 10000; i++) {
                        ps2.setObject(1, new BigDecimal(i - 1));

                        // Verify that both the 'getObject' and the 'getBigDecimal' methods return the same value\

                        try (ResultSet rs = ps2.executeQuery()) {
                            assertTrue(rs.next());
                            dec32 = rs.getObject(2, BigDecimal.class);
                            dec64 = rs.getObject(3, BigDecimal.class);
                            dec128 = rs.getObject(4, BigDecimal.class);
                            assertEquals(dec32_org, dec32);
                            assertEquals(dec64_org, dec64);
                            assertEquals(dec128_org, dec128);
                        }

                        try (ResultSet rs = ps2.executeQuery()) {
                            assertTrue(rs.next());
                            dec32 = rs.getBigDecimal(2);
                            dec64 = rs.getBigDecimal(3);
                            dec128 = rs.getBigDecimal(4);
                            assertEquals(dec32_org, dec32);
                            assertEquals(dec64_org, dec64);
                            assertEquals(dec128_org, dec128);
                        }

                        // Apply the modification for the next iteration

                        dec32_org = dec32_org.multiply(multiplicant).setScale(4, java.math.RoundingMode.HALF_EVEN);
                        dec64_org = dec64_org.multiply(multiplicant).setScale(7, java.math.RoundingMode.HALF_EVEN);
                        dec128_org = dec128_org.multiply(multiplicant).setScale(10, java.math.RoundingMode.HALF_EVEN);

                        ps1.clearParameters();
                        ps1.setObject(1, new BigDecimal(i));
                        ps1.setObject(2, dec32_org);
                        ps1.setObject(3, dec64_org);
                        ps1.setObject(4, dec128_org);
                        assertFalse(ps1.execute());

                        ps2.clearParameters();
                    }
                }
            }
        }
    }

    public static void test_hugeint() throws Exception {
        try (
            Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 42::hugeint hi1, -42::hugeint hi2, 454564646545646546545646545::hugeint hi3, -454564646545646546545646545::hugeint hi4")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject("hi1"), new BigInteger("42"));
            assertEquals(rs.getObject("hi2"), new BigInteger("-42"));
            assertEquals(rs.getLong("hi1"), 42L);
            assertEquals(rs.getLong("hi2"), -42L);
            assertEquals(rs.getObject("hi3"), new BigInteger("454564646545646546545646545"));
            assertEquals(rs.getObject("hi4"), new BigInteger("-454564646545646546545646545"));
            assertTrue(rs.getBigDecimal("hi1").compareTo(new BigDecimal("42")) == 0);
            assertTrue(rs.getBigDecimal("hi2").compareTo(new BigDecimal("-42")) == 0);
            assertTrue(rs.getBigDecimal("hi3").compareTo(new BigDecimal("454564646545646546545646545")) == 0);
            assertTrue(rs.getBigDecimal("hi4").compareTo(new BigDecimal("-454564646545646546545646545")) == 0);
            assertFalse(rs.next());
        }
    }

    public static void test_unsigned_integers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT 201::utinyint uint8, 40001::usmallint uint16, 4000000001::uinteger uint32, 18446744073709551615::ubigint uint64")) {
                assertTrue(rs.next());

                assertEquals(rs.getShort("uint8"), Short.valueOf((short) 201));
                assertEquals(rs.getObject("uint8"), Short.valueOf((short) 201));
                assertEquals(rs.getInt("uint8"), Integer.valueOf((int) 201));

                assertEquals(rs.getInt("uint16"), Integer.valueOf((int) 40001));
                assertEquals(rs.getObject("uint16"), Integer.valueOf((int) 40001));
                assertEquals(rs.getLong("uint16"), Long.valueOf((long) 40001));

                assertEquals(rs.getLong("uint32"), Long.valueOf((long) 4000000001L));
                assertEquals(rs.getObject("uint32"), Long.valueOf((long) 4000000001L));

                assertEquals(rs.getObject("uint64"), new BigInteger("18446744073709551615"));
            }

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT NULL::utinyint uint8, NULL::usmallint uint16, NULL::uinteger uint32, NULL::ubigint uint64")) {
                assertTrue(rs.next());

                rs.getObject(1);
                assertTrue(rs.wasNull());

                rs.getObject(2);
                assertTrue(rs.wasNull());

                rs.getObject(3);
                assertTrue(rs.wasNull());

                rs.getObject(4);
                assertTrue(rs.wasNull());
            }
        }
    }

    private static <T> void check_get_object_with_class(ResultSet rs, long value, Class<T> clazz) throws Exception {
        assertTrue(rs.getObject(1, clazz).getClass().equals(clazz));
        if (clazz.equals(Byte.class)) {
            assertEquals(rs.getObject(1, clazz), (byte) value);
        } else if (clazz.equals(Short.class)) {
            assertEquals(rs.getObject(1, clazz), (short) value);
        } else if (clazz.equals(Integer.class)) {
            assertEquals(rs.getObject(1, clazz), (int) value);
        } else if (clazz.equals(Long.class)) {
            assertEquals(rs.getObject(1, clazz), value);
        } else if (clazz.equals(BigInteger.class)) {
            assertEquals(rs.getObject(1, clazz), BigInteger.valueOf(value));
        } else if (clazz.equals(BigDecimal.class)) {
            assertEquals(rs.getObject(1, clazz), BigDecimal.valueOf(value));
        }
    }

    public static void test_duckdb_get_object_tinyint() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::TINYINT")) {
                Class<?>[] classes = new Class[] {Byte.class, Short.class,      Integer.class,
                                                  Long.class, BigInteger.class, BigDecimal.class};
                ps.setByte(1, Byte.MIN_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Byte.MIN_VALUE, cl);
                    }
                }
                ps.setByte(1, Byte.MAX_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Byte.MAX_VALUE, cl);
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::UTINYINT")) {
                Class<?>[] classes =
                    new Class[] {Short.class, Integer.class, Long.class, BigInteger.class, BigDecimal.class};
                short MAX_UTINYINT = (1 << 8) - 1;
                ps.setShort(1, MAX_UTINYINT);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, MAX_UTINYINT, cl);
                    }
                    assertThrows(() -> check_get_object_with_class(rs, MAX_UTINYINT, Byte.class), SQLException.class);
                }
            }
        }
    }

    public static void test_duckdb_get_object_smallint() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::SMALLINT")) {
                Class<?>[] classes =
                    new Class[] {Short.class, Integer.class, Long.class, BigInteger.class, BigDecimal.class};
                ps.setShort(1, Short.MIN_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Short.MIN_VALUE, cl);
                    }
                }
                ps.setShort(1, Short.MAX_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Short.MAX_VALUE, cl);
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::USMALLINT")) {
                Class<?>[] classes = new Class[] {Integer.class, Long.class, BigInteger.class, BigDecimal.class};
                int MAX_USMALLINT = (1 << 16) - 1;
                ps.setInt(1, MAX_USMALLINT);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, MAX_USMALLINT, cl);
                    }
                    assertThrows(() -> rs.getObject(1, Byte.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Short.class), SQLException.class);
                }
            }
        }
    }

    public static void test_duckdb_get_object_integer() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::INTEGER")) {
                Class<?>[] classes = new Class[] {Integer.class, Long.class, BigInteger.class, BigDecimal.class};
                ps.setInt(1, Integer.MIN_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Integer.MIN_VALUE, cl);
                    }
                }
                ps.setInt(1, Integer.MAX_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Integer.MAX_VALUE, cl);
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::UINTEGER")) {
                Class<?>[] classes = new Class[] {Long.class, BigInteger.class, BigDecimal.class};
                long MAX_UINTEGER = (1L << 32) - 1;
                ps.setLong(1, MAX_UINTEGER);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, MAX_UINTEGER, cl);
                    }
                    assertThrows(() -> rs.getObject(1, Byte.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Short.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Integer.class), SQLException.class);
                }
            }
        }
    }

    public static void test_duckdb_get_object_bigint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::BIGINT")) {
                Class<?>[] classes = new Class[] {Long.class, BigInteger.class, BigDecimal.class};
                ps.setLong(1, Long.MIN_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Long.MIN_VALUE, cl);
                    }
                }
                ps.setLong(1, Long.MAX_VALUE);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    for (Class<?> cl : classes) {
                        check_get_object_with_class(rs, Long.MAX_VALUE, cl);
                    }
                }
            }
            try (DuckDBPreparedStatement ps =
                     conn.prepareStatement("SELECT ?::UBIGINT").unwrap(DuckDBPreparedStatement.class)) {
                BigInteger maxUbigint = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
                ps.setBigInteger(1, maxUbigint);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    assertTrue(rs.getObject(1, BigInteger.class).getClass().equals(BigInteger.class));
                    assertEquals(rs.getObject(1, BigInteger.class), maxUbigint);

                    assertTrue(rs.getObject(1, BigDecimal.class).getClass().equals(BigDecimal.class));
                    assertEquals(rs.getObject(1, BigDecimal.class), new BigDecimal(maxUbigint));

                    assertThrows(() -> rs.getObject(1, Byte.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Short.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Integer.class), SQLException.class);
                    assertThrows(() -> rs.getObject(1, Long.class), SQLException.class);
                }
            }
        }
    }

    public static void test_duckdb_get_object_hugeint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            try (DuckDBPreparedStatement ps =
                     conn.prepareStatement("SELECT ?::HUGEINT").unwrap(DuckDBPreparedStatement.class)) {
                BigInteger minVal = BigInteger.ONE.shiftLeft(127).negate();
                long minValLower = minVal.longValue();
                long minValHigher = minVal.shiftRight(64).longValue();
                ps.setHugeInt(1, conn.createHugeInt(minValLower, minValHigher));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    assertTrue(rs.getObject(1, BigInteger.class).getClass().equals(BigInteger.class));
                    assertEquals(rs.getObject(1, BigInteger.class), minVal);

                    assertTrue(rs.getObject(1, BigDecimal.class).getClass().equals(BigDecimal.class));
                    assertEquals(rs.getObject(1, BigDecimal.class), new BigDecimal(minVal));
                }
                BigInteger maxVal = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
                ps.setBigInteger(1, maxVal);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    assertTrue(rs.getObject(1, BigInteger.class).getClass().equals(BigInteger.class));
                    assertEquals(rs.getObject(1, BigInteger.class), maxVal);

                    assertTrue(rs.getObject(1, BigDecimal.class).getClass().equals(BigDecimal.class));
                    assertEquals(rs.getObject(1, BigDecimal.class), new BigDecimal(maxVal));
                }

                // UHUGEINT is not supported
                assertThrows(() -> { ps.setBigInteger(1, minVal.subtract(BigInteger.ONE)); }, SQLException.class);
                assertThrows(() -> { ps.setBigInteger(1, maxVal.add(BigInteger.ONE)); }, SQLException.class);
            }
        }
    }

    public static void test_numeric_types_mapping() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 42::TINYINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.TINYINT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::UTINYINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.SMALLINT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::SMALLINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.SMALLINT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::USMALLINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.INTEGER);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::INTEGER")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.INTEGER);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::UINTEGER")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.BIGINT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::BIGINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.BIGINT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::UBIGINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::HUGEINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::UHUGEINT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::FLOAT")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.FLOAT);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::DOUBLE")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.DOUBLE);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::DECIMAL")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.DECIMAL);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 42::NUMERIC")) {
                rs.next();
                assertEquals(rs.getMetaData().getColumnType(1), Types.DECIMAL);
            }
        }
    }
}

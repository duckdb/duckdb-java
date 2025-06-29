package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertTrue;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

public class TestAppender {

    public static void test_appender_numbers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            // int8, int4, int2, int1, float8, float4
            stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers")) {
                for (int i = 0; i < 50; i++) {
                    appender.beginRow()
                        .append(Long.MAX_VALUE - i)
                        .append(Integer.MAX_VALUE - i)
                        .append(Short.MAX_VALUE - i)
                        .append(Byte.MAX_VALUE - i)
                        .append(i)
                        .append(i)
                        .endRow();
                }
                appender.flush();
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT max(a), max(b), max(c), max(d), max(e), max(f) FROM numbers")) {
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
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static void test_appender_date_and_time() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            LocalDateTime ldt1 = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
            LocalDateTime ldt2 = LocalDateTime.of(-23434, 3, 5, 23, 2);
            LocalDateTime ldt3 = LocalDateTime.of(1970, 1, 1, 0, 0);
            LocalDateTime ldt4 = LocalDateTime.of(11111, 12, 31, 23, 59, 59, 999999000);

            stmt.execute("CREATE TABLE date_and_time (id INT4, a TIMESTAMP)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "date_and_time")) {
                appender.beginRow()
                    .append(1)
                    .append(ldt1)
                    .endRow()
                    .beginRow()
                    .append(2)
                    .append(ldt2)
                    .endRow()
                    .beginRow()
                    .append(3)
                    .append(ldt3)
                    .endRow()
                    .beginRow()
                    .append(4)
                    // deprecated call
                    .appendLocalDateTime(ldt4)
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT a FROM date_and_time ORDER BY id")) {
                assertFalse(rs.isClosed());
                assertTrue(rs.next());

                LocalDateTime res1 = rs.getObject(1, LocalDateTime.class);
                assertEquals(res1, ldt1);
                assertTrue(rs.next());

                LocalDateTime res2 = rs.getObject(1, LocalDateTime.class);
                assertEquals(res2, ldt2);
                assertTrue(rs.next());

                LocalDateTime res3 = rs.getObject(1, LocalDateTime.class);
                assertEquals(res3, ldt3);
                assertTrue(rs.next());

                LocalDateTime res4 = rs.getObject(1, LocalDateTime.class);
                assertEquals(res4, ldt4);
            }
        }
    }

    // todo
    public static void DISABLED_test_appender_date() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            LocalDate ld1 = LocalDate.now();
            LocalDate ld2 = LocalDate.of(-23434, 3, 5);
            LocalDate ld3 = LocalDate.of(1970, 1, 1);
            LocalDate ld4 = LocalDate.of(11111, 12, 31);
            LocalDate ld5 = LocalDate.of(999999999, 12, 31);

            stmt.execute("CREATE TABLE date_only (id INT4, a DATE)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "date_only")) {
                appender.beginRow()
                    .append(1)
                    .append(ld1)
                    .endRow()
                    .beginRow()
                    .append(2)
                    .append(ld2)
                    .endRow()
                    .beginRow()
                    .append(3)
                    .append(ld3)
                    .endRow()
                    .beginRow()
                    .append(4)
                    .append(ld4)
                    .endRow()
                    .beginRow()
                    .append(5);
                assertThrows(() -> { appender.append(ld5); }, SQLException.class);
                appender.append(ld4).endRow().flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT a FROM date_only ORDER BY id")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                LocalDate res1 = rs.getObject(1, LocalDate.class);
                assertEquals(res1, ld1);

                assertTrue(rs.next());
                LocalDate res2 = rs.getObject(1, LocalDate.class);
                assertEquals(res2, ld2);

                assertTrue(rs.next());
                LocalDate res3 = rs.getObject(1, LocalDate.class);
                assertEquals(res3, ld3);

                assertTrue(rs.next());
                LocalDate res4 = rs.getObject(1, LocalDate.class);
                assertEquals(res4, ld4);

                assertTrue(rs.next());
                LocalDate res5 = rs.getObject(1, LocalDate.class);
                assertEquals(res5, ld4);

                assertFalse(rs.next());
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static void test_appender_decimal() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

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

            stmt.execute(
                "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                appender.beginRow()
                    .append(1)
                    .append(bigdec16)
                    .append(bigdec32)
                    .append(bigdec64)
                    .append(bigdec128)
                    .endRow()
                    .beginRow()
                    .append(2)
                    .append(negbigdec16)
                    .append(negbigdec32)
                    .append(negbigdec64)
                    .append(negbigdec128)
                    .endRow()
                    .beginRow()
                    .append(3)
                    .append(smallbigdec16)
                    .append(smallbigdec32)
                    .append(smallbigdec64)
                    .append(smallbigdec128)
                    .endRow()
                    .beginRow()
                    .append(4)
                    .append(intbigdec16)
                    .append(intbigdec32)
                    .append(intbigdec64)
                    .append(intbigdec128)
                    .endRow()
                    .beginRow()
                    .append(5)
                    .append(onebigdec16)
                    .append(onebigdec32)
                    .append(onebigdec64)
                    // deprecated call
                    .appendBigDecimal(onebigdec128)
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT a,b,c,d FROM decimals ORDER BY id")) {
                assertFalse(rs.isClosed());
                assertTrue(rs.next());

                BigDecimal rs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal rs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal rs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal rs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(rs1, bigdec16);
                assertEquals(rs2, bigdec32);
                assertEquals(rs3, bigdec64);
                assertEquals(rs4, bigdec128);
                assertTrue(rs.next());

                BigDecimal nrs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal nrs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal nrs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal nrs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(nrs1, negbigdec16);
                assertEquals(nrs2, negbigdec32);
                assertEquals(nrs3, negbigdec64);
                assertEquals(nrs4, negbigdec128);
                assertTrue(rs.next());

                BigDecimal srs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal srs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal srs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal srs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(srs1, smallbigdec16);
                assertEquals(srs2, smallbigdec32);
                assertEquals(srs3, smallbigdec64);
                assertEquals(srs4, smallbigdec128);
                assertTrue(rs.next());

                BigDecimal irs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal irs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal irs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal irs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(irs1, intbigdec16);
                assertEquals(irs2, intbigdec32);
                assertEquals(irs3, intbigdec64);
                assertEquals(irs4, intbigdec128);
                assertTrue(rs.next());

                BigDecimal oners1 = rs.getObject(1, BigDecimal.class);
                BigDecimal oners2 = rs.getObject(2, BigDecimal.class);
                BigDecimal oners3 = rs.getObject(3, BigDecimal.class);
                BigDecimal oners4 = rs.getObject(4, BigDecimal.class);

                assertEquals(oners1, onebigdec16);
                assertEquals(oners2, onebigdec32);
                assertEquals(oners3, onebigdec64);
                assertEquals(oners4, onebigdec128);
            }
        }
    }

    public static void test_appender_decimal_wrong_scale() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute(
                "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.append(1).beginRow().append(new BigDecimal("121.14").setScale(2));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.beginRow()
                        .append(2)
                        .append(new BigDecimal("21.1").setScale(2))
                        .append(new BigDecimal("12111.1411").setScale(4));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.beginRow()
                        .append(3)
                        .append(new BigDecimal("21.1").setScale(2))
                        .append(new BigDecimal("21.1").setScale(4))
                        .append(new BigDecimal("1234567890123.123456").setScale(6));
                }
            }, SQLException.class);
        }
    }

    public static void test_appender_int_string() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, s VARCHAR)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                for (int i = 0; i < 1000; i++) {
                    appender.beginRow().append(i).append("str " + i).endRow();
                }
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT max(a), min(s) FROM data")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                int resA = rs.getInt(1);
                assertEquals(resA, 999);
                String resB = rs.getString(2);
                assertEquals(resB, "str 0");
            }
        }
    }

    public static void test_appender_string_with_emoji() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE data (id INTEGER, str_value VARCHAR(10))");
            String expectedValue = "䭔\uD86D\uDF7C🔥\uD83D\uDE1C";
            char cjk1 = '䭔';
            char cjk2 = '字';

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().append(1).append(expectedValue).endRow();
                // append char
                appender.beginRow().append(2).append(cjk1).endRow();
                // append char array
                appender.beginRow().append(3).append(new char[] {cjk1, cjk2}).endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT str_value FROM data ORDER BY id")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                String row1 = rs.getString(1);
                assertEquals(row1, expectedValue);

                assertTrue(rs.next());
                String row2 = rs.getString(1);
                assertEquals(row2, String.valueOf(cjk1));

                assertTrue(rs.next());
                String row3 = rs.getString(1);
                assertEquals(row3, String.valueOf(new char[] {cjk1, cjk2}));

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_table_does_not_exist() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(() -> { conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data"); }, SQLException.class);
        }
    }

    public static void test_appender_table_deleted() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            DuckDBAppender appender =
                conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data").beginRow().append(1).endRow();

            stmt.execute("DROP TABLE data");

            appender.beginRow().append(2).endRow();

            assertThrows(appender::flush, SQLException.class);
        }
    }

    public static void test_appender_append_too_many_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            assertThrows(() -> {
                DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
                appender.beginRow().append(1).append(2).flush();
            }, SQLException.class);
        }
    }

    public static void test_appender_append_too_few_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append(1).endRow(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_type_mismatch() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append("str"); }, SQLException.class);
            }
        }
    }

    public static void test_appender_null_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().appendNull().endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                // java.sql.ResultSet.getInt(int) returns 0 if the value is NULL
                assertEquals(0, results.getInt(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_appender_boolean_wrapper() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();) {

            stmt.execute("CREATE TABLE data (a BOOLEAN)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                Boolean[] values = new Boolean[] {Boolean.TRUE, Boolean.FALSE, null};
                for (Boolean value : values) {
                    appender.beginRow().append(value).endRow();
                }
                appender.flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT count(1) FROM data WHERE a = true");) {
                assertTrue(results.next());
                assertEquals(1, results.getInt(1));
            }
            try (ResultSet results = stmt.executeQuery("SELECT count(1) FROM data WHERE a = false");) {
                assertTrue(results.next());
                assertEquals(1, results.getInt(1));
            }
            try (ResultSet results = stmt.executeQuery("SELECT count(1) FROM data WHERE a is null");) {
                assertTrue(results.next());
                assertEquals(1, results.getInt(1));
            }
        }
    }

    public static void test_appender_char_wrapper() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE data (str_value VARCHAR)");
            Character cjk1 = '䭔';

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().append(cjk1).endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT str_value FROM data")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                String row2 = rs.getString(1);
                assertEquals(row2, String.valueOf(cjk1));

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_number_wrappers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            // int8, int4, int2, int1, float8, float4
            stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers")) {
                for (int i = 0; i < 50; i++) {
                    appender.beginRow()
                        .append(Long.valueOf(Long.MAX_VALUE - i))
                        .append(Integer.valueOf(Integer.MAX_VALUE - i))
                        .append(Short.valueOf((short) (Short.MAX_VALUE - i)))
                        .append(Byte.valueOf((byte) (Byte.MAX_VALUE - i)))
                        .append(Double.valueOf(i))
                        .append(Float.valueOf(i))
                        .endRow();
                }
                appender.flush();
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT max(a), max(b), max(c), max(d), max(e), max(f) FROM numbers")) {
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
            }
        }
    }

    public static void test_appender_null_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a VARCHAR)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().appendNull().endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                assertNull(results.getString(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_appender_append_null_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a VARCHAR)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().appendNull().endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                assertNull(results.getString(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_appender_append_null_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().appendNull().endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                assertNull(results.getString(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_appender_roundtrip_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            SecureRandom random = SecureRandom.getInstanceStrong();
            byte[] data = new byte[512];
            random.nextBytes(data);

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().append(data).endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());

                Blob resultBlob = results.getBlob(1);
                byte[] resultBytes = resultBlob.getBytes(1, (int) resultBlob.length());
                assertTrue(Arrays.equals(resultBytes, data), "byte[] data is round tripped untouched");
            }
        }
    }
}

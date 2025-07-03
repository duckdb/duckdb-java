package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertTrue;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

@SuppressWarnings("deprecation")
public class TestSingleValueAppender {

    public static void test_sv_appender_numbers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            // int8, int4, int2, int1, float8, float4
            stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers")) {
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

    public static void test_sv_appender_date_and_time() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            LocalDateTime ldt1 = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
            LocalDateTime ldt2 = LocalDateTime.of(-23434, 3, 5, 23, 2);
            LocalDateTime ldt3 = LocalDateTime.of(1970, 1, 1, 0, 0);
            LocalDateTime ldt4 = LocalDateTime.of(11111, 12, 31, 23, 59, 59, 999999000);

            stmt.execute("CREATE TABLE date_and_time (id INT4, a TIMESTAMP)");
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "date_and_time")) {
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
                appender.flush();
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

    public static void test_sv_appender_decimal() throws Exception {
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

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
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
                appender.flush();
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

    public static void test_sv_appender_decimal_wrong_scale() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute(
                "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

            assertThrows(() -> {
                try (DuckDBSingleValueAppender appender =
                         conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.append(1);
                    appender.beginRow();
                    appender.appendBigDecimal(new BigDecimal("121.14").setScale(2));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBSingleValueAppender appender =
                         conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.beginRow();
                    appender.append(2);
                    appender.appendBigDecimal(new BigDecimal("21.1").setScale(2));
                    appender.appendBigDecimal(new BigDecimal("12111.1411").setScale(4));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBSingleValueAppender appender =
                         conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "decimals")) {
                    appender.beginRow();
                    appender.append(3);
                    appender.appendBigDecimal(new BigDecimal("21.1").setScale(2));
                    appender.appendBigDecimal(new BigDecimal("21.1").setScale(4));
                    appender.appendBigDecimal(new BigDecimal("1234567890123.123456").setScale(6));
                }
            }, SQLException.class);
        }
    }

    public static void test_sv_appender_int_string() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, s VARCHAR)");

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                for (int i = 0; i < 1000; i++) {
                    appender.beginRow();
                    appender.append(i);
                    appender.append("str " + i);
                    appender.endRow();
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

    public static void test_sv_appender_string_with_emoji() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE data (id INTEGER, str_value VARCHAR(10))");
            String expectedValue = "䭔\uD86D\uDF7C🔥\uD83D\uDE1C";

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow();
                appender.append(1);
                appender.append(expectedValue);
                appender.endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT str_value FROM data ORDER BY id")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                String row1 = rs.getString(1);
                assertEquals(row1, expectedValue);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_sv_appender_table_does_not_exist() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(
                () -> { conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data"); }, SQLException.class);
        }
    }

    public static void test_sv_appender_table_deleted() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            DuckDBSingleValueAppender appender =
                conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
            appender.beginRow();
            appender.append(1);
            appender.endRow();

            stmt.execute("DROP TABLE data");

            appender.beginRow();
            appender.append(2);
            appender.endRow();

            assertThrows(appender::flush, SQLException.class);
        }
    }

    public static void test_sv_appender_append_too_many_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            assertThrows(() -> {
                DuckDBSingleValueAppender appender =
                    conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
                appender.beginRow();
                appender.append(1);
                appender.append(2);
                appender.flush();
            }, SQLException.class);
        }
    }

    public static void test_sv_appender_append_too_few_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> {
                    appender.beginRow();
                    appender.append(1);
                    appender.endRow();
                }, SQLException.class);
            }
        }
    }

    public static void test_sv_appender_type_mismatch() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> {
                    appender.beginRow();
                    appender.append("str");
                }, SQLException.class);
            }
        }
    }

    public static void test_sv_appender_null_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow();
                appender.append((String) null);
                appender.endRow();
                appender.flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                // java.sql.ResultSet.getInt(int) returns 0 if the value is NULL
                assertEquals(0, results.getInt(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_sv_appender_number_wrappers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            // int8, int4, int2, int1, float8, float4
            stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers")) {
                for (int i = 0; i < 50; i++) {
                    appender.beginRow();
                    appender.append(Long.valueOf(Long.MAX_VALUE - i));
                    appender.append(Integer.valueOf(Integer.MAX_VALUE - i));
                    appender.append(Short.valueOf((short) (Short.MAX_VALUE - i)));
                    appender.append(Byte.valueOf((byte) (Byte.MAX_VALUE - i)));
                    appender.append(Double.valueOf(i));
                    appender.append(Float.valueOf(i));
                    appender.endRow();
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

    public static void test_sv_appender_append_null_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a VARCHAR)");

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow();
                appender.append((String) null);
                appender.endRow();
                appender.flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                assertNull(results.getString(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_sv_appender_append_null_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow();
                appender.append((String) null);
                appender.endRow();
                appender.flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                assertNull(results.getString(1));
                assertTrue(results.wasNull());
            }
        }
    }

    public static void test_sv_appender_roundtrip_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            SecureRandom random = SecureRandom.getInstanceStrong();
            byte[] data = new byte[512];
            random.nextBytes(data);

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBSingleValueAppender appender =
                     conn.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow();
                appender.append(data);
                appender.endRow();
                appender.flush();
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

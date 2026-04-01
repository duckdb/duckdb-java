package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_INTEGER;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class TestScalarFunctions {
    private interface ResultSetVerifier {
        void verify(ResultSet rs) throws Exception;
    }

    public static void test_bindings_scalar_function() throws Exception {
        ByteBuffer intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER.typeId);
        ByteBuffer scalarFunction = duckdb_create_scalar_function();
        assertNotNull(scalarFunction);

        duckdb_scalar_function_set_name(scalarFunction, "binding_scalar_fn".getBytes(UTF_8));
        duckdb_scalar_function_add_parameter(scalarFunction, intType);
        duckdb_scalar_function_set_return_type(scalarFunction, intType);

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertEquals(duckdb_register_scalar_function(conn.connRef, scalarFunction), 1);

            assertThrows(() -> { duckdb_register_scalar_function(null, scalarFunction); }, SQLException.class);
            assertThrows(() -> { duckdb_register_scalar_function(conn.connRef, null); }, SQLException.class);
        }

        duckdb_destroy_scalar_function(scalarFunction);
        duckdb_destroy_logical_type(intType);

        assertThrows(() -> { duckdb_destroy_scalar_function(null); }, SQLException.class);
        assertThrows(() -> { duckdb_scalar_function_set_name(null, "x".getBytes(UTF_8)); }, SQLException.class);
    }

    public static void test_register_scalar_function() throws Exception {
        test_register_scalar_function_integer();
    }

    public static void test_register_scalar_function_typed_logical_type() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            conn.registerScalarFunction("java_add_int_typed", new DuckDBLogicalType[] {intType}, intType,
                                        (input, rowCount, out) -> {
                                            DuckDBReadableVector in = input.vector(0);
                                            for (int i = 0; i < rowCount; i++) {
                                                if (in.isNull(i)) {
                                                    out.setNull(i);
                                                } else {
                                                    out.setInt(i, in.getInt(i) + 1);
                                                }
                                            }
                                        });
            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_typed(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 2);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_parallel() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType bigintType = DuckDBLogicalType.of(DuckDBColumnType.BIGINT)) {
            stmt.execute("PRAGMA threads=4");
            conn.registerScalarFunction("java_add_one_bigint", new DuckDBLogicalType[] {bigintType}, bigintType,
                                        (input, rowCount, out) -> {
                                            DuckDBReadableVector in = input.vector(0);
                                            for (int i = 0; i < rowCount; i++) {
                                                out.setLong(i, in.getLong(i) + 1);
                                            }
                                        });

            try (ResultSet rs = stmt.executeQuery("SELECT sum(java_add_one_bigint(i)) FROM range(1000000) t(i)")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 500000500000L);
                assertFalse(rs.wasNull());
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_exception_propagation() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            conn.registerScalarFunction("java_throws_exception", new DuckDBLogicalType[] {intType}, intType,
                                        (input, rowCount, out) -> { throw new IllegalStateException("boom"); });
            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_throws_exception(1)"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("IllegalStateException"));
            assertTrue(message.contains("boom"));
        }
    }

    public static void test_register_scalar_function_boolean() throws Exception {
        assertUnaryScalarFunction("java_not_bool", DuckDBColumnType.BOOLEAN, DuckDBColumnType.BOOLEAN,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setBoolean(i, !in.getBoolean(i));
                                          }
                                      }
                                  },
                                  "SELECT java_not_bool(v) FROM (VALUES (TRUE), (NULL), (FALSE)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Boolean.class), false);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Boolean.class), true);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_tinyint() throws Exception {
        assertUnaryScalarFunction("java_add_tinyint", DuckDBColumnType.TINYINT, DuckDBColumnType.TINYINT,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setByte(i, (byte) (in.getByte(i) + 1));
                                          }
                                      }
                                  },
                                  "SELECT java_add_tinyint(v) FROM (VALUES (41::TINYINT), (NULL), (-2::TINYINT)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Byte.class), (byte) 42);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Byte.class), (byte) -1);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_smallint() throws Exception {
        assertUnaryScalarFunction(
            "java_add_smallint", DuckDBColumnType.SMALLINT, DuckDBColumnType.SMALLINT,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setShort(i, (short) (in.getShort(i) + 2));
                    }
                }
            },
            "SELECT java_add_smallint(v) FROM (VALUES (40::SMALLINT), (NULL), (-4::SMALLINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) -2);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_integer() throws Exception {
        assertUnaryScalarFunction("java_add_int", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setInt(i, in.getInt(i) + 1);
                                          }
                                      }
                                  },
                                  "SELECT java_add_int(v) FROM (VALUES (1), (NULL), (41)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 2);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 42);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_integer_revalidates_after_null() throws Exception {
        assertUnaryScalarFunction("java_revalidate_int", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setNull(i);
                                              out.setInt(i, in.getInt(i) + 1);
                                          }
                                      }
                                  },
                                  "SELECT java_revalidate_int(v) FROM (VALUES (41), (NULL)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 42);
                                      assertFalse(rs.wasNull());
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_bigint() throws Exception {
        assertUnaryScalarFunction("java_add_bigint", DuckDBColumnType.BIGINT, DuckDBColumnType.BIGINT,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setLong(i, in.getLong(i) + 3);
                                          }
                                      }
                                  },
                                  "SELECT java_add_bigint(v) FROM (VALUES (39::BIGINT), (NULL), (-5::BIGINT)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Long.class), 42L);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Long.class), -2L);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_utinyint() throws Exception {
        assertUnaryScalarFunction(
            "java_add_utinyint", DuckDBColumnType.UTINYINT, DuckDBColumnType.UTINYINT,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setUint8(i, in.getUint8(i) + 1);
                    }
                }
            },
            "SELECT java_add_utinyint(v) FROM (VALUES (41::UTINYINT), (NULL), (254::UTINYINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) 255);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_usmallint() throws Exception {
        assertUnaryScalarFunction(
            "java_add_usmallint", DuckDBColumnType.USMALLINT, DuckDBColumnType.USMALLINT,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setUint16(i, in.getUint16(i) + 2);
                    }
                }
            },
            "SELECT java_add_usmallint(v) FROM (VALUES (40::USMALLINT), (NULL), (65533::USMALLINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 65535);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_uinteger() throws Exception {
        assertUnaryScalarFunction(
            "java_add_uinteger", DuckDBColumnType.UINTEGER, DuckDBColumnType.UINTEGER,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setUint32(i, in.getUint32(i) + 3);
                    }
                }
            },
            "SELECT java_add_uinteger(v) FROM (VALUES (39::UINTEGER), (NULL), (4294967292::UINTEGER)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 4294967295L);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_ubigint() throws Exception {
        assertUnaryScalarFunction("java_add_ubigint", DuckDBColumnType.UBIGINT, DuckDBColumnType.UBIGINT,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      BigInteger increment = BigInteger.ONE;
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setUint64(i, in.getUint64(i).add(increment));
                                          }
                                      }
                                  },
                                  "SELECT java_add_ubigint(v) FROM (VALUES (41::UBIGINT), (NULL), "
                                      + "(18446744073709551614::UBIGINT)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, BigInteger.class),
                                                   new BigInteger("18446744073709551615"));
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_float() throws Exception {
        assertUnaryScalarFunction("java_add_float", DuckDBColumnType.FLOAT, DuckDBColumnType.FLOAT,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setFloat(i, in.getFloat(i) + 1.25f);
                                          }
                                      }
                                  },
                                  "SELECT java_add_float(v) FROM (VALUES (40.75::FLOAT), (NULL), (-2.5::FLOAT)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Float.class), 42.0f);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Float.class), -1.25f);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_double() throws Exception {
        assertUnaryScalarFunction("java_add_double", DuckDBColumnType.DOUBLE, DuckDBColumnType.DOUBLE,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setDouble(i, in.getDouble(i) + 1.5d);
                                          }
                                      }
                                  },
                                  "SELECT java_add_double(v) FROM (VALUES (40.5::DOUBLE), (NULL), (-3.0::DOUBLE)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Double.class), 42.0d);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Double.class), -1.5d);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_decimal() throws Exception {
        try (DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(38, 10)) {
            assertUnaryScalarFunction("java_add_decimal", decimalType, decimalType,
                                      (input, rowCount, out)
                                          -> {
                                          DuckDBReadableVector in = input.vector(0);
                                          BigDecimal increment = new BigDecimal("0.0000000001");
                                          for (int i = 0; i < rowCount; i++) {
                                              if (in.isNull(i)) {
                                                  out.setNull(i);
                                              } else {
                                                  out.setBigDecimal(i, in.getBigDecimal(i).add(increment));
                                              }
                                          }
                                      },
                                      "SELECT java_add_decimal(v) FROM (VALUES "
                                          + "(CAST('12345678901234567890.1234567890' AS DECIMAL(38,10))), "
                                          + "(NULL), "
                                          + "(CAST('-0.0000000001' AS DECIMAL(38,10)))) t(v)",
                                      rs -> {
                                          assertTrue(rs.next());
                                          assertEquals(rs.getObject(1, BigDecimal.class),
                                                       new BigDecimal("12345678901234567890.1234567891"));
                                          assertTrue(rs.next());
                                          assertNullRow(rs);
                                          assertTrue(rs.next());
                                          assertEquals(rs.getObject(1, BigDecimal.class), BigDecimal.ZERO.setScale(10));
                                          assertFalse(rs.next());
                                      });
        }
    }

    public static void test_register_scalar_function_decimal_precision_overflow() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(10, 2)) {
            conn.registerScalarFunction("java_decimal_precision_overflow", new DuckDBLogicalType[] {decimalType},
                                        decimalType, (input, rowCount, out) -> {
                                            for (int i = 0; i < rowCount; i++) {
                                                out.setBigDecimal(i, new BigDecimal("12345678901.23"));
                                            }
                                        });

            String err = assertThrows(() -> {
                stmt.execute("SELECT java_decimal_precision_overflow(CAST(1 AS DECIMAL(10,2)))");
            }, SQLException.class);
            assertTrue(err.contains("DECIMAL(10,2)"));
        }
    }

    public static void test_register_scalar_function_decimal_scale_overflow() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(10, 2)) {
            conn.registerScalarFunction("java_decimal_scale_overflow", new DuckDBLogicalType[] {decimalType},
                                        decimalType, (input, rowCount, out) -> {
                                            for (int i = 0; i < rowCount; i++) {
                                                out.setBigDecimal(i, new BigDecimal("1.234"));
                                            }
                                        });

            String err = assertThrows(() -> {
                stmt.execute("SELECT java_decimal_scale_overflow(CAST(1 AS DECIMAL(10,2)))");
            }, SQLException.class);
            assertTrue(err.contains("DECIMAL(10,2)"));
        }
    }

    public static void test_register_scalar_function_date() throws Exception {
        assertUnaryScalarFunction(
            "java_add_date", DuckDBColumnType.DATE, DuckDBColumnType.DATE,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setDate(i, in.getLocalDate(i).plusDays(2));
                    }
                }
            },
            "SELECT java_add_date(v) FROM (VALUES (DATE '2024-07-20'), (NULL), (DATE '1969-12-31')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDate.class), LocalDate.of(2024, 7, 22));
                assertEquals(rs.getDate(1), Date.valueOf(LocalDate.of(2024, 7, 22)));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDate.class), LocalDate.of(1970, 1, 2));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_date_from_java_util_date() throws Exception {
        assertUnaryScalarFunction("java_date_from_util_date", DuckDBColumnType.DATE, DuckDBColumnType.DATE,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              LocalDate value = in.getLocalDate(i).plusDays(1);
                                              out.setDate(i, java.util.Date.from(value.atStartOfDay(UTC).toInstant()));
                                          }
                                      }
                                  },
                                  "SELECT java_date_from_util_date(v) FROM (VALUES (DATE '2024-07-21'), (NULL)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, LocalDate.class), LocalDate.of(2024, 7, 22));
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamp() throws Exception {
        assertUnaryScalarFunction("java_add_timestamp", DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIMESTAMP,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setTimestamp(i, in.getLocalDateTime(i).plusMinutes(30));
                                          }
                                      }
                                  },
                                  "SELECT java_add_timestamp(v) FROM (VALUES "
                                      + "(TIMESTAMP '2024-07-21 12:34:56.123456'), "
                                      + "(NULL), "
                                      + "(TIMESTAMP '1969-12-31 23:45:00')) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      Timestamp ts1 = rs.getTimestamp(1);
                                      assertEquals(ts1, Timestamp.valueOf("2024-07-21 13:04:56.123456"));
                                      assertEquals(rs.getObject(1, LocalDateTime.class),
                                                   LocalDateTime.of(2024, 7, 21, 13, 4, 56, 123456000));
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getTimestamp(1), Timestamp.valueOf("1970-01-01 00:15:00"));
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamp_s() throws Exception {
        assertUnaryScalarFunction(
            "java_add_timestamp_s", DuckDBColumnType.TIMESTAMP_S, DuckDBColumnType.TIMESTAMP_S,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setTimestamp(i, in.getLocalDateTime(i).plusSeconds(2));
                    }
                }
            },
            "SELECT java_add_timestamp_s(v) FROM (VALUES (TIMESTAMP_S '2024-07-21 12:34:56'), (NULL)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getTimestamp(1), Timestamp.valueOf("2024-07-21 12:34:58"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_s_pre_epoch() throws Exception {
        assertUnaryScalarFunction("java_copy_timestamp_s_pre_epoch", DuckDBColumnType.TIMESTAMP,
                                  DuckDBColumnType.TIMESTAMP_S,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setTimestamp(i, in.getLocalDateTime(i));
                                          }
                                      }
                                  },
                                  "SELECT java_copy_timestamp_s_pre_epoch(v) FROM (VALUES "
                                      + "(TIMESTAMP '1969-12-31 23:59:59.999')) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getTimestamp(1), Timestamp.valueOf("1969-12-31 23:59:59"));
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamp_ms() throws Exception {
        assertUnaryScalarFunction("java_add_timestamp_ms", DuckDBColumnType.TIMESTAMP_MS, DuckDBColumnType.TIMESTAMP_MS,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setTimestamp(i, in.getLocalDateTime(i).plusNanos(7_000_000));
                                          }
                                      }
                                  },
                                  "SELECT java_add_timestamp_ms(v) FROM (VALUES "
                                      + "(TIMESTAMP_MS '2024-07-21 12:34:56.123'), (NULL)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, LocalDateTime.class),
                                                   LocalDateTime.of(2024, 7, 21, 12, 34, 56, 130_000_000));
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamp_ms_pre_epoch() throws Exception {
        assertUnaryScalarFunction("java_copy_timestamp_ms_pre_epoch", DuckDBColumnType.TIMESTAMP,
                                  DuckDBColumnType.TIMESTAMP_MS,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setTimestamp(i, in.getLocalDateTime(i));
                                          }
                                      }
                                  },
                                  "SELECT java_copy_timestamp_ms_pre_epoch(v) FROM (VALUES "
                                      + "(TIMESTAMP '1969-12-31 23:59:59.9995')) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, LocalDateTime.class),
                                                   LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_000_000));
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamp_ns() throws Exception {
        assertUnaryScalarFunction("java_add_timestamp_ns", DuckDBColumnType.TIMESTAMP_NS, DuckDBColumnType.TIMESTAMP_NS,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setTimestamp(i, in.getLocalDateTime(i).plusNanos(789));
                                          }
                                      }
                                  },
                                  "SELECT java_add_timestamp_ns(v) FROM (VALUES "
                                      + "(TIMESTAMP_NS '2024-07-21 12:34:56.123456789'), (NULL)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, LocalDateTime.class),
                                                   LocalDateTime.of(2024, 7, 21, 12, 34, 56, 123457578));
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_timestamptz() throws Exception {
        assertUnaryScalarFunction(
            "java_add_timestamptz", DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
            DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setOffsetDateTime(i, in.getOffsetDateTime(i).plusMinutes(5));
                    }
                }
            },
            "SELECT java_add_timestamptz(v) FROM (VALUES "
                + "(TIMESTAMPTZ '2024-07-21 12:34:56.123456+02:00'), (NULL)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertTrue(rs.getObject(1, OffsetDateTime.class)
                               .isEqual(OffsetDateTime.of(2024, 7, 21, 10, 39, 56, 123456000, ZoneOffset.UTC)));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamptz_set_timestamp() throws Exception {
        assertUnaryScalarFunction(
            "java_copy_timestamptz_with_timestamp", DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
            DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setTimestamp(i, in.getTimestamp(i));
                    }
                }
            },
            "SELECT java_copy_timestamptz_with_timestamp(v) FROM (VALUES "
                + "(TIMESTAMPTZ '2024-07-21 12:34:56.123456+02:00')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertTrue(rs.getObject(1, OffsetDateTime.class)
                               .isEqual(OffsetDateTime.of(2024, 7, 21, 10, 34, 56, 123456000, ZoneOffset.UTC)));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_from_java_util_date() throws Exception {
        assertUnaryScalarFunction(
            "java_timestamp_from_util_date", DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIMESTAMP,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                long oneSecondMillis = 1000L;
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setTimestamp(i, new java.util.Date(in.getTimestamp(i).getTime() + oneSecondMillis));
                    }
                }
            },
            "SELECT epoch_ms(java_timestamp_from_util_date(v)) FROM (VALUES "
                + "(TIMESTAMP '2024-07-21 12:34:56.123456'), "
                + "(NULL)) t(v)",
            rs -> {
                assertTrue(rs.next());
                Timestamp input = Timestamp.valueOf("2024-07-21 12:34:56.123456");
                assertEquals(rs.getLong(1), input.getTime() + 1000L);
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_from_java_util_date_typed_timestamp() throws Exception {
        assertUnaryScalarFunction(
            "java_timestamp_from_util_ts", DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIMESTAMP,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        java.util.Date value = Timestamp.valueOf(in.getLocalDateTime(i).plusNanos(789000));
                        out.setTimestamp(i, value);
                    }
                }
            },
            "SELECT java_timestamp_from_util_ts(v) FROM (VALUES (TIMESTAMP '2024-07-21 12:34:56.123456')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getTimestamp(1), Timestamp.valueOf("2024-07-21 12:34:56.124245"));
                assertFalse(rs.wasNull());
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_from_java_util_date_typed_sql_date() throws Exception {
        assertUnaryScalarFunction(
            "java_timestamp_from_util_sql_date", DuckDBColumnType.DATE, DuckDBColumnType.TIMESTAMP,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        java.util.Date value = Date.valueOf(in.getLocalDate(i));
                        out.setTimestamp(i, value);
                    }
                }
            },
            "SELECT epoch_ms(java_timestamp_from_util_sql_date(v)) FROM (VALUES (DATE '2024-07-21')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), Date.valueOf("2024-07-21").getTime());
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_from_java_util_date_typed_sql_time() throws Exception {
        assertUnaryScalarFunction(
            "java_timestamp_from_util_sql_time", DuckDBColumnType.TIMESTAMP, DuckDBColumnType.TIMESTAMP,
            (input, rowCount, out)
                -> {
                for (int i = 0; i < rowCount; i++) {
                    java.util.Date value = Time.valueOf("12:34:56");
                    out.setTimestamp(i, value);
                }
            },
            "SELECT epoch_ms(java_timestamp_from_util_sql_time(v)) FROM (VALUES (TIMESTAMP '2024-07-21 00:00:00')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), Time.valueOf("12:34:56").getTime());
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_timestamp_from_local_date() throws Exception {
        assertUnaryScalarFunction(
            "java_timestamp_from_local_date", DuckDBColumnType.DATE, DuckDBColumnType.TIMESTAMP,
            (input, rowCount, out)
                -> {
                DuckDBReadableVector in = input.vector(0);
                for (int i = 0; i < rowCount; i++) {
                    if (in.isNull(i)) {
                        out.setNull(i);
                    } else {
                        out.setTimestamp(i, in.getLocalDate(i).plusDays(1));
                    }
                }
            },
            "SELECT java_timestamp_from_local_date(v) FROM (VALUES (DATE '2024-07-21'), (NULL)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getTimestamp(1), Timestamp.valueOf("2024-07-22 00:00:00"));
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(2024, 7, 22, 0, 0));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_varchar() throws Exception {
        assertUnaryScalarFunction("java_suffix_varchar", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setString(i, in.getString(i) + "_java");
                                          }
                                      }
                                  },
                                  "SELECT java_suffix_varchar(v) FROM (VALUES ('duck'), (NULL), "
                                      + "('abcdefghijklmnop')) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, String.class), "duck_java");
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, String.class), "abcdefghijklmnop_java");
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_varchar_get_string_handles_null() throws Exception {
        assertUnaryScalarFunction("java_echo_varchar_nullable", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          out.setString(i, in.getString(i));
                                      }
                                  },
                                  "SELECT java_echo_varchar_nullable(v) FROM (VALUES ('duck'), (NULL), "
                                      + "('abcdefghijklmnop')) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, String.class), "duck");
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, String.class), "abcdefghijklmnop");
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_varchar_revalidates_after_null() throws Exception {
        assertUnaryScalarFunction("java_revalidate_varchar", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
                                  (input, rowCount, out)
                                      -> {
                                      DuckDBReadableVector in = input.vector(0);
                                      for (int i = 0; i < rowCount; i++) {
                                          if (in.isNull(i)) {
                                              out.setNull(i);
                                          } else {
                                              out.setNull(i);
                                              out.setString(i, in.getString(i) + "_ok");
                                          }
                                      }
                                  },
                                  "SELECT java_revalidate_varchar(v) FROM (VALUES ('duck'), (NULL)) t(v)",
                                  rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, String.class), "duck_ok");
                                      assertFalse(rs.wasNull());
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    private static void assertUnaryScalarFunction(String functionName, DuckDBColumnType parameterType,
                                                  DuckDBColumnType returnType, DuckDBVectorizedScalarFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBLogicalType parameterLogicalType = DuckDBLogicalType.of(parameterType);
             DuckDBLogicalType returnLogicalType = DuckDBLogicalType.of(returnType)) {
            assertUnaryScalarFunction(functionName, parameterLogicalType, returnLogicalType, function, query, verifier);
        }
    }

    private static void assertUnaryScalarFunction(String functionName, DuckDBLogicalType parameterType,
                                                  DuckDBLogicalType returnType, DuckDBVectorizedScalarFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            conn.registerScalarFunction(functionName, new DuckDBLogicalType[] {parameterType}, returnType, function);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertNullRow(ResultSet rs) throws Exception {
        assertEquals(rs.getObject(1), null);
        assertTrue(rs.wasNull());
    }

    private static final java.time.ZoneOffset UTC = java.time.ZoneOffset.UTC;
}

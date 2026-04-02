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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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

    public static void test_register_scalar_function_builder() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBScalarFunction function = DuckDBFunctions.scalarFunction()
                                                .withName("java_add_int_builder")
                                                .withParameter(intType)
                                                .withReturnType(intType)
                                                .withVectorFunction((input, out) -> {
                                                    int rowCount = input.rowCount();
                                                    DuckDBReadableVector in = input.vector(0);
                                                    for (int i = 0; i < rowCount; i++) {
                                                        if (in.isNull(i)) {
                                                            out.setNull(i);
                                                        } else {
                                                            out.setInt(i, in.getInt(i) + 1);
                                                        }
                                                    }
                                                })
                                                .register(conn);
            assertEquals(function.name(), "java_add_int_builder");
            assertEquals(function.parameterTypes().size(), 1);
            assertEquals(function.parameterTypes().get(0), intType);
            assertEquals(function.returnType(), intType);
            assertEquals(function.varArgType(), null);
            assertEquals(function.isVolatile(), false);
            assertEquals(function.hasSpecialHandling(), false);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_builder(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_builder_varargs_and_flags() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBScalarFunction function = DuckDBFunctions.scalarFunction()
                                                .withName("java_sum_varargs_builder")
                                                .withParameter(intType)
                                                .withVarArgs(intType)
                                                .withReturnType(intType)
                                                .withVolatile()
                                                .withSpecialHandling()
                                                .withVectorFunction((input, out) -> {
                                                    int rowCount = input.rowCount();
                                                    int columnCount = input.columnCount();
                                                    for (int row = 0; row < rowCount; row++) {
                                                        int sum = 0;
                                                        for (int col = 0; col < columnCount; col++) {
                                                            DuckDBReadableVector vector = input.vector(col);
                                                            if (!vector.isNull(row)) {
                                                                sum += vector.getInt(row);
                                                            }
                                                        }
                                                        out.setInt(row, sum);
                                                    }
                                                })
                                                .register(conn);
            assertEquals(function.varArgType(), intType);
            assertEquals(function.isVolatile(), true);
            assertEquals(function.hasSpecialHandling(), true);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_sum_varargs_builder(1, 2, 3), java_sum_varargs_builder(5)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 6);
                assertEquals(rs.getObject(2, Integer.class), 5);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_column_type_overloads() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBScalarFunction function = DuckDBFunctions.scalarFunction()
                                                .withName("java_add_int_builder_col_type")
                                                .withParameter(DuckDBColumnType.INTEGER)
                                                .withReturnType(DuckDBColumnType.INTEGER)
                                                .withVectorFunction((input, out) -> {
                                                    int rowCount = input.rowCount();
                                                    DuckDBReadableVector in = input.vector(0);
                                                    for (int i = 0; i < rowCount; i++) {
                                                        if (in.isNull(i)) {
                                                            out.setNull(i);
                                                        } else {
                                                            out.setInt(i, in.getInt(i) + 1);
                                                        }
                                                    }
                                                })
                                                .register(conn);
            assertEquals(function.parameterColumnTypes().size(), 1);
            assertEquals(function.parameterColumnTypes().get(0), DuckDBColumnType.INTEGER);
            assertEquals(function.parameterTypes().get(0), null);
            assertEquals(function.returnColumnType(), DuckDBColumnType.INTEGER);
            assertEquals(function.returnType(), null);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_int_builder_col_type(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_builder_java_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_function")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Function<Integer, Integer>) value -> value + 1)
                .register(conn);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_function(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_builder_java_bifunction() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_bifunction")
                .withParameter(Integer.class)
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((BiFunction<Integer, Integer, Integer>) Integer::sum)
                .register(conn);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_add_int_bifunction(a, b) FROM (VALUES (1, 2), (NULL, 2), (39, 3), (5, NULL)) t(a, b)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 3);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_java_function_class_cast_error() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_invalid_cast_function")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Function<String, Integer>) String::length)
                .register(conn);

            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_invalid_cast_function(1)"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("ClassCastException"));
        }
    }

    public static void test_register_scalar_function_builder_java_supplier() throws Exception {
        assertNullaryJavaFunction("java_constant_supplier", Integer.class,
                                  (Supplier<Integer>) () -> 42, "SELECT java_constant_supplier() FROM range(3)", rs -> {
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 42);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 42);
                                      assertTrue(rs.next());
                                      assertEquals(rs.getObject(1, Integer.class), 42);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_builder_java_supplier_null_value() throws Exception {
        assertNullaryJavaFunction("java_null_supplier", String.class,
                                  (Supplier<String>) () -> null, "SELECT java_null_supplier() FROM range(2)", rs -> {
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertTrue(rs.next());
                                      assertNullRow(rs);
                                      assertFalse(rs.next());
                                  });
    }

    public static void test_register_scalar_function_builder_java_supplier_class_cast_error() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_invalid_supplier_cast")
                .withReturnType(Integer.class)
                .withFunction((Supplier<String>) () -> "not_an_integer")
                .register(conn);

            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_invalid_supplier_cast()"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("ClassCastException"));
        }
    }

    public static void test_register_scalar_function_builder_java_supplier_rejects_parameters() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_supplier_arity").withParameter(Integer.class).withReturnType(Integer.class);
            String message =
                assertThrows(() -> { builder.withFunction((Supplier<Integer>) () -> 1); }, SQLException.class);
            assertTrue(message.contains("Supplier callback requires zero declared parameters"));
        }
    }

    public static void test_register_scalar_function_builder_java_supplier_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            builder.withName("java_invalid_supplier_varargs").withReturnType(Integer.class).withVarArgs(intType);
            String message =
                assertThrows(() -> { builder.withFunction((Supplier<Integer>) () -> 1); }, SQLException.class);
            assertTrue(message.contains("Supplier callback does not support varargs"));
        }
    }

    public static void test_register_scalar_function_builder_java_function_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            builder.withName("java_invalid_function_varargs")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withVarArgs(intType);
            String message = assertThrows(
                () -> { builder.withFunction((Function<Integer, Integer>) value -> value + 1); }, SQLException.class);
            assertTrue(message.contains("Function callback does not support varargs"));
            assertTrue(message.contains("withVarArgsFunction"));
        }
    }

    public static void test_register_scalar_function_builder_java_bifunction_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            builder.withName("java_invalid_bifunction_varargs")
                .withParameter(Integer.class)
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withVarArgs(intType);
            String message = assertThrows(() -> {
                builder.withFunction((BiFunction<Integer, Integer, Integer>) Integer::sum);
            }, SQLException.class);
            assertTrue(message.contains("BiFunction callback does not support varargs"));
            assertTrue(message.contains("withVarArgsFunction"));
        }
    }

    public static void test_register_scalar_function_builder_java_varargs_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_sum_varargs_function")
                .withParameter(Integer.class)
                .withVarArgs(intType)
                .withReturnType(Integer.class)
                .withVarArgsFunction(args -> {
                    int sum = 0;
                    for (Object arg : args) {
                        sum += (Integer) arg;
                    }
                    return sum;
                })
                .register(conn);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_sum_varargs_function(1, 2, 3), java_sum_varargs_function(5), "
                                       + "java_sum_varargs_function(NULL, 2), java_sum_varargs_function(2, NULL)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 6);
                assertEquals(rs.getObject(2, Integer.class), 5);
                assertEquals(rs.getObject(3), null);
                assertEquals(rs.getObject(4), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_java_varargs_function_requires_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_varargs_function")
                .withParameter(Integer.class)
                .withReturnType(Integer.class);
            String message = assertThrows(() -> { builder.withVarArgsFunction(args -> 0); }, SQLException.class);
            assertTrue(message.contains("requires withVarArgs"));
        }
    }

    public static void test_register_scalar_function_builder_java_function_supported_class_types() throws Exception {
        assertUnaryJavaFunction("java_not_bool_function", Boolean.class, Boolean.class,
                                (Function<Boolean, Boolean>) value
                                -> !value,
                                "SELECT java_not_bool_function(v) FROM (VALUES (TRUE), (NULL), (FALSE)) t(v)", rs -> {
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, Boolean.class), false);
                                    assertTrue(rs.next());
                                    assertNullRow(rs);
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, Boolean.class), true);
                                    assertFalse(rs.next());
                                });

        assertUnaryJavaFunction(
            "java_add_tinyint_function", Byte.class, Byte.class,
            (Function<Byte, Byte>) value
            -> (byte) (value + 1),
            "SELECT java_add_tinyint_function(v) FROM (VALUES (41::TINYINT), (NULL), (-2::TINYINT)) t(v)", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Byte.class), (byte) 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Byte.class), (byte) -1);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_bigint_function", Long.class, Long.class,
            (Function<Long, Long>) value
            -> value + 3,
            "SELECT java_add_bigint_function(v) FROM (VALUES (39::BIGINT), (NULL), (-5::BIGINT)) t(v)", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), -2L);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_double_function", Double.class, Double.class,
            (Function<Double, Double>) value
            -> value + 0.5,
            "SELECT java_add_double_function(v) FROM (VALUES (41.5::DOUBLE), (NULL), (-2.5::DOUBLE)) t(v)", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), -2.0);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction("java_suffix_varchar_function", String.class, String.class,
                                (Function<String, String>) value
                                -> value + "_ok",
                                "SELECT java_suffix_varchar_function(v) FROM (VALUES ('duck'), (NULL), ('db')) t(v)",
                                rs -> {
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, String.class), "duck_ok");
                                    assertTrue(rs.next());
                                    assertNullRow(rs);
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, String.class), "db_ok");
                                    assertFalse(rs.next());
                                });

        assertUnaryJavaFunction(
            "java_add_date_function", LocalDate.class, LocalDate.class,
            (Function<LocalDate, LocalDate>) value
            -> value.plusDays(2),
            "SELECT java_add_date_function(v) FROM (VALUES (DATE '2024-07-21'), (NULL), (DATE '2024-07-30')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDate.class), LocalDate.of(2024, 7, 23));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDate.class), LocalDate.of(2024, 8, 1));
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_timestamp_function", LocalDateTime.class, LocalDateTime.class,
            (Function<LocalDateTime, LocalDateTime>) value
            -> value.plusMinutes(30),
            "SELECT java_add_timestamp_function(v) FROM (VALUES "
                + "(TIMESTAMP '2024-07-21 12:34:56.123456'), "
                + "(NULL), "
                + "(TIMESTAMP '1969-12-31 23:45:00')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(2024, 7, 21, 13, 4, 56, 123456000));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(1970, 1, 1, 0, 15, 0));
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_timestamptz_function", OffsetDateTime.class, OffsetDateTime.class,
            (Function<OffsetDateTime, OffsetDateTime>) value
            -> value.plusMinutes(5),
            "SELECT java_add_timestamptz_function(v) FROM (VALUES "
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

    public static void test_register_scalar_function_builder_java_function_supported_unsigned_types() throws Exception {
        assertUnaryJavaFunction(
            "java_add_utinyint_function", DuckDBColumnType.UTINYINT, DuckDBColumnType.UTINYINT,
            (Function<Short, Short>) value
            -> (short) (value + 1),
            "SELECT java_add_utinyint_function(v) FROM (VALUES (41::UTINYINT), (NULL), (254::UTINYINT)) t(v)", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Short.class), (short) 255);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_usmallint_function", DuckDBColumnType.USMALLINT, DuckDBColumnType.USMALLINT,
            (Function<Integer, Integer>) value
            -> value + 2,
            "SELECT java_add_usmallint_function(v) FROM (VALUES (40::USMALLINT), (NULL), (65533::USMALLINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 65535);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction(
            "java_add_uinteger_function", DuckDBColumnType.UINTEGER, DuckDBColumnType.UINTEGER,
            (Function<Long, Long>) value
            -> value + 3,
            "SELECT java_add_uinteger_function(v) FROM (VALUES (39::UINTEGER), (NULL), (4294967292::UINTEGER)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 4294967295L);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction("java_add_ubigint_function", DuckDBColumnType.UBIGINT, DuckDBColumnType.UBIGINT,
                                (Function<BigInteger, BigInteger>) value
                                -> value.add(BigInteger.ONE),
                                "SELECT java_add_ubigint_function(v) FROM (VALUES (41::UBIGINT), (NULL), "
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

    public static void test_register_scalar_function_builder_java_function_decimal() throws Exception {
        try (DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(10, 2)) {
            assertUnaryJavaFunction("java_add_decimal_function", decimalType, decimalType,
                                    (Function<BigDecimal, BigDecimal>) value
                                    -> value.add(new BigDecimal("1.25")),
                                    "SELECT java_add_decimal_function(v) FROM (VALUES (CAST(40.75 AS DECIMAL(10,2))), "
                                        + "(NULL), (CAST(-1.25 AS DECIMAL(10,2)))) t(v)",
                                    rs -> {
                                        assertTrue(rs.next());
                                        assertEquals(rs.getObject(1, BigDecimal.class), new BigDecimal("42.00"));
                                        assertTrue(rs.next());
                                        assertNullRow(rs);
                                        assertTrue(rs.next());
                                        assertEquals(rs.getObject(1, BigDecimal.class), new BigDecimal("0.00"));
                                        assertFalse(rs.next());
                                    });
        }
    }

    public static void test_register_scalar_function_builder_java_function_sql_date_class_mapping() throws Exception {
        assertUnaryJavaFunction(
            "java_add_sql_date_function", java.sql.Date.class, java.sql.Date.class,
            (Function<java.sql.Date, java.sql.Date>) value
            -> java.sql.Date.valueOf(value.toLocalDate().plusDays(1)),
            "SELECT java_add_sql_date_function(v) FROM (VALUES (DATE '2024-07-21'), (NULL), (DATE '2024-07-30')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, java.sql.Date.class), java.sql.Date.valueOf("2024-07-22"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, java.sql.Date.class), java.sql.Date.valueOf("2024-07-31"));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_builder_java_function_sql_timestamp_class_mapping()
        throws Exception {
        assertUnaryJavaFunction("java_add_sql_timestamp_function", java.sql.Timestamp.class, java.sql.Timestamp.class,
                                (Function<java.sql.Timestamp, java.sql.Timestamp>) value
                                -> java.sql.Timestamp.valueOf(value.toLocalDateTime().plusSeconds(1)),
                                "SELECT java_add_sql_timestamp_function(v) FROM (VALUES "
                                    + "(TIMESTAMP '2024-07-21 12:34:56.123456'), (NULL)) t(v)",
                                rs -> {
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, java.sql.Timestamp.class),
                                                 java.sql.Timestamp.valueOf("2024-07-21 12:34:57.123456"));
                                    assertTrue(rs.next());
                                    assertNullRow(rs);
                                    assertFalse(rs.next());
                                });
    }

    public static void test_register_scalar_function_builder_java_function_java_util_date_class_mapping()
        throws Exception {
        assertUnaryJavaFunction("java_add_java_util_date_function", java.util.Date.class, java.util.Date.class,
                                (Function<java.util.Date, java.util.Date>) value
                                -> new java.util.Date(value.getTime() + 1000L),
                                "SELECT java_add_java_util_date_function(v) = date_trunc('millisecond', v) + "
                                    + "INTERVAL 1 SECOND FROM (VALUES "
                                    + "(TIMESTAMP '2024-07-21 12:34:56.123456'), (NULL)) t(v)",
                                rs -> {
                                    assertTrue(rs.next());
                                    assertEquals(rs.getObject(1, Boolean.class), true);
                                    assertTrue(rs.next());
                                    assertNullRow(rs);
                                    assertFalse(rs.next());
                                });
    }

    public static void test_register_scalar_function_builder_java_bifunction_supported_types() throws Exception {
        assertBinaryJavaFunction(
            "java_concat_varchar_bifunction", String.class, String.class, String.class,
            (BiFunction<String, String, String>) (left, right)
                -> left + "_" + right,
            "SELECT java_concat_varchar_bifunction(a, b) FROM (VALUES ('duck', 'db'), (NULL, 'x'), ('a', NULL)) t(a, b)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, String.class), "duck_db");
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });

        assertBinaryJavaFunction(
            "java_add_double_bifunction", Double.class, Double.class, Double.class,
            (BiFunction<Double, Double, Double>) Double::sum,
            "SELECT java_add_double_bifunction(a, b) FROM (VALUES (10.5::DOUBLE, 31.5::DOUBLE), (NULL, 2::DOUBLE), (2::DOUBLE, NULL)) t(a, b)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_typed_logical_type() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_typed")
                .withParameter(intType)
                .withReturnType(intType)
                .withVectorFunction((input, out) -> {
                    int rowCount = input.rowCount();
                    DuckDBReadableVector in = input.vector(0);
                    for (int i = 0; i < rowCount; i++) {
                        if (in.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setInt(i, in.getInt(i) + 1);
                        }
                    }
                })
                .register(conn);
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
            DuckDBFunctions.scalarFunction()
                .withName("java_add_one_bigint")
                .withParameter(bigintType)
                .withReturnType(bigintType)
                .withVectorFunction((input, out) -> {
                    int rowCount = input.rowCount();
                    DuckDBReadableVector in = input.vector(0);
                    for (int i = 0; i < rowCount; i++) {
                        out.setLong(i, in.getLong(i) + 1);
                    }
                })
                .register(conn);

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
            DuckDBFunctions.scalarFunction()
                .withName("java_throws_exception")
                .withParameter(intType)
                .withReturnType(intType)
                .withVectorFunction((input, out) -> { throw new IllegalStateException("boom"); })
                .register(conn);
            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_throws_exception(1)"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("IllegalStateException"));
            assertTrue(message.contains("boom"));
        }
    }

    public static void test_register_scalar_function_boolean() throws Exception {
        assertUnaryScalarFunction("java_not_bool", DuckDBColumnType.BOOLEAN, DuckDBColumnType.BOOLEAN,
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                      (input, out)
                                          -> {
                                          int rowCount = input.rowCount();
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
            DuckDBFunctions.scalarFunction()
                .withName("java_decimal_precision_overflow")
                .withParameter(decimalType)
                .withReturnType(decimalType)
                .withVectorFunction((input, out) -> {
                    int rowCount = input.rowCount();
                    for (int i = 0; i < rowCount; i++) {
                        out.setBigDecimal(i, new BigDecimal("12345678901.23"));
                    }
                })
                .register(conn);

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
            DuckDBFunctions.scalarFunction()
                .withName("java_decimal_scale_overflow")
                .withParameter(decimalType)
                .withReturnType(decimalType)
                .withVectorFunction((input, out) -> {
                    int rowCount = input.rowCount();
                    for (int i = 0; i < rowCount; i++) {
                        out.setBigDecimal(i, new BigDecimal("1.234"));
                    }
                })
                .register(conn);

            String err = assertThrows(() -> {
                stmt.execute("SELECT java_decimal_scale_overflow(CAST(1 AS DECIMAL(10,2)))");
            }, SQLException.class);
            assertTrue(err.contains("DECIMAL(10,2)"));
        }
    }

    public static void test_register_scalar_function_date() throws Exception {
        assertUnaryScalarFunction(
            "java_add_date", DuckDBColumnType.DATE, DuckDBColumnType.DATE,
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
            (input, out)
                -> {
                int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                  (input, out)
                                      -> {
                                      int rowCount = input.rowCount();
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
                                                  DuckDBColumnType returnType, DuckDBScalarVectorFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBLogicalType parameterLogicalType = DuckDBLogicalType.of(parameterType);
             DuckDBLogicalType returnLogicalType = DuckDBLogicalType.of(returnType)) {
            assertUnaryScalarFunction(functionName, parameterLogicalType, returnLogicalType, function, query, verifier);
        }
    }

    private static void assertUnaryScalarFunction(String functionName, DuckDBLogicalType parameterType,
                                                  DuckDBLogicalType returnType, DuckDBScalarVectorFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(parameterType)
                .withReturnType(returnType)
                .withVectorFunction(function)
                .register(conn);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertUnaryJavaFunction(String functionName, Class<?> parameterType, Class<?> returnType,
                                                Function<?, ?> function, String query, ResultSetVerifier verifier)
        throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(parameterType)
                .withReturnType(returnType)
                .withFunction(function)
                .register(conn);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertUnaryJavaFunction(String functionName, DuckDBColumnType parameterType,
                                                DuckDBColumnType returnType, Function<?, ?> function, String query,
                                                ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(parameterType)
                .withReturnType(returnType)
                .withFunction(function)
                .register(conn);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertUnaryJavaFunction(String functionName, DuckDBLogicalType parameterType,
                                                DuckDBLogicalType returnType, Function<?, ?> function, String query,
                                                ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(parameterType)
                .withReturnType(returnType)
                .withFunction(function)
                .register(conn);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertBinaryJavaFunction(String functionName, Class<?> leftType, Class<?> rightType,
                                                 Class<?> returnType, BiFunction<?, ?, ?> function, String query,
                                                 ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(leftType)
                .withParameter(rightType)
                .withReturnType(returnType)
                .withFunction(function)
                .register(conn);
            try (ResultSet rs = stmt.executeQuery(query)) {
                verifier.verify(rs);
            }
        }
    }

    private static void assertNullaryJavaFunction(String functionName, Class<?> returnType, Supplier<?> function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withReturnType(returnType)
                .withFunction(function)
                .register(conn);
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

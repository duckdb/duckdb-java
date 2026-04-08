package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.DUCKDB_TYPE_INTEGER;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestScalarFunctions {
    private interface ResultSetVerifier {
        void verify(ResultSet rs) throws Exception;
    }

    private static int sumNonNullIntColumns(DuckDBScalarContext ctx, DuckDBScalarRow row) {
        int sum = 0;
        for (int columnIndex = 0; columnIndex < ctx.columnCount(); columnIndex++) {
            if (!row.isNull(columnIndex)) {
                sum += row.getInt(columnIndex);
            }
        }
        return sum;
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
            DuckDBRegisteredFunction function =
                DuckDBFunctions.scalarFunction()
                    .withName("java_add_int_builder")
                    .withParameter(intType)
                    .withReturnType(intType)
                    .withVectorizedFunction(ctx -> {
                        ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setInt(row.getInt(0) + 1));
                    })
                    .register(conn);
            assertEquals(function.name(), "java_add_int_builder");
            assertEquals(function.parameterTypes().size(), 1);
            assertEquals(function.parameterTypes().get(0), intType);
            assertEquals(function.returnType(), intType);
            assertEquals(function.varArgType(), null);
            assertEquals(function.isVolatile(), false);
            assertEquals(function.hasSpecialHandling(), false);
            assertEquals(function.propagateNulls(), false);

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

    public static void test_register_scalar_function_builder_connection_without_unwrap() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_connection")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> null != x ? x + 1 : null)
                .register(conn);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_connection(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_builder_returns_detached_metadata() throws Exception {
        DuckDBRegisteredFunction function;
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            function = builder.withName("java_add_int_detached")
                           .withParameter(Integer.class)
                           .withReturnType(Integer.class)
                           .withFunction((Integer x) -> null != x ? x + 1 : null)
                           .register(conn);

            String message =
                assertThrows(() -> { builder.withName("java_add_int_detached_again"); }, SQLException.class);
            assertTrue(message.contains("already finalized"));

            assertEquals(function.name(), "java_add_int_detached");
            assertEquals(function.parameterColumnTypes().size(), 1);
            assertEquals(function.parameterColumnTypes().get(0), DuckDBColumnType.INTEGER);
            assertEquals(function.returnColumnType(), DuckDBColumnType.INTEGER);
            assertNotNull(function.function());
            assertEquals(function.functionKind(), DuckDBFunctions.DuckDBFunctionKind.SCALAR);
            assertTrue(function.isScalar());
            assertEquals(function.propagateNulls(), false);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_detached(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_registry_records_registered_functions() throws Exception {
        DuckDBDriver.clearFunctionsRegistry();
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_registry_recorded")
                                                    .withParameter(Integer.class)
                                                    .withReturnType(Integer.class)
                                                    .withFunction((Integer x) -> x + 1)
                                                    .register(conn);

            List<DuckDBRegisteredFunction> registeredFunctions = DuckDBDriver.registeredFunctions();
            assertEquals(registeredFunctions.size(), 1);
            assertEquals(registeredFunctions.get(0), function);
            assertEquals(registeredFunctions.get(0).functionKind(), DuckDBFunctions.DuckDBFunctionKind.SCALAR);
            assertTrue(registeredFunctions.get(0).isScalar());

            try (ResultSet rs = stmt.executeQuery("SELECT java_registry_recorded(41)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertFalse(rs.next());
            }
        } finally {
            DuckDBDriver.clearFunctionsRegistry();
        }
    }

    public static void test_register_scalar_function_registry_is_read_only() throws Exception {
        DuckDBDriver.clearFunctionsRegistry();
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_registry_read_only")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> x + 1)
                .register(conn);

            List<DuckDBRegisteredFunction> registeredFunctions = DuckDBDriver.registeredFunctions();
            assertThrows(() -> { registeredFunctions.add(null); }, UnsupportedOperationException.class);
        } finally {
            DuckDBDriver.clearFunctionsRegistry();
        }
    }

    public static void test_register_scalar_function_registry_clear_only_clears_java_registry() throws Exception {
        DuckDBDriver.clearFunctionsRegistry();
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_registry_clear_only")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> x + 1)
                .register(conn);

            assertEquals(DuckDBDriver.registeredFunctions().size(), 1);
            DuckDBDriver.clearFunctionsRegistry();
            assertEquals(DuckDBDriver.registeredFunctions().size(), 0);

            try (ResultSet rs = stmt.executeQuery("SELECT java_registry_clear_only(41)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertFalse(rs.next());
            }
        } finally {
            DuckDBDriver.clearFunctionsRegistry();
        }
    }

    public static void test_register_scalar_function_registry_allows_duplicate_names() throws Exception {
        DuckDBDriver.clearFunctionsRegistry();
        Path tempDir = Files.createTempDirectory("duckdb-registry");
        Path dbPathA = tempDir.resolve("registry-a.db");
        Path dbPathB = tempDir.resolve("registry-b.db");
        String urlA = "jdbc:duckdb:" + dbPathA.toAbsolutePath();
        String urlB = "jdbc:duckdb:" + dbPathB.toAbsolutePath();

        try (Connection connA = DriverManager.getConnection(urlA);
             Connection connB = DriverManager.getConnection(urlB)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_registry_duplicate_name")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> x + 1)
                .register(connA);
            DuckDBFunctions.scalarFunction()
                .withName("java_registry_duplicate_name")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> x + 2)
                .register(connB);

            List<DuckDBRegisteredFunction> registeredFunctions = DuckDBDriver.registeredFunctions();
            assertEquals(registeredFunctions.size(), 2);
            assertEquals(registeredFunctions.get(0).name(), "java_registry_duplicate_name");
            assertEquals(registeredFunctions.get(1).name(), "java_registry_duplicate_name");
        } finally {
            DuckDBDriver.clearFunctionsRegistry();
            Files.deleteIfExists(dbPathA);
            Files.deleteIfExists(dbPathB);
            Files.deleteIfExists(tempDir);
        }
    }

    public static void test_register_scalar_function_builder_rejects_non_duckdb_connection() throws Exception {
        Connection connection = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                switch (method.getName()) {
                case "unwrap":
                    throw new SQLException("not a DuckDB connection");
                case "isWrapperFor":
                    return false;
                case "toString":
                    return "invalid-connection";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(method.getName());
                }
            });

        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_connection")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer x) -> x + 1);

            String message = assertThrows(() -> { builder.register(connection); }, SQLException.class);
            assertTrue(message.contains("requires a DuckDB JDBC connection"));
        }
    }

    public static void test_register_scalar_function_builder_varargs_and_flags() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBRegisteredFunction function =
                DuckDBFunctions.scalarFunction()
                    .withName("java_sum_varargs_builder")
                    .withParameter(intType)
                    .withVarArgs(intType)
                    .withReturnType(intType)
                    .withVolatile()
                    .withSpecialHandling()
                    .withVectorizedFunction(
                        ctx -> { ctx.stream().forEachOrdered(row -> { row.setInt(sumNonNullIntColumns(ctx, row)); }); })
                    .register(conn);
            assertEquals(function.varArgType(), intType);
            assertEquals(function.isVolatile(), true);
            assertEquals(function.hasSpecialHandling(), true);
            assertEquals(function.propagateNulls(), false);

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
            DuckDBRegisteredFunction function =
                DuckDBFunctions.scalarFunction()
                    .withName("java_add_int_builder_col_type")
                    .withParameter(DuckDBColumnType.INTEGER)
                    .withReturnType(DuckDBColumnType.INTEGER)
                    .withVectorizedFunction(ctx -> {
                        ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setInt(row.getInt(0) + 1));
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

    public static void test_register_scalar_function_builder_with_parameters_class_helper() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_with_parameters")
                .withParameters(Integer.class, Integer.class)
                .withReturnType(Integer.class)
                .withFunction((Integer left, Integer right) -> left != null && right != null ? left + right : null)
                .register(conn);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_int_with_parameters(a, b) FROM (VALUES (1, 2), (NULL, 1), (20, 22)) t(a, b)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 3);
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
                .withFunction((Integer x) -> null != x ? x + 1 : null)
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

    public static void test_register_scalar_function_builder_java_function_propagate_nulls_false() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_int_function_nullable")
                                                    .withParameter(Integer.class)
                                                    .withReturnType(Integer.class)
                                                    .withFunction((Integer x) -> x == null ? 99 : x + 1)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), false);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_int_function_nullable(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 99);
                assertFalse(rs.wasNull());
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
                .withFunction((Integer x, Integer y) -> null != x && null != y ? x + y : null)
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

    public static void test_register_scalar_function_builder_java_bifunction_propagate_nulls_false() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function =
                DuckDBFunctions.scalarFunction()
                    .withName("java_add_int_bifunction_nullable")
                    .withParameter(Integer.class)
                    .withParameter(Integer.class)
                    .withReturnType(Integer.class)
                    .withFunction(
                        (Integer left, Integer right) -> (left == null ? 0 : left) + (right == null ? 0 : right))
                    .register(conn);
            assertEquals(function.propagateNulls(), false);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_add_int_bifunction_nullable(a, b) FROM (VALUES (1, 2), (NULL, 2), (39, NULL), (NULL, NULL)) t(a, b)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 3);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 39);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 0);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_with_int_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_int_with_int_function")
                                                    .withParameter(Integer.class)
                                                    .withReturnType(Integer.class)
                                                    .withIntFunction(x -> x + 1)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_int_with_int_function(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_builder_with_int_binary_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_int_with_int_binary_function")
                                                    .withParameter(Integer.class)
                                                    .withParameter(Integer.class)
                                                    .withReturnType(Integer.class)
                                                    .withIntFunction((left, right) -> left + right)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (ResultSet rs = stmt.executeQuery("SELECT java_add_int_with_int_binary_function(a, b) "
                                                  + "FROM (VALUES (1, 2), (NULL, 2), (39, 3), (5, NULL)) t(a, b)")) {
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

    public static void test_register_scalar_function_builder_with_double_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_double_with_double_function")
                                                    .withParameter(Double.class)
                                                    .withReturnType(Double.class)
                                                    .withDoubleFunction(x -> x + 0.5d)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_double_with_double_function(v) FROM (VALUES (41.5), (NULL), (-2.5)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0d);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), -2.0d);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_with_double_binary_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_double_with_double_binary_function")
                                                    .withParameter(Double.class)
                                                    .withParameter(Double.class)
                                                    .withReturnType(Double.class)
                                                    .withDoubleFunction((left, right) -> left + right)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_double_with_double_binary_function(a, b) FROM "
                                       + "(VALUES (1.0, 2.0), (NULL, 2.0), (39.5, 2.5), (5.0, NULL)) t(a, b)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 3.0d);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0d);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_with_long_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_long_with_long_function")
                                                    .withParameter(Long.class)
                                                    .withReturnType(Long.class)
                                                    .withLongFunction(x -> x + 3)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_add_long_with_long_function(v) FROM (VALUES (39::BIGINT), (NULL), (-5::BIGINT)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), -2L);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_builder_with_long_binary_function() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                                    .withName("java_add_long_with_long_binary_function")
                                                    .withParameter(Long.class)
                                                    .withParameter(Long.class)
                                                    .withReturnType(Long.class)
                                                    .withLongFunction((left, right) -> left + right)
                                                    .register(conn);
            assertEquals(function.propagateNulls(), true);

            try (ResultSet rs = stmt.executeQuery("SELECT java_add_long_with_long_binary_function(a, b) "
                                                  + "FROM (VALUES (1::BIGINT, 2::BIGINT), (NULL, 2::BIGINT), "
                                                  + "(39::BIGINT, 3::BIGINT), (5::BIGINT, NULL)) t(a, b)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 3L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
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
                .withFunction((String value) -> value.length())
                .register(conn);

            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_invalid_cast_function(1)"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("ClassCastException"));
        }
    }

    public static void test_register_scalar_function_builder_java_supplier() throws Exception {
        assertNullaryJavaFunction("java_constant_supplier", Integer.class,
                                  () -> 42, "SELECT java_constant_supplier() FROM range(3)", rs -> {
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
                                  () -> null, "SELECT java_null_supplier() FROM range(2)", rs -> {
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
                .withFunction(() -> "not_an_integer")
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
            String message = assertThrows(() -> { builder.withFunction(() -> 1); }, SQLException.class);
            assertTrue(message.contains("Supplier callback requires zero declared parameters"));
        }
    }

    public static void test_register_scalar_function_builder_java_supplier_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            builder.withName("java_invalid_supplier_varargs").withReturnType(Integer.class).withVarArgs(intType);
            String message = assertThrows(() -> { builder.withFunction(() -> 1); }, SQLException.class);
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
            String message = assertThrows(() -> { builder.withFunction((Integer x) -> x + 1); }, SQLException.class);
            assertTrue(message.contains("Function callback does not support varargs"));
            assertTrue(message.contains("withVarArgsFunction"));
        }
    }

    public static void test_register_scalar_function_builder_with_int_function_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            builder.withName("java_invalid_int_function_varargs")
                .withParameter(Integer.class)
                .withReturnType(Integer.class)
                .withVarArgs(intType);
            String message = assertThrows(() -> { builder.withIntFunction(x -> x + 1); }, SQLException.class);
            assertTrue(message.contains("withIntFunction does not support varargs"));
        }
    }

    public static void test_register_scalar_function_builder_with_int_function_rejects_wrong_types() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_int_function_type")
                .withParameter(Double.class)
                .withReturnType(Integer.class);
            String message = assertThrows(() -> { builder.withIntFunction(x -> x + 1); }, SQLException.class);
            assertTrue(message.contains("withIntFunction requires parameter 0 to be INTEGER"));
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
            String message =
                assertThrows(() -> { builder.withFunction((Integer x, Integer y) -> x + y); }, SQLException.class);
            assertTrue(message.contains("BiFunction callback does not support varargs"));
            assertTrue(message.contains("withVarArgsFunction"));
        }
    }

    public static void test_register_scalar_function_builder_with_double_function_rejects_wrong_types()
        throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_double_function_type")
                .withParameter(Integer.class)
                .withReturnType(Double.class);
            String message = assertThrows(() -> { builder.withDoubleFunction(x -> x + 0.5d); }, SQLException.class);
            assertTrue(message.contains("withDoubleFunction requires parameter 0 to be DOUBLE"));
        }
    }

    public static void test_register_scalar_function_builder_with_long_function_rejects_varargs() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction();
             DuckDBLogicalType bigintType = DuckDBLogicalType.of(DuckDBColumnType.BIGINT)) {
            builder.withName("java_invalid_long_function_varargs")
                .withParameter(Long.class)
                .withReturnType(Long.class)
                .withVarArgs(bigintType);
            String message = assertThrows(() -> { builder.withLongFunction(x -> x + 1); }, SQLException.class);
            assertTrue(message.contains("withLongFunction does not support varargs"));
        }
    }

    public static void test_register_scalar_function_builder_with_long_function_rejects_wrong_types() throws Exception {
        try (DuckDBScalarFunctionBuilder builder = DuckDBFunctions.scalarFunction()) {
            builder.withName("java_invalid_long_function_type").withParameter(Integer.class).withReturnType(Long.class);
            String message = assertThrows(() -> { builder.withLongFunction(x -> x + 1); }, SQLException.class);
            assertTrue(message.contains("withLongFunction requires parameter 0 to be BIGINT"));
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
        Function<Boolean, Boolean> notBoolean = value -> null != value ? !value : null;
        Function<Byte, Byte> addTinyInt = value -> null != value ? (byte) (value + 1) : null;
        Function<Long, Long> addBigInt = value -> null != value ? value + 3 : null;
        Function<Double, Double> addDouble = value -> null != value ? value + 0.5 : null;
        Function<String, String> suffixString = value -> null != value ? value + "_ok" : null;
        Function<LocalDate, LocalDate> addDate = value -> null != value ? value.plusDays(2) : null;
        Function<LocalDateTime, LocalDateTime> addTimestamp = value -> null != value ? value.plusMinutes(30) : null;
        Function<OffsetDateTime, OffsetDateTime> addTimestampTz = value -> null != value ? value.plusMinutes(5) : null;
        assertUnaryJavaFunction("java_not_bool_function", Boolean.class, Boolean.class, notBoolean,
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
            "java_add_tinyint_function", Byte.class, Byte.class, addTinyInt,
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
            "java_add_bigint_function", Long.class, Long.class, addBigInt,
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
            "java_add_double_function", Double.class, Double.class, addDouble,
            "SELECT java_add_double_function(v) FROM (VALUES (41.5::DOUBLE), (NULL), (-2.5::DOUBLE)) t(v)", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), -2.0);
                assertFalse(rs.next());
            });

        assertUnaryJavaFunction("java_suffix_varchar_function", String.class, String.class, suffixString,
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
            "java_add_date_function", LocalDate.class, LocalDate.class, addDate,
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
            "java_add_timestamp_function", LocalDateTime.class, LocalDateTime.class, addTimestamp,
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
            "java_add_timestamptz_function", OffsetDateTime.class, OffsetDateTime.class, addTimestampTz,
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
        Function<Short, Short> addUTinyInt = value -> null != value ? (short) (value + 1) : null;
        Function<Integer, Integer> addUSmallInt = value -> null != value ? value + 2 : null;
        Function<Long, Long> addUInteger = value -> null != value ? value + 3 : null;
        Function<BigInteger, BigInteger> addUBigInt = value -> null != value ? value.add(BigInteger.ONE) : null;
        assertUnaryJavaFunction(
            "java_add_utinyint_function", DuckDBColumnType.UTINYINT, DuckDBColumnType.UTINYINT, addUTinyInt,
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
            "java_add_usmallint_function", DuckDBColumnType.USMALLINT, DuckDBColumnType.USMALLINT, addUSmallInt,
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
            "java_add_uinteger_function", DuckDBColumnType.UINTEGER, DuckDBColumnType.UINTEGER, addUInteger,
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

        assertUnaryJavaFunction(
            "java_add_ubigint_function", DuckDBColumnType.UBIGINT, DuckDBColumnType.UBIGINT, addUBigInt,
            "SELECT java_add_ubigint_function(v) FROM (VALUES (41::UBIGINT), (NULL), "
                + "(18446744073709551614::UBIGINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("18446744073709551615"));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_builder_java_function_hugeint_class_mapping() throws Exception {
        Function<BigInteger, BigInteger> addHugeInt =
            value -> null != value ? value.add(BigInteger.ONE) : null;
        assertUnaryJavaFunction(
            "java_add_hugeint_function", BigInteger.class, BigInteger.class, addHugeInt,
            "SELECT java_add_hugeint_function(v) FROM (VALUES (CAST('41' AS HUGEINT)), (NULL), "
                + "(CAST('170141183460469231731687303715884105726' AS HUGEINT))) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class),
                             new BigInteger("170141183460469231731687303715884105727"));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_builder_java_function_decimal() throws Exception {
        try (DuckDBLogicalType decimalType = DuckDBLogicalType.decimal(10, 2)) {
            Function<BigDecimal, BigDecimal> addDecimal =
                value -> null != value ? value.add(new BigDecimal("1.25")) : null;
            assertUnaryJavaFunction("java_add_decimal_function", decimalType, decimalType, addDecimal,
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
        Function<java.sql.Date, java.sql.Date> addSqlDate =
            value -> null != value ? java.sql.Date.valueOf(value.toLocalDate().plusDays(1)) : null;
        assertUnaryJavaFunction(
            "java_add_sql_date_function", java.sql.Date.class, java.sql.Date.class, addSqlDate,
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
        Function<java.sql.Timestamp, java.sql.Timestamp> addSqlTimestamp =
            value -> null != value ? java.sql.Timestamp.valueOf(value.toLocalDateTime().plusSeconds(1)) : null;
        assertUnaryJavaFunction("java_add_sql_timestamp_function", java.sql.Timestamp.class, java.sql.Timestamp.class,
                                addSqlTimestamp,
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
        Function<java.util.Date, java.util.Date> addUtilDate =
            value -> null != value ? new java.util.Date(value.getTime() + 1000L) : null;
        assertUnaryJavaFunction("java_add_java_util_date_function", java.util.Date.class, java.util.Date.class,
                                addUtilDate,
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
        BiFunction<String, String, String> concatUnderscore =
            (left, right) -> left != null && right != null ? left + "_" + right : null;
        BiFunction<Double, Double, Double> sumDouble =
            (left, right) -> left != null && right != null ? Double.sum(left, right) : null;
        assertBinaryJavaFunction(
            "java_concat_varchar_bifunction", String.class, String.class, String.class, concatUnderscore,
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
            "java_add_double_bifunction", Double.class, Double.class, Double.class, sumDouble,
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
                .withVectorizedFunction(
                    ctx -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setInt(row.getInt(0) + 1)); })
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
                .withVectorizedFunction(ctx -> {
                    DuckDBWritableVector out = ctx.output();
                    DuckDBReadableVector in = ctx.input(0);
                    long rowCount = ctx.rowCount();
                    for (long row = 0; row < rowCount; row++) {
                        out.setLong(row, in.getLong(row) + 1);
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

    public static void test_register_scalar_function_context_row_stream_int() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_row_stream")
                .withParameter(intType)
                .withReturnType(intType)
                .withVectorizedFunction(
                    ctx -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setInt(row.getInt(0) + 1)); })
                .register(conn);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_row_stream(v) FROM (VALUES (1), (NULL), (41)) t(v)")) {
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

    public static void test_register_scalar_function_context_row_stream_double() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBLogicalType doubleType = DuckDBLogicalType.of(DuckDBColumnType.DOUBLE)) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_double_row_stream")
                .withParameter(doubleType)
                .withReturnType(doubleType)
                .withVectorizedFunction(ctx -> {
                    ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setDouble(row.getDouble(0) + 1.5d));
                })
                .register(conn);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_add_double_row_stream(v) FROM (VALUES (40.5::DOUBLE), (NULL), (-3.0::DOUBLE)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), 42.0d);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Double.class), -1.5d);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_primitive_nulls_handling() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_primitive_nulls_handling")
                .withParameter(DuckDBColumnType.BOOLEAN)
                .withParameter(DuckDBColumnType.TINYINT)
                .withParameter(DuckDBColumnType.UTINYINT)
                .withParameter(DuckDBColumnType.SMALLINT)
                .withParameter(DuckDBColumnType.USMALLINT)
                .withParameter(DuckDBColumnType.INTEGER)
                .withParameter(DuckDBColumnType.UINTEGER)
                .withParameter(DuckDBColumnType.BIGINT)
                .withParameter(DuckDBColumnType.FLOAT)
                .withParameter(DuckDBColumnType.DOUBLE)
                .withReturnType(DuckDBColumnType.VARCHAR)
                .withSpecialHandling()
                .withVectorizedFunction(ctx -> {
                    assertFalse(ctx.nullsPropagated());
                    ctx.stream().forEachOrdered(row -> {
                        try {
                            DuckDBReadableVector booleanVector = ctx.input(0);
                            DuckDBReadableVector intVector = ctx.input(5);
                            assertThrows(() -> { booleanVector.getBoolean(row.index()); }, DuckDBFunctionException.class);
                            assertTrue(booleanVector.getBoolean(row.index(), true));
                            assertThrows(() -> { intVector.getInt(row.index()); }, DuckDBFunctionException.class);
                            assertEquals(intVector.getInt(row.index(), 42), 42);

                            assertThrows(() -> { row.getBoolean(0); }, DuckDBFunctionException.class);
                            try {
                                row.getBoolean(0);
                                fail("Expected row.getBoolean(0) to fail on NULL");
                            } catch (DuckDBFunctionException exception) {
                                assertTrue(exception.getMessage().contains("Failed to read BOOLEAN"));
                                assertNotNull(exception.getCause());
                                assertTrue(exception.getCause() instanceof DuckDBFunctionException);
                                assertTrue(
                                    exception.getCause().getMessage().contains("Primitive value for BOOLEAN"));
                            }
                            assertTrue(row.getBoolean(0, true));
                            assertThrows(() -> { row.getByte(1); }, DuckDBFunctionException.class);
                            assertEquals(row.getByte(1, (byte) 42), (byte) 42);
                            assertThrows(() -> { row.getUint8(2); }, DuckDBFunctionException.class);
                            assertEquals(row.getUint8(2, (short) 42), (short) 42);
                            assertThrows(() -> { row.getShort(3); }, DuckDBFunctionException.class);
                            assertEquals(row.getShort(3, (short) 42), (short) 42);
                            assertThrows(() -> { row.getUint16(4); }, DuckDBFunctionException.class);
                            assertEquals(row.getUint16(4, 42), 42);
                            assertThrows(() -> { row.getInt(5); }, DuckDBFunctionException.class);
                            assertEquals(row.getInt(5, 42), 42);
                            assertThrows(() -> { row.getUint32(6); }, DuckDBFunctionException.class);
                            assertEquals(row.getUint32(6, (long) 42), (long) 42);
                            assertThrows(() -> { row.getLong(7); }, DuckDBFunctionException.class);
                            assertEquals(row.getLong(7, (long) 42), (long) 42);
                            assertThrows(() -> { row.getFloat(8); }, DuckDBFunctionException.class);
                            assertEquals(row.getFloat(8, (float) 42.1), (float) 42.1);
                            assertThrows(() -> { row.getDouble(9); }, DuckDBFunctionException.class);
                            assertEquals(row.getDouble(9, 42.1), 42.1);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        row.setString("ok");
                    });
                })
                .register(conn);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_primitive_nulls_handling(NULL::BOOLEAN, NULL::TINYINT, NULL::UTINYINT, NULL::SMALLINT,"
                    + " NULL::USMALLINT, NULL::INTEGER, NULL::UINTEGER, NULL::BIGINT, NULL::FLOAT, NULL::DOUBLE)")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "ok");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_context_row_stream_propagate_nulls_false() throws Exception {
        assertUnaryScalarFunction(
            "java_suffix_varchar_row_stream_nullable", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
            ctx
            -> {
                ctx.stream().forEachOrdered(row -> {
                    String value = row.getString(0);
                    row.setString(value == null ? "NULL_SEEN" : value + "_ok");
                });
            },
            "SELECT java_suffix_varchar_row_stream_nullable(v) FROM (VALUES ('duck'), (NULL), ('db')) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, String.class), "duck_ok");
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, String.class), "NULL_SEEN");
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, String.class), "db_ok");
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_integer_append() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_int_append")
                .withParameter(DuckDBColumnType.INTEGER)
                .withReturnType(DuckDBColumnType.INTEGER)
                .withIntFunction(x -> x + 1)
                .register(conn);

            try (ResultSet rs =
                     stmt.executeQuery("SELECT java_add_int_append(v) FROM (VALUES (41), (NULL), (-2)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), 42);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Integer.class), -1);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_bigint_append() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_bigint_append")
                .withParameter(DuckDBColumnType.BIGINT)
                .withReturnType(DuckDBColumnType.BIGINT)
                .withLongFunction(x -> x + 1)
                .register(conn);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT java_add_bigint_append(v) FROM (VALUES (41::BIGINT), (NULL), (-2::BIGINT)) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), 42L);
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, Long.class), -1L);
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
                .withVectorizedFunction(ctx -> { throw new IllegalStateException("boom"); })
                .register(conn);
            String message =
                assertThrows(() -> { stmt.executeQuery("SELECT java_throws_exception(1)"); }, SQLException.class);
            assertTrue(message.contains("Java scalar function threw exception"));
            assertTrue(message.contains("IllegalStateException"));
            assertTrue(message.contains("boom"));
        }
    }

    public static void test_register_scalar_function_boolean() throws Exception {
        assertUnaryScalarFunction(
            "java_not_bool", DuckDBColumnType.BOOLEAN, DuckDBColumnType.BOOLEAN,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setBoolean(!row.getBoolean(0))); },
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
        assertUnaryScalarFunction(
            "java_add_tinyint", DuckDBColumnType.TINYINT, DuckDBColumnType.TINYINT,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setByte((byte) (row.getByte(0) + 1))); },
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setShort((short) (row.getShort(0) + 2)));
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
        assertUnaryScalarFunction(
            "java_add_int", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setInt(row.getInt(0) + 1)); },
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(row -> {
                                          row.setNull();
                                          row.setInt(row.getInt(0) + 1);
                                      });
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
        assertUnaryScalarFunction(
            "java_add_bigint", DuckDBColumnType.BIGINT, DuckDBColumnType.BIGINT,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setLong(row.getLong(0) + 3)); },
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
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setUint8(row.getUint8(0) + 1)); },
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
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setUint16(row.getUint16(0) + 2)); },
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
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setUint32(row.getUint32(0) + 3)); },
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
        assertUnaryScalarFunction(
            "java_add_ubigint", DuckDBColumnType.UBIGINT, DuckDBColumnType.UBIGINT,
            ctx
            -> {
                BigInteger increment = BigInteger.ONE;
                ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setUint64(row.getUint64(0).add(increment)));
            },
            "SELECT java_add_ubigint(v) FROM (VALUES (41::UBIGINT), (NULL), "
                + "(18446744073709551614::UBIGINT)) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("18446744073709551615"));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_uhugeint() throws Exception {
        assertUnaryScalarFunction(
            "java_add_uhugeint", DuckDBColumnType.UHUGEINT, DuckDBColumnType.UHUGEINT,
            ctx
            -> {
                BigInteger increment = BigInteger.ONE;
                ctx.propagateNulls(true).stream().forEachOrdered(
                    row -> row.setUHugeInt(row.getUHugeInt(0).add(increment)));
            },
            "SELECT java_add_uhugeint(v) FROM (VALUES (CAST('41' AS UHUGEINT)), (NULL), "
                + "(CAST('340282366920938463463374607431768211454' AS UHUGEINT))) t(v)",
            rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class),
                             new BigInteger("340282366920938463463374607431768211455"));
                assertFalse(rs.next());
            });
    }

    public static void test_register_scalar_function_builder_java_function_uhugeint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName("java_add_uhugeint_function")
                .withParameter(DuckDBColumnType.UHUGEINT)
                .withReturnType(DuckDBColumnType.UHUGEINT)
                .withFunction(
                    (BigInteger value)
                    -> null != value ? value.add(BigInteger.ONE) : null)
                .register(conn);

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT java_add_uhugeint_function(v) FROM (VALUES (CAST('41' AS UHUGEINT)), (NULL), "
                    + "(CAST('340282366920938463463374607431768211454' AS UHUGEINT))) t(v)")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class), new BigInteger("42"));
                assertTrue(rs.next());
                assertNullRow(rs);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, BigInteger.class),
                             new BigInteger("340282366920938463463374607431768211455"));
                assertFalse(rs.next());
            }
        }
    }

    public static void test_register_scalar_function_float() throws Exception {
        assertUnaryScalarFunction(
            "java_add_float", DuckDBColumnType.FLOAT, DuckDBColumnType.FLOAT,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setFloat(row.getFloat(0) + 1.25f)); },
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
        assertUnaryScalarFunction(
            "java_add_double", DuckDBColumnType.DOUBLE, DuckDBColumnType.DOUBLE,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setDouble(row.getDouble(0) + 1.5d)); },
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
                                      ctx
                                      -> {
                                          BigDecimal increment = new BigDecimal("0.0000000001");
                                          ctx.propagateNulls(true).stream().forEachOrdered(
                                              row -> row.setBigDecimal(row.getBigDecimal(0).add(increment)));
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
                .withVectorizedFunction(ctx -> {
                    DuckDBWritableVector out = ctx.output();
                    long rowCount = ctx.rowCount();
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
                .withVectorizedFunction(ctx -> {
                    DuckDBWritableVector out = ctx.output();
                    long rowCount = ctx.rowCount();
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setDate(row.getLocalDate(0).plusDays(2)));
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(row -> {
                                          LocalDate value = row.getLocalDate(0).plusDays(1);
                                          row.setDate(java.util.Date.from(value.atStartOfDay(UTC).toInstant()));
                                      });
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(
                                          row -> row.setTimestamp(row.getLocalDateTime(0).plusMinutes(30)));
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(
                    row -> row.setTimestamp(row.getLocalDateTime(0).plusSeconds(2)));
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
                                  ctx
                                  -> { ctx.stream().forEachOrdered(row -> row.setTimestamp(row.getLocalDateTime(0))); },
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(
                                          row -> row.setTimestamp(row.getLocalDateTime(0).plusNanos(7_000_000)));
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
                                  ctx
                                  -> { ctx.stream().forEachOrdered(row -> row.setTimestamp(row.getLocalDateTime(0))); },
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(
                                          row -> row.setTimestamp(row.getLocalDateTime(0).plusNanos(789)));
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(
                    row -> row.setOffsetDateTime(row.getOffsetDateTime(0).plusMinutes(5)));
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
            ctx
            -> { ctx.stream().forEachOrdered(row -> row.setTimestamp(row.getTimestamp(0))); },
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
            ctx
            -> {
                long oneSecondMillis = 1000L;
                ctx.propagateNulls(true).stream().forEachOrdered(
                    row -> { row.setTimestamp(new java.util.Date(row.getTimestamp(0).getTime() + oneSecondMillis)); });
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(row -> {
                    java.util.Date value = Timestamp.valueOf(row.getLocalDateTime(0).plusNanos(789000));
                    row.setTimestamp(value);
                });
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
            ctx
            -> {
                ctx.stream().forEachOrdered(row -> {
                    java.util.Date value = Date.valueOf(row.getLocalDate(0));
                    row.setTimestamp(value);
                });
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
            ctx
            -> {
                ctx.stream().forEachOrdered(row -> {
                    java.util.Date value = Time.valueOf("12:34:56");
                    row.setTimestamp(value);
                });
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
            ctx
            -> {
                ctx.propagateNulls(true).stream().forEachOrdered(
                    row -> row.setTimestamp(row.getLocalDate(0).plusDays(1)));
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
        assertUnaryScalarFunction(
            "java_suffix_varchar", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setString(row.getString(0) + "_java")); },
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
        assertUnaryScalarFunction(
            "java_echo_varchar_nullable", DuckDBColumnType.VARCHAR, DuckDBColumnType.VARCHAR,
            ctx
            -> { ctx.propagateNulls(true).stream().forEachOrdered(row -> row.setString(row.getString(0))); },
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
                                  ctx
                                  -> {
                                      ctx.propagateNulls(true).stream().forEachOrdered(row -> {
                                          row.setNull();
                                          row.setString(row.getString(0) + "_ok");
                                      });
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
                                                  DuckDBColumnType returnType, DuckDBScalarFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBLogicalType parameterLogicalType = DuckDBLogicalType.of(parameterType);
             DuckDBLogicalType returnLogicalType = DuckDBLogicalType.of(returnType)) {
            assertUnaryScalarFunction(functionName, parameterLogicalType, returnLogicalType, function, query, verifier);
        }
    }

    private static void assertUnaryScalarFunction(String functionName, DuckDBLogicalType parameterType,
                                                  DuckDBLogicalType returnType, DuckDBScalarFunction function,
                                                  String query, ResultSetVerifier verifier) throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            DuckDBFunctions.scalarFunction()
                .withName(functionName)
                .withParameter(parameterType)
                .withReturnType(returnType)
                .withVectorizedFunction(function)
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

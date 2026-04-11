package org.duckdb;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.StringJoiner;

public final class DuckDBFunctions {
    public enum Kind { SCALAR, TABLE }

    private DuckDBFunctions() {
    }

    public static DuckDBScalarFunctionBuilder scalarFunction() throws SQLException {
        return new DuckDBScalarFunctionBuilder();
    }

    public static DuckDBTableFunctionBuilder tableFunction() throws SQLException {
        return new DuckDBTableFunctionBuilder();
    }

    public static class FunctionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public FunctionException(String message) {
            super(message);
        }

        public FunctionException(Throwable cause) {
            super(cause);
        }

        public FunctionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class RegisteredFunction {
        private final String name;
        private final Kind functionKind;
        private final LocalDateTime registeredAt;

        RegisteredFunction(String name, Kind functionKind) {
            this.name = name;
            this.functionKind = functionKind;
            this.registeredAt = LocalDateTime.now();
        }

        public String name() {
            return name;
        }

        public Kind functionKind() {
            return functionKind;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RegisteredFunction.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("functionKind=" + functionKind)
                .add("registeredAt=" + registeredAt)
                .toString();
        }
    }
}

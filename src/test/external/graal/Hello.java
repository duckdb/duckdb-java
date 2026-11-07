import java.sql.*;
import java.time.LocalDateTime;

public class Hello {
    public static void main(String[] a) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement();
                 ResultSet r = s.executeQuery("SELECT 42 AS answer")) {
                while (r.next()) {
                    System.out.println(r.getInt(1));
                }
            }
            checkTimestampNs();
        } catch (Throwable t) {
            // The default uncaught exception handler can fail to print in a native image,
            // masking the real cause, so report it explicitly here.
            for (Throwable e = t; e != null; e = e.getCause()) {
                System.out.println((e == t ? "ERROR: " : "Caused by: ") + e.getClass().getName());
                try {
                    System.out.println("  message: " + e.getMessage());
                    for (StackTraceElement el : e.getStackTrace()) {
                        System.out.println("  at " + el);
                    }
                } catch (Throwable ignored) {
                    System.out.println("  (could not print message/stack trace: " + ignored.getClass().getName() + ")");
                }
            }
            System.exit(1);
        }
    }

    private static void checkTimestampNs() throws Exception {
        LocalDateTime expected = LocalDateTime.of(2026, 2, 20, 12, 0, 0, 123456789);
        long expectedNanos = 1771588800123456789L;
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ns_bind (id INT, ts TIMESTAMP_NS)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO ns_bind VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setTimestamp(2, Timestamp.valueOf(expected));
                ps.execute();
                ps.setInt(1, 2);
                ps.setObject(2, expected);
                ps.execute();
            }
            try (ResultSet r = s.executeQuery("SELECT epoch_ns(ts), ts FROM ns_bind ORDER BY id")) {
                for (int i = 0; i < 2; i++) {
                    if (!r.next()) {
                        throw new IllegalStateException("TIMESTAMP_NS round trip returned no row " + i);
                    }
                    long actual = r.getLong(1);
                    if (actual != expectedNanos) {
                        throw new IllegalStateException("TIMESTAMP_NS epoch_ns mismatch on row " + i + ": expected " +
                                                        expectedNanos + ", actual " + actual);
                    }
                    LocalDateTime readBack = r.getObject(2, LocalDateTime.class);
                    if (!expected.equals(readBack)) {
                        throw new IllegalStateException("TIMESTAMP_NS read back mismatch on row " + i + ": expected " +
                                                        expected + ", actual " + readBack);
                    }
                }
                if (r.next()) {
                    throw new IllegalStateException("TIMESTAMP_NS round trip returned extra rows");
                }
            }
        }
    }
}

import java.sql.*;

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
}

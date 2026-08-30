import java.sql.*;

public class Hello {
    public static void main(String[] a) throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT 42 AS answer")) {
            while (r.next()) {
                System.out.println(r.getInt(1));
            }
        }
    }
}

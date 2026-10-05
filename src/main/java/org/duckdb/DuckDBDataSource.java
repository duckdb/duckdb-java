package org.duckdb;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DuckDBDataSource implements DataSource {
    private final String url;
    private final Properties props;
    private PrintWriter logWriter = null;

    public DuckDBDataSource() {
        this(DuckDBDriver.DUCKDB_URL_PREFIX, null);
    }

    public DuckDBDataSource(String url) {
        this(url, null);
    }

    public DuckDBDataSource(String url, Properties info) {
        this.url = url;
        props = info;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getDriver(url).connect(url, props);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        logWriter = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("no logger");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLException(e);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}

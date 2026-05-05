package org.duckdb;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Indirection over {@link DuckDBMemoryMonitor} that is safe to reference on JVMs without JFR
 * support (e.g. Java 8). When {@code jdk.jfr.FlightRecorder} is not available at runtime, every
 * method on this class is a silent no-op.
 *
 * <p>Only this class may reference {@code DuckDBMemoryMonitor} or {@code DuckDBMemoryEvent}; any
 * direct reference from the other main classes would trigger class resolution at load time and
 * fail verification on a non-JFR JVM.
 */
final class JfrMemoryMonitor {

    private static final Logger logger = Logger.getLogger(JfrMemoryMonitor.class.getName());

    private static final Method INIT;
    private static final Method CONNECTION_OPENED;
    private static final Method CONNECTION_CLOSED;

    static {
        Method init = null;
        Method opened = null;
        Method closed = null;
        try {
            // Probe for JFR first: on Java 8 this throws ClassNotFoundException
            // and we never touch DuckDBMemoryMonitor (which imports jdk.jfr.*).
            Class.forName("jdk.jfr.FlightRecorder");
            Class<?> impl = Class.forName("org.duckdb.DuckDBMemoryMonitor");
            init = impl.getDeclaredMethod("init");
            opened = impl.getDeclaredMethod("connectionOpened", DuckDBConnection.class);
            closed = impl.getDeclaredMethod("connectionClosed", long.class);
            init.setAccessible(true);
            opened.setAccessible(true);
            closed.setAccessible(true);
        } catch (Throwable t) {
            // JFR unavailable; every method becomes a silent no-op.
            logger.log(Level.FINE, "JFR memory monitor is not available on this JVM", t);
        }
        INIT = init;
        CONNECTION_OPENED = opened;
        CONNECTION_CLOSED = closed;
    }

    private JfrMemoryMonitor() {
    }

    static void init() {
        if (INIT == null) {
            return;
        }
        try {
            INIT.invoke(null);
        } catch (Throwable t) {
            logger.log(Level.FINE, "JFR memory monitor init failed", t);
        }
    }

    static void connectionOpened(DuckDBConnection conn) {
        if (CONNECTION_OPENED == null) {
            return;
        }
        try {
            CONNECTION_OPENED.invoke(null, conn);
        } catch (Throwable t) {
            logger.log(Level.FINE, "JFR connectionOpened failed", t);
        }
    }

    static void connectionClosed(long dbAddress) {
        if (CONNECTION_CLOSED == null) {
            return;
        }
        try {
            CONNECTION_CLOSED.invoke(null, dbAddress);
        } catch (Throwable t) {
            logger.log(Level.FINE, "JFR connectionClosed failed", t);
        }
    }
}

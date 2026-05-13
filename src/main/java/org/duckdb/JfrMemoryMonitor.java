package org.duckdb;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Indirection over {@link DuckDBMemoryMonitor} that is safe to reference on JVMs without JFR
 * support (e.g. stripped Java 8 builds). When {@code jdk.jfr.FlightRecorder} is not available
 * at runtime, every method on this class is a silent no-op and {@code DuckDBMemoryMonitor} —
 * which imports {@code jdk.jfr.*} — is never resolved.
 *
 * <p>This relies on the JVM's lazy class resolution: an {@code invokestatic} against
 * {@link DuckDBMemoryMonitor} only triggers resolution of that class when the instruction
 * actually executes. The {@link #AVAILABLE} guard therefore prevents the JFR-dependent class
 * from ever being loaded on non-JFR JVMs.
 *
 * <p>Only this class may reference {@code DuckDBMemoryMonitor} or {@code DuckDBMemoryEvent};
 * any direct reference from the other main classes would risk eager resolution on class load.
 */
final class JfrMemoryMonitor {

    private static final Logger logger = Logger.getLogger(JfrMemoryMonitor.class.getName());

    private static final boolean AVAILABLE;

    static {
        boolean available;
        try {
            Class.forName("jdk.jfr.FlightRecorder");
            available = true;
        } catch (Throwable t) {
            available = false;
            logger.log(Level.FINE, "JFR memory monitor is not available on this JVM", t);
        }
        AVAILABLE = available;
    }

    private JfrMemoryMonitor() {
    }

    static void init() {
        if (AVAILABLE) {
            DuckDBMemoryMonitor.init();
        }
    }

    static void connectionOpened(DuckDBConnection conn) {
        if (AVAILABLE) {
            DuckDBMemoryMonitor.connectionOpened(conn);
        }
    }

    static void connectionClosed(long dbAddress) {
        if (AVAILABLE) {
            DuckDBMemoryMonitor.connectionClosed(dbAddress);
        }
    }
}

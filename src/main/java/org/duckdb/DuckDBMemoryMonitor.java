package org.duckdb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.jfr.FlightRecorder;

/**
 * Manages per-database JFR memory monitors.
 *
 * <h2>Activation</h2>
 * <p>Monitoring is opt-in per connection. The caller supplies a user-assigned identifier
 * via the {@value DuckDBDriver#JDBC_JFR_MEMORY_MONITOR} connection property; that value
 * becomes the {@code component} field of every {@link DuckDBMemoryEvent} emitted for the
 * connection's DuckDB instance. A monitor is created for a DuckDB instance when the first
 * opted-in connection to it is opened and destroyed when the last such connection is closed.
 *
 * <h2>Event scheduling</h2>
 * <p>This class does <em>not</em> own a scheduler. It registers a single
 * {@linkplain FlightRecorder#addPeriodicEvent periodic JFR hook} for
 * {@link DuckDBMemoryEvent}; JFR invokes the hook at the period configured on
 * the recording (e.g. {@code <setting name="period">1 s</setting>}) and only
 * while at least one active recording has the event enabled. Consequently, the
 * JDBC property is a pure enable/label switch and JFR alone governs the
 * sampling rate and the enabled state of the event.
 *
 * <h2>Attribution model</h2>
 * <p>The monitor registry is keyed on the native DuckDB instance address so that multiple
 * connections to the same underlying database share a single sample stream — avoiding
 * double-counting of shared memory. The user-supplied component identifier is captured from
 * the first opted-in connection and emitted on every event for that monitor. When attributing
 * memory to distinct application components, use a distinct DuckDB instance per component and
 * give each one a unique {@value DuckDBDriver#JDBC_JFR_MEMORY_MONITOR} value.
 *
 * <h2>Thread safety</h2>
 * <p>Lifecycle transitions (start+insert, stop+remove) are performed inside
 * {@link ConcurrentHashMap#compute}, which provides per-key mutual exclusion.
 * The JFR periodic hook iterates {@link ConcurrentHashMap#values} without
 * locking and relies on volatile reads in {@link PerDbMonitor} for visibility.
 */
final class DuckDBMemoryMonitor {

    private static final Logger logger = Logger.getLogger(DuckDBMemoryMonitor.class.getName());

    /** Registry: native DuckDB* address -> per-database monitor. */
    private static final ConcurrentHashMap<Long, PerDbMonitor> monitors = new ConcurrentHashMap<>();

    private static boolean initialized;

    // Non-instantiable
    private DuckDBMemoryMonitor() {
    }

    /**
     * Registers the periodic JFR hook for {@link DuckDBMemoryEvent}. Idempotent
     * and called from {@link DuckDBDriver}'s static initializer so that recordings
     * started before the first monitored connection still see the event type.
     * Iterating an empty monitor map at each tick is cheap, so there is no
     * downside to registering unconditionally.
     */
    static synchronized void init() {
        if (initialized) {
            return;
        }
        FlightRecorder.addPeriodicEvent(DuckDBMemoryEvent.class, DuckDBMemoryMonitor::firePeriodicEvent);
        initialized = true;
    }

    /**
     * Called when a new connection is opened with JFR memory monitoring enabled.
     * The caller guarantees {@code conn.monitorName} is non-null.
     */
    static void connectionOpened(DuckDBConnection conn) {
        monitors.compute(conn.dbAddress, (k, existing) -> {
            PerDbMonitor m = (existing != null) ? existing : new PerDbMonitor();
            m.open(conn);
            return m;
        });
    }

    /**
     * Called when a monitored connection is closed.
     *
     * @param dbAddress the native db address captured at construction time
     */
    static void connectionClosed(long dbAddress) {
        monitors.compute(dbAddress, (k, existing) -> {
            if (existing == null) {
                return null;
            }
            return existing.close() ? null : existing;
        });
    }

    /**
     * JFR-invoked hook. Runs on a JFR thread at the period configured on the
     * active recording. Must never throw.
     */
    private static void firePeriodicEvent() {
        for (PerDbMonitor m : monitors.values()) {
            try {
                m.sample();
            } catch (Throwable t) {
                // Defensive: a failure in one monitor must not prevent emission for others.
            }
        }
    }

    /**
     * Per-name monitor state. Mutating methods ({@link #open}, {@link #close})
     * are invoked inside {@link ConcurrentHashMap#compute} on {@link #monitors}
     * and are serialized per key. {@link #sample} runs on the JFR hook thread
     * without the compute lock and reads {@code volatile} fields.
     */
    static final class PerDbMonitor {

        private static final String QUERY =
            "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()";

        /** Guarded by compute()-serialization. */
        private int openConnections = 0;

        /**
         * Written under compute(); read without lock by the JFR hook.
         *
         * <p>The prepared statement is parsed + planned once and executed on every tick,
         * avoiding the per-tick parse overhead of a fresh {@code createStatement}. It is
         * only touched from the JFR hook thread (JFR serialises periodic callbacks), so
         * no additional synchronisation is required for re-execution.
         */
        private volatile DuckDBConnection monitorConn;
        private volatile PreparedStatement sampleStmt;
        private volatile String component;
        private volatile long dbAddress;

        /**
         * Opens (or re-attempts opening) the monitor connection and increments
         * the ref count. Failure to create the monitor connection or prepare the
         * sampling statement is logged and the ref count is still incremented so
         * close-balance is preserved; a subsequent {@code open()} will retry
         * while {@code monitorConn == null}.
         */
        void open(DuckDBConnection conn) {
            if (monitorConn == null) {
                DuckDBConnection mc = null;
                try {
                    mc = conn.duplicateForMonitor();
                    PreparedStatement ps = mc.prepareStatement(QUERY);
                    component = conn.monitorName;
                    dbAddress = conn.dbAddress;
                    sampleStmt = ps;
                    // Publish monitorConn last so readers see fully-populated state.
                    monitorConn = mc;
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to open JFR memory-monitor connection; will retry on next open()",
                               e);
                    if (mc != null) {
                        try {
                            mc.close();
                        } catch (SQLException ce) {
                            // best-effort cleanup on setup failure
                        }
                    }
                }
            }
            openConnections++;
        }

        /**
         * Decrements the ref count; when it reaches zero, releases the cached
         * statement and monitor connection, and signals the caller to remove
         * the map entry.
         *
         * @return {@code true} when the entry should be removed
         */
        boolean close() {
            if (--openConnections <= 0) {
                PreparedStatement ps = sampleStmt;
                DuckDBConnection mc = monitorConn;
                sampleStmt = null;
                monitorConn = null;
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        logger.log(Level.FINE, "Failed to close JFR memory-monitor statement", e);
                    }
                }
                if (mc != null) {
                    try {
                        mc.close();
                    } catch (SQLException e) {
                        logger.log(Level.FINE, "Failed to close JFR memory-monitor connection", e);
                    }
                }
                return true;
            }
            return false;
        }

        /** Invoked by the JFR periodic hook. Must not throw. */
        void sample() {
            DuckDBConnection mc = monitorConn;
            PreparedStatement ps = sampleStmt;
            if (mc == null || ps == null) {
                return;
            }
            try {
                if (mc.isClosed()) {
                    return;
                }
            } catch (SQLException ignored) {
                return;
            }
            String componentSnap = component;
            long addr = dbAddress;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tag = rs.getString(1);
                    long memoryUsageBytes = rs.getLong(2);
                    long temporaryStorageBytes = rs.getLong(3);
                    emitEvent(componentSnap, addr, tag, memoryUsageBytes, temporaryStorageBytes);
                }
            } catch (Exception ignored) {
                // Propagating would break JFR's periodic dispatch; swallow.
            }
        }

        private static void emitEvent(String component, long addr, String tag, long memoryUsageBytes,
                                      long temporaryStorageBytes) {
            DuckDBMemoryEvent event = new DuckDBMemoryEvent();
            event.begin();
            event.component = component;
            event.tag = tag;
            event.dbAddress = addr;
            event.memoryUsageBytes = memoryUsageBytes;
            event.temporaryStorageBytes = temporaryStorageBytes;
            event.commit();
        }
    }
}

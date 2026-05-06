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
     *
     * <p>Any failure from JFR (e.g. {@link SecurityException} under a
     * {@code SecurityManager}, or an unexpected {@link Error} from a non-standard
     * JFR implementation) is caught and logged: the feature must never prevent
     * {@link DuckDBDriver} from loading.
     */
    static synchronized void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        try {
            FlightRecorder.addPeriodicEvent(DuckDBMemoryEvent.class, DuckDBMemoryMonitor::firePeriodicEvent);
        } catch (Throwable t) {
            logger.log(Level.WARNING, "JFR periodic event registration failed; memory monitoring disabled", t);
        }
    }

    /**
     * Called when a new connection is opened with JFR memory monitoring enabled.
     * The caller guarantees {@code conn.monitorName} is non-null and that
     * {@link #init()} has already been called (which is the contract of
     * {@link DuckDBConnection#newConnection}).
     *
     * <p>A zero {@code dbAddress} is treated as a "no such instance" sentinel and
     * skipped: the native layer is expected to return a non-zero pointer for every
     * live DuckDB instance, so a zero value indicates a bug or an unexpected state.
     * Registering under key {@code 0} would silently alias every such connection
     * into a single monitor entry.
     */
    static void connectionOpened(DuckDBConnection conn) {
        if (conn.dbAddress == 0L) {
            logger.log(Level.FINE, "Skipping JFR memory monitor registration: native DuckDB address is 0");
            return;
        }
        monitors.compute(conn.dbAddress, (k, existing) -> {
            PerDbMonitor m = (existing != null) ? existing : new PerDbMonitor();
            m.open(conn);
            return m;
        });
    }

    /**
     * Called when a monitored connection is closed. Symmetric with
     * {@link #connectionOpened(DuckDBConnection)}: a zero {@code dbAddress} means
     * no entry was ever registered, so there is nothing to tear down.
     *
     * @param dbAddress the native db address captured at construction time
     */
    static void connectionClosed(long dbAddress) {
        if (dbAddress == 0L) {
            return;
        }
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
                logger.log(Level.FINE, "JFR memory sample failed", t);
            }
        }
    }

    /**
     * Per-database monitor state.
     *
     * <p>Two threads may touch an instance: the user thread closing a monitored
     * connection (via {@link #close()}, invoked inside {@link ConcurrentHashMap#compute})
     * and the JFR periodic thread sampling the database (via {@link #sample()}).
     * They are mutually exclusive under the instance's intrinsic lock so that
     * {@link #close()} can never free a {@link PreparedStatement} that
     * {@link #sample()} is executing against.
     *
     * <p>{@link #open(DuckDBConnection)} is only invoked inside {@code compute}, which
     * already serialises it against itself and {@link #close()} per key, but it still
     * synchronises on {@code this} to establish a happens-before with any concurrent
     * {@link #sample()}.
     */
    static final class PerDbMonitor {

        private static final String QUERY =
            "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()";

        // All fields below are guarded by the instance's intrinsic lock.
        private int openConnections = 0;
        private DuckDBConnection monitorConn;
        private PreparedStatement sampleStmt;
        private String component;
        private long dbAddress;

        /**
         * Opens (or re-attempts opening) the monitor connection and increments
         * the ref count. Failure to create the monitor connection or prepare the
         * sampling statement is logged and the ref count is still incremented so
         * close-balance is preserved; a subsequent {@code open()} will retry
         * while {@code monitorConn == null}.
         */
        synchronized void open(DuckDBConnection conn) {
            if (monitorConn == null) {
                DuckDBConnection mc = null;
                try {
                    mc = conn.duplicateForMonitor();
                    sampleStmt = mc.prepareStatement(QUERY);
                    component = conn.monitorName;
                    dbAddress = conn.dbAddress;
                    monitorConn = mc;
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to open JFR memory-monitor connection; will retry on next open()",
                               e);
                    sampleStmt = null;
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
         * the map entry. Blocks any in-flight {@link #sample()} until it
         * completes, ensuring the statement is never closed mid-execution.
         *
         * @return {@code true} when the entry should be removed
         */
        synchronized boolean close() {
            if (--openConnections > 0) {
                return false;
            }
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

        /**
         * Invoked by the JFR periodic hook. Must not throw. Holds the monitor's
         * intrinsic lock for the duration so that {@link #close()} cannot release
         * the cached statement mid-execution. The query against {@code duckdb_memory()}
         * is a quick system-table scan; any contention with a concurrent connection
         * close is bounded by its runtime.
         */
        synchronized void sample() {
            PreparedStatement ps = sampleStmt;
            if (ps == null) {
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
            } catch (Throwable t) {
                // Propagating would break JFR's periodic dispatch; log at FINE so
                // operators can diagnose silent emission gaps without flooding logs.
                logger.log(Level.FINE, "JFR memory sample query failed", t);
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

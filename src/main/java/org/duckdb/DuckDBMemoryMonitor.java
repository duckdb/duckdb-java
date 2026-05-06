package org.duckdb;

import java.nio.ByteBuffer;
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
 * <h2>Sampling model</h2>
 * <p>The monitor does <em>not</em> hold a JDBC connection. It pins the underlying
 * DuckDB instance via {@link DuckDBNative#duckdb_jdbc_create_db_ref} and, on each JFR tick,
 * calls {@link DuckDBNative#duckdb_jdbc_memory_snapshot} to read the per-tag memory counters
 * straight from the native {@code BufferManager}. This avoids running SQL, allocating result
 * sets, and taking any connection-level lock that might interfere with user traffic.
 *
 * <h2>Event scheduling</h2>
 * <p>This class registers a single {@linkplain FlightRecorder#addPeriodicEvent periodic JFR hook}
 * for {@link DuckDBMemoryEvent}; JFR invokes it at the period configured on the recording
 * and only while at least one active recording has the event enabled.
 *
 * <h2>Attribution model</h2>
 * <p>The monitor registry is keyed on the native DuckDB instance address so that multiple
 * connections to the same underlying database share a single sample stream — avoiding
 * double-counting of shared memory. The user-supplied component identifier is captured from
 * the first opted-in connection and emitted on every event for that monitor.
 *
 * <h2>Thread safety</h2>
 * <p>Lifecycle transitions (start+insert, stop+remove) are performed inside
 * {@link ConcurrentHashMap#compute}, which provides per-key mutual exclusion.
 * The JFR periodic hook iterates {@link ConcurrentHashMap#values} without locking and
 * relies on a single volatile read of {@code dbRef} for visibility.
 */
final class DuckDBMemoryMonitor {

    private static final Logger logger = Logger.getLogger(DuckDBMemoryMonitor.class.getName());

    /** Memory-tag names in enum order; fetched once from the native side and cached. */
    private static final String[] MEMORY_TAGS = loadMemoryTags();

    private static String[] loadMemoryTags() {
        try {
            return DuckDBNative.duckdb_jdbc_memory_tags();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Registry: native DuckDB* address -> per-database monitor. */
    private static final ConcurrentHashMap<Long, PerDbMonitor> monitors = new ConcurrentHashMap<>();

    private static boolean initialized;

    // Non-instantiable
    private DuckDBMemoryMonitor() {
    }

    /**
     * Registers the periodic JFR hook for {@link DuckDBMemoryEvent}. Idempotent and called
     * from {@link DuckDBDriver}'s static initializer so that recordings started before any
     * monitored connection is opened still see the event type.
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
     * JFR-invoked hook. Runs on a JFR thread at the period configured on the active recording.
     * JFR invokes it only when the event is enabled in some recording; the per-event
     * {@link jdk.jfr.Event#commit()} performs the final {@code shouldCommit()} check.
     * Must never throw.
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
     * Per-instance monitor state. Mutating methods ({@link #open}, {@link #close}) are
     * invoked inside {@link ConcurrentHashMap#compute} on {@link #monitors} and are
     * serialized per key. {@link #sample} runs on the JFR hook thread without the compute
     * lock and reads the {@code volatile dbRef} first to establish happens-before for the
     * other fields.
     */
    static final class PerDbMonitor {

        /** Guarded by compute()-serialization. */
        private int openConnections = 0;

        /**
         * Pinned DuckDB-instance reference (DBHolder ByteBuffer). Volatile: read lock-free by
         * the JFR hook. Writing this last publishes the fully-initialized {@link #component}
         * and {@link #dbAddress} fields.
         */
        private volatile ByteBuffer dbRef;
        private String component;
        private long dbAddress;

        /**
         * Opens (or re-attempts opening) the pinned DB reference and increments the ref count.
         * Failure to pin is logged; the ref count is still incremented so close-balance is
         * preserved and a subsequent {@code open()} will retry while {@code dbRef == null}.
         */
        void open(DuckDBConnection conn) {
            if (dbRef == null) {
                try {
                    ByteBuffer ref = DuckDBNative.duckdb_jdbc_create_db_ref(conn.connRef);
                    component = conn.monitorName;
                    dbAddress = conn.dbAddress;
                    // Publish dbRef last so readers see fully-populated state.
                    dbRef = ref;
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to pin DB for JFR memory monitor; will retry on next open()", e);
                }
            }
            openConnections++;
        }

        /**
         * Decrements the ref count; when it reaches zero, releases the pinned DB reference
         * and signals the caller to remove the map entry.
         *
         * @return {@code true} when the entry should be removed
         */
        boolean close() {
            if (--openConnections > 0) {
                return false;
            }
            // Clamp on imbalance: a stray close() without a matching open() must not leave
            // the counter negative so that a future open() starts from a consistent state.
            openConnections = 0;
            ByteBuffer ref = dbRef;
            dbRef = null;
            if (ref != null) {
                try {
                    DuckDBNative.duckdb_jdbc_destroy_db_ref(ref);
                } catch (SQLException e) {
                    logger.log(Level.FINE, "Failed to release JFR memory-monitor DB ref", e);
                }
            }
            return true;
        }

        /** Invoked by the JFR periodic hook. Must not throw. */
        void sample() {
            ByteBuffer ref = dbRef;
            if (ref == null) {
                return;
            }
            long[] snapshot;
            try {
                snapshot = DuckDBNative.duckdb_jdbc_memory_snapshot(ref);
            } catch (Throwable t) {
                // Rare: native snapshot failure. Swallow so JFR's periodic dispatch survives.
                return;
            }
            if (snapshot == null) {
                return;
            }
            String componentSnap = component;
            long addr = dbAddress;
            // Snapshot is packed as [tag, size, evicted] triples; the tag index is
            // emitted explicitly by the native side, so the order of entries does
            // not matter. An unknown tag index (e.g. from a future native lib that
            // added tags Java does not know about) is skipped rather than misnamed.
            for (int i = 0; i + 3 <= snapshot.length; i += 3) {
                int tagIdx = (int) snapshot[i];
                if (tagIdx < 0 || tagIdx >= MEMORY_TAGS.length) {
                    continue;
                }
                emitEvent(componentSnap, addr, MEMORY_TAGS[tagIdx], snapshot[i + 1], snapshot[i + 2]);
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

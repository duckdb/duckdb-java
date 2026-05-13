package org.duckdb;

import static java.util.Collections.singletonList;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.duckdb.test.TempDirectory;

public class TestJfrEvents {

    /**
     * Verifies that:
     * <ul>
     *   <li>No events are emitted when the {@code jdbc_jfr_memory_monitor} property is absent.</li>
     *   <li>Events are emitted when the property is set, for each independent in-memory database,
     *   tagged with the user-supplied component identifier.</li>
     *   <li>Each event carries a non-empty tag and non-negative memory values.</li>
     * </ul>
     */
    public static void test_jfr_memory_event() throws Exception {
        // --- Part 1: no property -> no events ---
        try (Recording recOff = new Recording()) {
            recOff.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(100));
            recOff.start();
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                Connection conn1 = conn;
                conn1 = null;
                Thread.sleep(300);
            }
            recOff.stop();
            List<RecordedEvent> events = dumpEvents(recOff);
            assertTrue(events.isEmpty(), "Expected no events when monitor property is not set");
        }

        // --- Part 2: property set -> events emitted under each supplied component ---
        Properties propsA = new Properties();
        propsA.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "component-a");
        Properties propsB = new Properties();
        propsB.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "component-b");

        try (Recording rec = new Recording()) {
            rec.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(100));
            rec.start();

            // Two independent in-memory databases, each with its own component name.
            try (Connection conn1 = DriverManager.getConnection(JDBC_URL, propsA);
                 Connection conn2 = DriverManager.getConnection(JDBC_URL, propsB)) {
                Connection conn = conn1;
                conn = conn2;
                conn = null;
                Thread.sleep(400);
            }

            rec.stop();
            List<RecordedEvent> events = dumpEvents(rec);
            assertFalse(events.isEmpty(), "Expected at least one DuckDBMemory JFR event");

            Set<String> components = new HashSet<>();
            for (RecordedEvent event : events) {
                String component = event.getString("component");
                assertTrue(component != null && !component.isEmpty(), "component field must be non-empty");
                components.add(component);

                String tag = event.getString("tag");
                assertTrue(tag != null && !tag.isEmpty(), "tag field must be non-empty");

                long dbAddress = event.getLong("dbAddress");
                assertTrue(dbAddress != 0L, "dbAddress must be non-zero");

                long memUsage = event.getLong("memoryUsageBytes");
                assertTrue(memUsage >= 0, "memoryUsageBytes must be >= 0");
                long tmpStorage = event.getLong("temporaryStorageBytes");
                assertTrue(tmpStorage >= 0, "temporaryStorageBytes must be >= 0");
            }

            // Each component must emit events under its own identifier.
            assertTrue(components.contains("component-a") && components.contains("component-b"),
                       "Expected events for both component identifiers, got: " + components);
        }
    }

    /**
     * After the last monitored connection is closed, the monitor entry must be removed so
     * a subsequent recording observes no events.
     */
    public static void test_jfr_memory_event_cleanup_after_close() throws Exception {
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "cleanup-test");

        // Prime: open and close a monitored connection outside of any recording.
        DriverManager.getConnection(JDBC_URL, props).close();

        // Start a fresh recording with no monitored connection open.
        try (Recording rec = new Recording()) {
            rec.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(100));
            rec.start();
            Thread.sleep(400);
            rec.stop();
            List<RecordedEvent> events = dumpEvents(rec);
            assertTrue(events.isEmpty(),
                       "Expected no events after all monitored connections were closed, got " + events.size());
        }
    }

    /**
     * For a file-based database, two monitored connections share a single underlying DuckDB
     * instance, so the monitor samples it once and events carry a single {@code dbAddress}.
     * The monitor must remain active while at least one monitored connection is open.
     */
    public static void test_jfr_memory_event_file_db_refcount() throws Exception {
        try (TempDirectory dir = new TempDirectory()) {
            Path dbFile = dir.path().resolve("refcount.db");
            String url = JDBC_URL + dbFile;
            Properties monitored = new Properties();
            monitored.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "shared-db");

            try (Recording rec = new Recording()) {
                rec.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(100));
                rec.start();

                Connection conn1 = DriverManager.getConnection(url, monitored);
                Connection conn2 = DriverManager.getConnection(url, monitored);
                try {
                    Thread.sleep(250);
                    // Close one connection; monitor must stay alive via conn2.
                    conn1.close();
                    Thread.sleep(250);
                } finally {
                    conn2.close();
                }

                rec.stop();
                List<RecordedEvent> events = dumpEvents(rec);
                assertFalse(events.isEmpty(), "Expected events for the shared file-based DB");

                Set<Long> addresses = new HashSet<>();
                Set<String> components = new HashSet<>();
                for (RecordedEvent e : events) {
                    addresses.add(e.getLong("dbAddress"));
                    components.add(e.getString("component"));
                }
                assertEquals(addresses.size(), 1,
                             "Two connections to the same file DB must share dbAddress, got " + addresses);
                assertEquals(components, new HashSet<>(singletonList("shared-db")),
                             "All events must be tagged with the supplied component, got: " + components);
            }
        }
    }

    /**
     * An empty {@code jdbc_jfr_memory_monitor} value must be treated as "not set" and emit
     * no events, mirroring the absent-property behaviour.
     */
    public static void test_jfr_memory_event_empty_property_disables() throws Exception {
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "");

        try (Recording rec = new Recording()) {
            rec.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(100));
            rec.start();
            try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
                Connection conn1 = conn;
                conn1 = null;
                Thread.sleep(300);
            }
            rec.stop();
            List<RecordedEvent> events = dumpEvents(rec);
            assertTrue(events.isEmpty(),
                       "Expected no events when jdbc_jfr_memory_monitor is empty, got " + events.size());
        }
    }

    private static List<RecordedEvent> dumpEvents(Recording rec) throws Exception {
        Path jfrPath = Files.createTempFile("duckdb-jfr-", ".jfr");
        try {
            rec.dump(jfrPath);
            return RecordingFile.readAllEvents(jfrPath);
        } finally {
            Files.deleteIfExists(jfrPath);
        }
    }
}

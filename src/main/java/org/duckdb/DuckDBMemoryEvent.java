package org.duckdb;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event that records DuckDB memory usage for a single memory tag.
 *
 * <p>One event is emitted per memory tag per firing interval. Emission is
 * driven by JFR's periodic-event machinery: a single hook is registered via
 * {@link jdk.jfr.FlightRecorder#addPeriodicEvent} in
 * {@link DuckDBMemoryMonitor}, and JFR invokes it at the period configured on
 * the recording. Configure both the enabled state and the period in a JFR
 * configuration file or via JMC:
 *
 * <pre>{@code
 * <event name="duckdb.MemoryUsage">
 *   <setting name="enabled">true</setting>
 *   <setting name="period">1 s</setting>
 * </event>
 * }</pre>
 *
 * <p>Participation is opt-in per connection: set the
 * {@link DuckDBDriver#JDBC_JFR_MEMORY_MONITOR} connection property to the
 * identifier under which this connection's DuckDB instance should be tracked.
 * An absent or empty value disables emission for that connection. The JDBC
 * property is a pure enable/label switch; JFR controls whether and how often
 * the event fires.
 */
@Name("duckdb.MemoryUsage")
@Label("DuckDB Memory Usage")
@Description("Periodic snapshot of DuckDB internal memory consumption per tag")
@Category("DuckDB")
@StackTrace(false)
public class DuckDBMemoryEvent extends Event {

    @Label("Name")
    @Description(
        "User-assigned identifier of the DuckDB instance (value of the jdbc_jfr_memory_monitor connection property)")
    String name;

    @Label("Tag")
    @Description("DuckDB internal memory tag (e.g. \"Base\", \"Hash Table\", \"Buffer Manager\")")
    String tag;

    @Label("Database URL")
    @Description("JDBC URL or database name of the DuckDB instance emitting this event")
    String dbUrl;

    @Label("Database Address")
    @Description("Native address of the underlying DuckDB instance; disambiguates databases when names collide")
    long dbAddress;

    @Label("Memory Usage") @Description("Bytes currently allocated for this tag") long memoryUsageBytes;

    @Label("Temporary Storage Usage")
    @Description("Bytes spilled to the temporary storage for this tag")
    long temporaryStorageBytes;
}

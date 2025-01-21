package org.duckdb;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class DuckDBTime extends DuckDBTimestamp {
    public DuckDBTime(Time time) {
        super(TimeUnit.MILLISECONDS.toMicros(time.getTime()));
    }
}

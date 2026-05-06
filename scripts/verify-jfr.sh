#!/usr/bin/env bash
#
# Manual verification of the JFR memory-monitoring feature on Java >= 9.
#
# Usage:   ./scripts/verify-jfr.sh
# Assumes: `java`, `javac`, `jfr` on PATH point at Java 9+ (same major version).
#          `make release` has been run (artifacts under build/release).

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$REPO/build/release/duckdb_jdbc.jar"
TESTS="$REPO/build/release/duckdb_jdbc_tests.jar"
WORK="$(mktemp -d -t duckdb-jfr-verify.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

die()  { echo "error: $*" >&2; exit 1; }
step() { printf '\n== %s ==\n' "$*"; }

# Preconditions ---------------------------------------------------------------

java_major=$(java -version 2>&1 | awk -F\" '/version/ {split($2,a,"."); print (a[1]=="1")?a[2]:a[1]; exit}')
[[ "$java_major" -ge 9 ]] || die "need Java >= 9, got $java_major (use verify-jfr-java8.sh for Java 8)"
command -v jfr >/dev/null || die "'jfr' CLI not found on PATH"
[[ -f "$JAR" && -f "$TESTS" ]] || die "build artifacts missing; run 'make release' first"

echo "java $java_major -- $JAR"

# 1. Unit tests ---------------------------------------------------------------

step "running JFR unit tests"
java --enable-native-access=ALL-UNNAMED \
     -cp "$TESTS:$JAR" \
     org/duckdb/TestDuckDBJDBC test_jfr_memory

# 2. End-to-end demo + jfr CLI inspection ------------------------------------

step "capturing a live recording"
cat > "$WORK/JfrDemo.java" <<'EOF'
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.Properties;
import jdk.jfr.Recording;

public class JfrDemo {
    public static void main(String[] a) throws Exception {
        try (Recording r = new Recording()) {
            r.enable("duckdb.MemoryUsage").withPeriod(Duration.ofMillis(500));
            r.start();
            Properties p = new Properties();
            p.setProperty("jdbc_jfr_memory_monitor", "verify-jfr");
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:", p);
                 Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t AS SELECT range AS i FROM range(2000000)");
                Thread.sleep(2000);
            }
            r.stop();
            r.dump(Path.of(a[0]));
        }
    }
}
EOF
javac -d "$WORK" -cp "$JAR" "$WORK/JfrDemo.java"
java --enable-native-access=ALL-UNNAMED -cp "$WORK:$JAR" JfrDemo "$WORK/demo.jfr"

step "jfr summary (expect a non-zero count for duckdb.MemoryUsage)"
jfr summary "$WORK/demo.jfr" | grep duckdb.MemoryUsage \
    || die "duckdb.MemoryUsage event not found in recording"

step "first event (expect component=verify-jfr, non-zero dbAddress)"
jfr print --events duckdb.MemoryUsage "$WORK/demo.jfr" | sed -n '1,12p'

printf '\nOK\n'

#!/usr/bin/env bash
#
# Manual verification of the JFR memory-monitoring feature on Java 8.
#
# Usage:   ./scripts/verify-jfr-java8.sh
# Assumes: `java`, `javac` on PATH point at JDK 8 (e.g. `sdk u java 8.0.462-amzn`).
#          `make release` has been run so that the platform-specific native
#          library is available; this script rebuilds only the Java jars.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE="$REPO/build/release"
BUILD="$REPO/build/java8"
WORK="$(mktemp -d -t duckdb-jfr-verify8.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

die()  { echo "error: $*" >&2; exit 1; }
step() { printf '\n== %s ==\n' "$*"; }

# Preconditions ---------------------------------------------------------------

java_major=$(java -version 2>&1 | awk -F\" '/version/ {split($2,a,"."); print (a[1]=="1")?a[2]:a[1]; exit}')
[[ "$java_major" == "8" ]] || die "need Java 8, got $java_major (use verify-jfr.sh for Java >= 9)"
[[ -d "$RELEASE" ]] || die "run 'make release' first (native library not found)"
NATIVE_LIB=$(ls "$RELEASE"/libduckdb_java.so_* 2>/dev/null | head -n1) \
    || die "no libduckdb_java.so_* under $RELEASE"

# Build Java 8 jars (idempotent) ---------------------------------------------

if [[ ! -f "$BUILD/duckdb_jdbc_tests.jar" ]]; then
    step "building Java 8 jars"
    mkdir -p "$BUILD"
    (cd "$BUILD" \
        && cmake -DCMAKE_BUILD_TYPE=Release "$REPO" >/dev/null \
        && cmake --build . --target duckdb_jdbc_tests >/dev/null)
    cp "$BUILD/duckdb_jdbc_nolib.jar" "$BUILD/duckdb_jdbc.jar"
    jar uf "$BUILD/duckdb_jdbc.jar" -C "$(dirname "$NATIVE_LIB")" "$(basename "$NATIVE_LIB")"
fi

JAR="$BUILD/duckdb_jdbc.jar"
TESTS="$BUILD/duckdb_jdbc_tests.jar"

# Confirm bytecode 52 (Java 8) ------------------------------------------------

bc_hex=$(unzip -p "$JAR" org/duckdb/DuckDBMemoryEvent.class | od -An -N8 -tx1 | awk '{print $8}')
[[ "$bc_hex" == "34" ]] || die "expected bytecode 0x34 (Java 8), got 0x$bc_hex"

# 1. Unit tests on Java 8 + JFR ----------------------------------------------

step "running JFR unit tests on Java 8 (jdk.jfr backport present)"
java -cp "$TESTS:$JAR" org/duckdb/TestDuckDBJDBC test_jfr_memory

# 2. Fallback path: jfr.jar stripped from the bootclasspath -------------------

step "verifying the JFR-absent fallback path"
cat > "$WORK/NoJfrDemo.java" <<'EOF'
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Properties;

public class NoJfrDemo {
    public static void main(String[] a) throws Exception {
        try { Class.forName("jdk.jfr.FlightRecorder"); throw new AssertionError("JFR present"); }
        catch (ClassNotFoundException ok) {}
        Class.forName("org.duckdb.DuckDBDriver");
        Properties p = new Properties();
        p.setProperty("jdbc_jfr_memory_monitor", "ignored");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:", p);
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT 42")) { r.next(); }
        Method f = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
        f.setAccessible(true);
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        if (f.invoke(cl, "org.duckdb.DuckDBMemoryMonitor") != null
         || f.invoke(cl, "org.duckdb.DuckDBMemoryEvent")   != null)
            throw new AssertionError("JFR-dependent class was loaded");
        System.out.println("OK");
    }
}
EOF
javac -d "$WORK" -cp "$JAR" "$WORK/NoJfrDemo.java"

JRE_LIB="$JAVA_HOME/jre/lib"
BOOT="$JRE_LIB/rt.jar:$JRE_LIB/jsse.jar:$JRE_LIB/jce.jar:$JRE_LIB/charsets.jar"
java -Xbootclasspath:"$BOOT" -cp "$WORK:$JAR" NoJfrDemo

printf '\nOK\n'

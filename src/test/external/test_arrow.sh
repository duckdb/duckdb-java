#!/usr/bin/env bash
# Runs Arrow tests (src/test/external/TestArrow.java) directly from source, requires Java 17+.
# Arrow JARs listed in data/arrow/arrow_jars.txt are downloaded into .arrow if missing.
# Usage: src/test/external/test_arrow.sh [test_name_filter]

set -euo pipefail

# paths are relative to the project root
cd "$(dirname "$0")/../../.."

BUILD_DIR=build/release
ARROW_DIR=".arrow"

if [ -n "${JAVA_HOME:-}" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  JAVA=java
fi

if [ ! -d "${ARROW_DIR}" ] || [ -z "$(ls -A "${ARROW_DIR}")" ]; then
  mkdir -p "${ARROW_DIR}"
  xargs curl -fsSL --retry 3 --output-dir "${ARROW_DIR}" --remote-name-all < data/arrow/arrow_jars.txt
fi

"${JAVA}" -version

exec "${JAVA}" \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "${BUILD_DIR}/duckdb_jdbc.jar:${BUILD_DIR}/duckdb_jdbc_tests.jar:${ARROW_DIR}/*" \
  src/test/external/TestArrow.java "$@"

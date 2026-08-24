.PHONY: build test stress clean

SEP=
JARS=

ifeq ($(OS),Windows_NT)
	# windows is weird
	SEP=";"
	JARS=build/release
else
	SEP=":"
	JARS=build/release
endif

OS_NAME_OVERRIDE=
ifneq ($(OVERRIDE_JDBC_OS_NAME),)
	OS_NAME_OVERRIDE=-DOVERRIDE_JDBC_OS_NAME=$(OVERRIDE_JDBC_OS_NAME)
endif

OS_ARCH_OVERRIDE=
ifneq ($(OVERRIDE_JDBC_OS_ARCH),)
	OS_ARCH_OVERRIDE=-DOVERRIDE_JDBC_OS_ARCH=$(OVERRIDE_JDBC_OS_ARCH)
endif

LIBC_OVERRIDE=
ifneq ($(OVERRIDE_JDBC_LIBC),)
	LIBC_OVERRIDE=-DOVERRIDE_JDBC_LIBC=$(OVERRIDE_JDBC_LIBC)
endif

PERF_ROWS?=2000000
PERF_SAMPLES?=5

# the platform classifier used for the CMake-produced native JAR
NATIVE_UNAME:=$(shell uname -s | tr 'A-Z' 'a-z')
NATIVE_MACHINE:=$(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
NATIVE_CLASSIFIER?=$(NATIVE_UNAME)_$(NATIVE_MACHINE)
ifeq ($(NATIVE_UNAME),darwin)
	NATIVE_CLASSIFIER=osx_universal
endif
ifeq ($(OS),Windows_NT)
	NATIVE_CLASSIFIER=windows_amd64
endif
ifneq ($(OVERRIDE_JDBC_LIBC),)
	NATIVE_CLASSIFIER:=$(NATIVE_CLASSIFIER)_$(OVERRIDE_JDBC_LIBC)
endif


GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

JAR=$(JARS)/duckdb_jdbc.jar
NATIVE_JAR=$(JARS)/duckdb_jdbc_native_$(NATIVE_CLASSIFIER).jar
TEST_JAR=$(JARS)/duckdb_jdbc_tests.jar
CP=$(JAR)$(SEP)$(NATIVE_JAR)$(SEP)$(TEST_JAR)

test: 
	java -cp $(CP) org.duckdb.TestDuckDBJDBC

stress:
	java -Dduckdb.perf.rows=$(PERF_ROWS) -Dduckdb.perf.samples=$(PERF_SAMPLES) \
		-Dduckdb.perf.assert=true -cp $(CP) org.duckdb.TestDuckDBJDBC TestStringWritePerformance

debug:
	mkdir -p build/debug
	cd build/debug && cmake -DCMAKE_BUILD_TYPE=Debug $(GENERATOR) $(OS_NAME_OVERRIDE) $(OS_ARCH_OVERRIDE) $(LIBC_OVERRIDE) ../.. && cmake --build . --config Debug

release:
	mkdir -p build/release
	cd build/release && cmake -DCMAKE_BUILD_TYPE=Release $(GENERATOR) $(OS_NAME_OVERRIDE) $(OS_ARCH_OVERRIDE) $(LIBC_OVERRIDE) ../.. && cmake --build . --config Release

sanitized:
	mkdir -p build/sanitized
	cd build/sanitized && cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_ADDRESS_SANITIZER=ON $(GENERATOR) $(OS_NAME_OVERRIDE) $(OS_ARCH_OVERRIDE) $(LIBC_OVERRIDE) ../.. && cmake --build . --config Release

format:
	python3 scripts/format.py

format-check:
	python3 scripts/format.py --check

clean:
	rm -rf build

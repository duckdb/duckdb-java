#pragma once

// Helpers for the parts of the driver that are built on top of the DuckDB C API v2 (duckdb_v2.h).
//
// The Java side keeps native handles in direct ByteBuffers, the C API v2 handles are passed to Java directly.

// The driver uses the parts of the C API v2 that are not yet stabilized, the engine is always vendored
// so the API version always matches the engine
#ifndef DUCKDB_V2_API_ALLOW_UNSTABLE
#define DUCKDB_V2_API_ALLOW_UNSTABLE 1
#endif
#include "duckdb_v2.h"

#include <cstdint>
#include <string>
#include <utility>

/**
 * Return codes, that are passed to Java, numerically the same as the ones of C API v1 (DuckDBSuccess / DuckDBError).
 */
static constexpr int32_t CAPI_STATE_SUCCESS = 0;
static constexpr int32_t CAPI_STATE_ERROR = 1;

/**
 * Type IDs, that are passed to Java (see DuckDBBindings.CAPIType), numerically the same as the DUCKDB_TYPE
 * enum of C API v1.
 */
enum class capi_type : int32_t {
	INVALID = 0,
	BOOLEAN = 1,
	TINYINT = 2,
	SMALLINT = 3,
	INTEGER = 4,
	BIGINT = 5,
	UTINYINT = 6,
	USMALLINT = 7,
	UINTEGER = 8,
	UBIGINT = 9,
	FLOAT = 10,
	DOUBLE = 11,
	TIMESTAMP = 12,
	DATE = 13,
	TIME = 14,
	INTERVAL = 15,
	HUGEINT = 16,
	VARCHAR = 17,
	BLOB = 18,
	DECIMAL = 19,
	TIMESTAMP_S = 20,
	TIMESTAMP_MS = 21,
	TIMESTAMP_NS = 22,
	ENUM = 23,
	LIST = 24,
	STRUCT = 25,
	MAP = 26,
	UUID = 27,
	UNION = 28,
	BIT = 29,
	TIME_TZ = 30,
	TIMESTAMP_TZ = 31,
	UHUGEINT = 32,
	ARRAY = 33,
	ANY = 34,
	BIGNUM = 35,
	SQLNULL = 36,
	STRING_LITERAL = 37,
	INTEGER_LITERAL = 38,
	TIME_NS = 39,
	GEOMETRY = 40,
	VARIANT = 41,
	TIMESTAMP_TZ_NS = 42,
	GEOGRAPHY = 43,
};

/**
 * Converts the C API v2 type ID to the C API v1 type ID that is used on Java side.
 */
capi_type type_id_to_capi(DUCKDB_V2_LOGICAL_TYPE_ID type_id);

/**
 * Converts the C API v1 type ID, that is used on Java side, to the C API v2 type ID, returns false
 * if the type cannot be created with the C API v2.
 */
bool type_id_from_capi(int32_t capi_type_id, DUCKDB_V2_LOGICAL_TYPE_ID &type_id);

/**
 * Owned error info, that is filled by a C API v2 call on failure.
 */
class v2_error {
public:
	v2_error() = default;
	~v2_error();
	v2_error(const v2_error &) = delete;
	v2_error &operator=(const v2_error &) = delete;

	//! The out parameter to pass to a C API v2 call
	duckdb_v2_error_info_handle *out();
	//! The error code, DUCKDB_V2_ERROR_API if the call did not report it
	DUCKDB_V2_ERROR code() const;
	//! The error message with the error type prefix, e.g. "Invalid Input Error: ..."
	std::string message() const;
	//! The error message without the error type prefix
	std::string raw_message() const;

private:
	duckdb_v2_error_info_handle handle = nullptr;
};

/**
 * Owned C API v2 handle, that is destroyed when it goes out of scope.
 */
template <class H, DUCKDB_V2_ERROR (*DESTROY)(H *)>
class v2_handle {
public:
	v2_handle() = default;
	explicit v2_handle(H handle_p) : handle(handle_p) {
	}
	~v2_handle() {
		reset();
	}
	v2_handle(const v2_handle &) = delete;
	v2_handle &operator=(const v2_handle &) = delete;
	v2_handle(v2_handle &&other) noexcept : handle(other.release()) {
	}
	v2_handle &operator=(v2_handle &&other) noexcept {
		reset();
		handle = other.release();
		return *this;
	}

	H get() const {
		return handle;
	}
	H *out() {
		reset();
		return &handle;
	}
	H release() {
		H res = handle;
		handle = nullptr;
		return res;
	}
	void reset() {
		if (handle != nullptr) {
			DESTROY(&handle);
			handle = nullptr;
		}
	}
	explicit operator bool() const {
		return handle != nullptr;
	}

private:
	H handle = nullptr;
};

using v2_logical_type = v2_handle<duckdb_v2_logical_type_handle, duckdb_v2_logical_type_destroy>;
using v2_value = v2_handle<duckdb_v2_value_handle, duckdb_v2_value_destroy>;
using v2_data_chunk = v2_handle<duckdb_v2_data_chunk_handle, duckdb_v2_data_chunk_destroy>;
using v2_schema = v2_handle<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
using v2_result = v2_handle<duckdb_v2_result_handle, duckdb_v2_result_destroy>;
using v2_scalar_function = v2_handle<duckdb_v2_scalar_function_handle, duckdb_v2_scalar_function_destroy>;
using v2_table_function = v2_handle<duckdb_v2_table_function_handle, duckdb_v2_table_function_destroy>;

/**
 * The factory of a process-wide DuckDB instance that has no databases attached to it, or nullptr if
 * the instance cannot be created.
 *
 * Unlike the C API v1, the C API v2 requires a factory to create types, data chunks and values. Logical types
 * and data chunks can be created on Java side without a connection, the factory of this hidden instance is
 * used in such cases. It knows only built-in types and casts.
 */
duckdb_v2_factory_handle default_factory();

/**
 * A placeholder handle that represents the INVALID logical type, that cannot be created with the C API v2.
 * It is not a valid C API v2 handle and must not be passed to the C API v2 calls.
 */
duckdb_v2_logical_type_handle invalid_logical_type();

/**
 * Returns the C API v1 type ID of the specified type, the placeholder INVALID type is reported as INVALID.
 */
capi_type logical_type_to_capi(duckdb_v2_logical_type_handle logical_type);

/**
 * Returns an owned copy of the specified type (the placeholder INVALID type is not copied), or nullptr on failure.
 */
duckdb_v2_logical_type_handle copy_logical_type(duckdb_v2_logical_type_handle logical_type);

/**
 * Destroys the specified type, the placeholder INVALID type is not destroyed.
 */
void destroy_logical_type(duckdb_v2_logical_type_handle logical_type);

/**
 * Reports an error from a function callback through its error slot. The engine raises it as an exception of
 * the type that corresponds to the code, e.g. DUCKDB_V2_ERROR_QUERY_BINDER is raised as BinderException.
 */
void set_callback_error(duckdb_v2_error_info_handle *err, DUCKDB_V2_ERROR code, const std::string &message);

/**
 * Builds the string (blob) element of a VARCHAR/BLOB vector, the bytes that do not fit inline are copied
 * into the specified vector heap. The bytes are not validated.
 */
bool make_v2_bytes(duckdb_v2_arena_handle arena, const char *data, idx_t len, duckdb_v2_bytes &out);

/**
 * The data and the length of the string (blob) element of a VARCHAR/BLOB vector.
 */
const char *v2_bytes_data(const duckdb_v2_bytes &bytes);

idx_t v2_bytes_length(const duckdb_v2_bytes &bytes);

/**
 * Owned logical type, that can also hold the placeholder INVALID type, used to keep the types configured from Java.
 */
class spec_logical_type {
public:
	spec_logical_type() = default;
	//! Copies the specified type, the placeholder INVALID type is kept as is
	explicit spec_logical_type(duckdb_v2_logical_type_handle logical_type)
	    : handle(logical_type == invalid_logical_type() ? logical_type : copy_logical_type(logical_type)) {
	}
	~spec_logical_type() {
		destroy_logical_type(handle);
	}
	spec_logical_type(const spec_logical_type &) = delete;
	spec_logical_type &operator=(const spec_logical_type &) = delete;
	spec_logical_type(spec_logical_type &&other) noexcept : handle(other.handle) {
		other.handle = nullptr;
	}
	spec_logical_type &operator=(spec_logical_type &&other) noexcept {
		std::swap(handle, other.handle);
		return *this;
	}

	duckdb_v2_logical_type_handle get() const {
		return handle;
	}
	//! Whether the type is set and is not INVALID
	bool is_valid() const {
		return handle != nullptr && handle != invalid_logical_type();
	}

private:
	duckdb_v2_logical_type_handle handle = nullptr;
};

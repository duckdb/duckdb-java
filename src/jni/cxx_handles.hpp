#pragma once

#include "capi_v2.hpp"
#include "duckdb_cpp.hpp"

/**
 * The stable C++ API wrappers own C API v2 handles. Their handle() accessors are internal to the API and are not
 * available outside of it by default, the traits that map a wrapper to its handle type are specialized here for
 * the wrappers whose handles are passed to the C API v2 by the driver (the bindings use the C API v2 directly).
 */
namespace duckdb {
namespace cxx {
namespace detail {

template <>
struct HandleTraits<Connection> {
	using handle = duckdb_v2_connection_handle;
};
template <>
struct HandleTraits<PreparedStatement> {
	using handle = duckdb_v2_prepared_statement_handle;
};
template <>
struct HandleTraits<Value> {
	using handle = duckdb_v2_value_handle;
};
template <>
struct HandleTraits<LogicalType> {
	using handle = duckdb_v2_logical_type_handle;
};

} // namespace detail
} // namespace cxx
} // namespace duckdb

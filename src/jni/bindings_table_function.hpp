#pragma once

#include "bindings.hpp"
#include "holders.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

/*
 * Bind data, global init data and local init data of a table function, holds a global reference to the Java object.
 * Same as in C API v1, the Java object is closed when the state is destroyed.
 */
struct TableFunctionState {
	explicit TableFunctionState(GlobalRefHolder *holder_p) : holder(holder_p) {
	}

	~TableFunctionState() noexcept {
		GlobalRefHolder::destroy_table_function_state(holder);
	}

	TableFunctionState(const TableFunctionState &) = delete;

	TableFunctionState &operator=(const TableFunctionState &) = delete;

	jobject get_object() const {
		return holder != nullptr ? holder->global_ref : nullptr;
	}

	GlobalRefHolder *release() {
		auto res = holder;
		holder = nullptr;
		return res;
	}

	static void destroy(void *state) {
		delete reinterpret_cast<TableFunctionState *>(state);
	}

	static jobject get_object(void *state) {
		return state != nullptr ? reinterpret_cast<TableFunctionState *>(state)->get_object() : nullptr;
	}

	GlobalRefHolder *holder;
};

/*
 * User data of the registered C API v2 table function, the same as "extra info" in C API v1.
 */
struct TableFunctionUserData {
	std::string name;
	std::unique_ptr<GlobalRefHolder> callback;
	std::vector<std::pair<std::string, spec_logical_type>> named_parameters;
	bool has_local_init = false;
};

/*
 * Bind info that is passed to Java during the bind call.
 */
struct TableFunctionBindInfoHolder {
	TableFunctionBindInfoHolder(duckdb_v2_function_bind_info_handle info_p,
	                            duckdb_v2_table_function_bind_info_handle result_p, duckdb_v2_context_handle context_p,
	                            TableFunctionUserData &user_data_p)
	    : info(info_p), result(result_p), context(context_p), user_data(user_data_p) {
	}

	duckdb_v2_function_bind_info_handle info;
	duckdb_v2_table_function_bind_info_handle result;
	duckdb_v2_context_handle context;
	TableFunctionUserData &user_data;
	//! The number of the positional arguments passed to the function
	idx_t positional_count = 0;
	//! The named parameters passed to the function, cast to the declared types
	std::vector<std::pair<std::string, v2_value>> named_parameters;
	std::unique_ptr<TableFunctionState> bind_data;
	bool has_error = false;
	std::string error;
};

/*
 * Init info that is passed to Java during the global and local init calls.
 */
struct TableFunctionInitInfoHolder {
	duckdb_v2_table_function_init_global_info_handle global_info = nullptr;
	duckdb_v2_table_function_init_local_info_handle local_info = nullptr;
	std::unique_ptr<TableFunctionState> init_data;
	bool has_error = false;
	std::string error;
};

/*
 * Function info that is passed to Java during the function call.
 */
struct TableFunctionInfoHolder {
	TableFunctionInfoHolder(duckdb_v2_table_function_exec_info_handle info_p, bool has_local_init_p)
	    : info(info_p), has_local_init(has_local_init_p) {
	}

	duckdb_v2_table_function_exec_info_handle info;
	bool has_local_init;
	bool has_error = false;
	std::string error;
};

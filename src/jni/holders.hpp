#pragma once

#include "capi_v2.hpp"
#include "refs.hpp"

#include <jni.h>
#include <mutex>
#include <string>
#include <vector>

/**
 * Result of a prepared statement execution that is read by chunks (see DuckDBChunkedResult) using C API v2.
 * The column names and types are captured when the result is created, the error is set when fetching of
 * a chunk fails, same as with the C API v1 result.
 */
struct ChunkedResultHolder {
	v2_result result;
	std::vector<std::string> column_names;
	std::vector<capi_type> column_types;
	bool has_error = false;
	std::string error;

	//! Takes ownership of the result, throws if the result schema cannot be read
	explicit ChunkedResultHolder(duckdb_v2_result_handle result_p);
};

struct AttachedJNIEnv {
	JavaVM *vm = nullptr;
	JNIEnv *env = nullptr;
	bool need_to_detach = false;

	AttachedJNIEnv();

	AttachedJNIEnv(JavaVM *vm_in, JNIEnv *env_in, bool need_to_detach_in);

	~AttachedJNIEnv() noexcept;
};

struct GlobalRefHolder {
	JavaVM *vm = nullptr;
	jobject global_ref = nullptr;

	GlobalRefHolder(JNIEnv *env, jobject local_ref);

	~GlobalRefHolder() noexcept;

	AttachedJNIEnv attach_current_thread();

	void detach_current_thread();

	static void destroy(void *holder_in) noexcept;
	static void destroy_table_function_state(void *holder_in) noexcept;
};

struct LocalRefHolder {
	JNIEnv *env = nullptr;
	jobject local_ref = nullptr;

	LocalRefHolder(JNIEnv *env_in, jobject local_ref_in);

	~LocalRefHolder() noexcept;
};

/**
 * Returns the C API v2 handle of the connection that is passed from Java as a direct buffer. Throws a SQLException and
 * returns nullptr if a valid connection can't be retrieved from the buffer.
 */
duckdb_v2_connection_handle conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf);

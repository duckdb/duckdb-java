#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstring>
#include <vector>

static ChunkedResultHolder *result_buf_to_result(JNIEnv *env, jobject result_buf) {

	if (result_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid result buffer");
		return nullptr;
	}

	auto result = reinterpret_cast<ChunkedResultHolder *>(env->GetDirectBufferAddress(result_buf));
	if (result == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid result");
		return nullptr;
	}

	return result;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_result
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1result(JNIEnv *env, jclass, jobject result) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return;
	}

	delete res;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_fetch_chunk
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1fetch_1chunk(JNIEnv *env, jclass, jobject result) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_v2_data_chunk_handle chunk = nullptr;
	v2_error err;
	if (duckdb_v2_result_fetch_chunk(res->result.get(), &chunk, err.out()) != DUCKDB_V2_ERROR_NONE) {
		// same as in C API v1, the error is set on the result so duckdb_result_error can retrieve it
		res->has_error = true;
		res->error = err.message();
		return nullptr;
	}

	// an empty handle marks the end of the result
	return make_data_chunk_buf(env, chunk);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_name
 * Signature: (Ljava/nio/ByteBuffer;J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1name(JNIEnv *env, jclass, jobject result,
                                                                                 jlong col) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t col_idx = jlong_to_idx(env, col);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (col_idx >= res->column_names.size()) {
		return nullptr;
	}
	auto &name = res->column_names[col_idx];

	return make_jbyteArray(env, name.c_str(), static_cast<idx_t>(std::strlen(name.c_str())));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_type
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1type(JNIEnv *env, jclass, jobject result,
                                                                           jlong col) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t col_idx = jlong_to_idx(env, col);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (col_idx >= res->column_types.size()) {
		return static_cast<jint>(capi_type::INVALID);
	}

	return static_cast<jint>(res->column_types[col_idx]);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1count(JNIEnv *env, jclass, jobject result) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return -1;
	}

	return static_cast<jlong>(res->column_names.size());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_result_error
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1result_1error(JNIEnv *env, jclass, jobject result) {

	auto res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (!res->has_error) {
		return nullptr;
	}

	return make_jbyteArray(env, res->error.c_str(), static_cast<idx_t>(std::strlen(res->error.c_str())));
}

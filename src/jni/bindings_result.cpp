#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstring>
#include <vector>

duckdb_result *result_buf_to_result(JNIEnv *env, jobject result_buf) {

	if (result_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid result buffer");
		return nullptr;
	}

	duckdb_result *result = reinterpret_cast<duckdb_result *>(env->GetDirectBufferAddress(result_buf));
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

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_result *ptr = res;
	duckdb_destroy_result(res);
	delete ptr;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_fetch_chunk
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1fetch_1chunk(JNIEnv *env, jclass, jobject result) {

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_data_chunk chunk = duckdb_fetch_chunk(*res);

	return make_ptr_buf(env, chunk);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_name
 * Signature: (Ljava/nio/ByteBuffer;J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1name(JNIEnv *env, jclass, jobject result,
                                                                                 jlong col) {

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t col_idx = jlong_to_idx(env, col);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	const char *name = duckdb_column_name(res, col_idx);
	if (name == nullptr) {
		return nullptr;
	}

	idx_t len = static_cast<idx_t>(std::strlen(name));

	return make_jbyteArray(env, name, len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_type
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1type(JNIEnv *env, jclass, jobject result,
                                                                           jlong col) {

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t col_idx = jlong_to_idx(env, col);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_type dt = duckdb_column_type(res, col_idx);

	return static_cast<jint>(dt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1column_1count(JNIEnv *env, jclass, jobject result) {

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = duckdb_column_count(res);

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_result_error
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1result_1error(JNIEnv *env, jclass, jobject result) {

	duckdb_result *res = result_buf_to_result(env, result);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	const char *error_msg = duckdb_result_error(res);
	if (error_msg == nullptr) {
		return nullptr;
	}

	idx_t len = static_cast<idx_t>(std::strlen(error_msg));

	return make_jbyteArray(env, error_msg, len);
}
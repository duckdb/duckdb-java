#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <vector>

duckdb_data_chunk chunk_buf_to_chunk(JNIEnv *env, jobject chunk_buf) {

	if (chunk_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid data chunk buffer");
		return nullptr;
	}

	duckdb_data_chunk chunk = reinterpret_cast<duckdb_data_chunk>(env->GetDirectBufferAddress(chunk_buf));

	if (chunk == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid data chunk");
		return nullptr;
	}

	return chunk;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_data_chunk
 * Signature: ([Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1data_1chunk(JNIEnv *env, jclass,
                                                                                     jobjectArray logical_types) {

	if (logical_types == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid logical type array");
		return nullptr;
	}

	size_t column_count = static_cast<size_t>(env->GetArrayLength(logical_types));

	std::vector<duckdb_logical_type> lt_vec;
	lt_vec.reserve(column_count);

	for (size_t i = 0; i < column_count; i++) {

		jobject lt_buf = env->GetObjectArrayElement(logical_types, i);
		if (env->ExceptionCheck()) {
			return nullptr;
		}

		duckdb_logical_type lt = logical_type_buf_to_logical_type(env, lt_buf);
		if (env->ExceptionCheck()) {
			return nullptr;
		}

		lt_vec.push_back(lt);
	}

	duckdb_data_chunk data_chunk = duckdb_create_data_chunk(lt_vec.data(), static_cast<idx_t>(column_count));

	return make_ptr_buf(env, data_chunk);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_data_chunk
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1data_1chunk(JNIEnv *env, jclass, jobject chunk) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_data_chunk(&dc);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_reset
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1reset(JNIEnv *env, jclass, jobject chunk) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_data_chunk_reset(dc);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_get_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1get_1column_1count(JNIEnv *env, jclass,
                                                                                               jobject chunk) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = duckdb_data_chunk_get_column_count(dc);

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_get_vector
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1get_1vector(JNIEnv *env, jclass,
                                                                                          jobject chunk,
                                                                                          jlong col_idx) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t idx = jlong_to_idx(env, col_idx);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_vector vec = duckdb_data_chunk_get_vector(dc, idx);

	return make_ptr_buf(env, vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_get_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1get_1size(JNIEnv *env, jclass,
                                                                                      jobject chunk) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t dc_size = duckdb_data_chunk_get_size(dc);

	return static_cast<jlong>(dc_size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_set_size
 * Signature: (Ljava/nio/ByteBuffer;J)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1set_1size(JNIEnv *env, jclass, jobject chunk,
                                                                                     jlong size) {

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_data_chunk_set_size(dc, static_cast<idx_t>(size));
}

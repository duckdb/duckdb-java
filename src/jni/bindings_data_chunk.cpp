#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <vector>

thread_local ScalarFunctionInputRowCount scalar_function_input_row_count;

duckdb_v2_data_chunk_handle chunk_buf_to_chunk(JNIEnv *env, jobject chunk_buf) {

	if (chunk_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid data chunk buffer");
		return nullptr;
	}

	auto chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(env->GetDirectBufferAddress(chunk_buf));

	if (chunk == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid data chunk");
		return nullptr;
	}

	return chunk;
}

jobject make_data_chunk_buf(JNIEnv *env, duckdb_v2_data_chunk_handle chunk) {
	if (chunk == nullptr) {
		return nullptr;
	}
	jobject buf = make_ptr_buf(env, chunk);
	if (buf == nullptr) {
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	return buf;
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

	std::vector<duckdb_v2_logical_type_handle> lt_vec;
	lt_vec.reserve(column_count);

	for (size_t i = 0; i < column_count; i++) {

		jobject lt_buf = env->GetObjectArrayElement(logical_types, i);
		if (env->ExceptionCheck()) {
			return nullptr;
		}

		auto lt = logical_type_buf_to_logical_type(env, lt_buf);
		env->DeleteLocalRef(lt_buf);
		if (env->ExceptionCheck()) {
			return nullptr;
		}

		// same as in C API v1, INVALID types are rejected, ANY types are rejected when the chunk is created
		if (lt == invalid_logical_type()) {
			return nullptr;
		}

		lt_vec.push_back(lt);
	}

	duckdb_v2_data_chunk_handle data_chunk = nullptr;
	if (duckdb_v2_data_chunk_create(default_factory(), lt_vec.data(), static_cast<idx_t>(column_count), &data_chunk,
	                                nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}

	return make_data_chunk_buf(env, data_chunk);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_data_chunk
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1data_1chunk(JNIEnv *env, jclass, jobject chunk) {

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_v2_data_chunk_destroy(&dc);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_reset
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1reset(JNIEnv *env, jclass, jobject chunk) {

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	v2_error err;
	if (duckdb_v2_data_chunk_clear(dc, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_get_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1get_1column_1count(JNIEnv *env, jclass,
                                                                                               jobject chunk) {

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = 0;
	v2_error err;
	if (duckdb_v2_data_chunk_get_vector_count(dc, &count, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return -1;
	}

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

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t idx = jlong_to_idx(env, col_idx);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t count = 0;
	if (duckdb_v2_data_chunk_get_vector_count(dc, &count, nullptr) != DUCKDB_V2_ERROR_NONE || idx >= count) {
		return nullptr;
	}

	// vectors are borrowed from their chunk
	duckdb_v2_vector_handle vec = nullptr;
	v2_error err;
	if (duckdb_v2_data_chunk_get_vector(dc, idx, &vec, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return make_ptr_buf(env, vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_get_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1get_1size(JNIEnv *env, jclass,
                                                                                      jobject chunk) {

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (dc == scalar_function_input_row_count.chunk) {
		return static_cast<jlong>(scalar_function_input_row_count.row_count);
	}

	idx_t dc_size = 0;
	v2_error err;
	if (duckdb_v2_data_chunk_get_size(dc, &dc_size, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return -1;
	}

	return static_cast<jlong>(dc_size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_data_chunk_set_size
 * Signature: (Ljava/nio/ByteBuffer;J)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1data_1chunk_1set_1size(JNIEnv *env, jclass, jobject chunk,
                                                                                     jlong size) {

	auto dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return;
	}

	// The C API v2 derives the row count of a chunk from its vectors, so the size is set on every vector,
	// same as in C API v1, errors are not reported here
	idx_t count = 0;
	if (duckdb_v2_data_chunk_get_vector_count(dc, &count, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_vector_handle vec = nullptr;
		if (duckdb_v2_data_chunk_get_vector(dc, i, &vec, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_vector_set_size(vec, static_cast<idx_t>(size), nullptr);
	}
	if (dc == scalar_function_input_row_count.chunk) {
		scalar_function_input_row_count.row_count = static_cast<idx_t>(size);
	}
}

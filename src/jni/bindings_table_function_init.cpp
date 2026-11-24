#include "bindings_table_function.hpp"
#include "refs.hpp"
#include "util.hpp"

static TableFunctionInitInfoHolder *init_info_buf_to_init_info(JNIEnv *env, jobject init_info_buf) {
	if (init_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function init info buffer");
		return nullptr;
	}
	auto init_info = reinterpret_cast<TableFunctionInitInfoHolder *>(env->GetDirectBufferAddress(init_info_buf));
	if (init_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function init info");
		return nullptr;
	}
	return init_info;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_get_bind_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1bind_1data(JNIEnv *env, jclass,
                                                                                       jobject init_info) {

	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *bind_data = nullptr;
	v2_error err;
	auto state = ii->global_info != nullptr
	                 ? duckdb_v2_table_function_init_global_get_bind_data(ii->global_info, &bind_data, err.out())
	                 : duckdb_v2_table_function_init_local_get_bind_data(ii->local_info, &bind_data, err.out());
	if (state != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return TableFunctionState::get_object(bind_data);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_set_init_data
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1init_1data(JNIEnv *env, jclass,
                                                                                    jobject init_info,
                                                                                    jobject init_data) {
	if (init_data == nullptr) {
		return;
	}

	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	auto init_data_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, init_data));
	if (init_data_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified init data");
		return;
	}

	ii->init_data = std::unique_ptr<TableFunctionState>(new TableFunctionState(init_data_holder.release()));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_get_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1column_1count(JNIEnv *env, jclass,
                                                                                        jobject init_info) {

	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t count = 0;
	v2_error err;
	auto state = ii->global_info != nullptr
	                 ? duckdb_v2_table_function_init_global_get_column_count(ii->global_info, &count, err.out())
	                 : duckdb_v2_table_function_init_local_get_column_count(ii->local_info, &count, err.out());
	if (state != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return 0;
	}
	return uint64_to_jlong(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_get_column_index
 * Signature: (Ljava/nio/ByteBuffer;J)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1column_1index(JNIEnv *env, jclass,
                                                                                        jobject init_info,
                                                                                        jlong column_index) {

	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t column_index_idx = jlong_to_idx(env, column_index);
	if (env->ExceptionCheck()) {
		return 0;
	}

	// same as in C API v1, 0 is returned for an invalid index
	idx_t count = 0;
	idx_t idx = 0;
	if (ii->global_info != nullptr) {
		if (duckdb_v2_table_function_init_global_get_column_count(ii->global_info, &count, nullptr) ==
		        DUCKDB_V2_ERROR_NONE &&
		    column_index_idx < count) {
			duckdb_v2_table_function_init_global_get_column_index(ii->global_info, column_index_idx, &idx, nullptr);
		}
	} else {
		if (duckdb_v2_table_function_init_local_get_column_count(ii->local_info, &count, nullptr) ==
		        DUCKDB_V2_ERROR_NONE &&
		    column_index_idx < count) {
			duckdb_v2_table_function_init_local_get_column_index(ii->local_info, column_index_idx, &idx, nullptr);
		}
	}
	return uint64_to_jlong(idx);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_set_max_threads
 * Signature: (Ljava/nio/ByteBuffer;J)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1max_1threads(JNIEnv *env, jclass,
                                                                                      jobject init_info,
                                                                                      jlong max_threads) {

	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	idx_t mt = jlong_to_idx(env, max_threads);
	if (env->ExceptionCheck()) {
		return;
	}

	// the number of threads can only be set in the global init
	if (ii->global_info == nullptr) {
		return;
	}
	v2_error err;
	if (duckdb_v2_table_function_init_global_set_max_threads(ii->global_info, mt, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1error(JNIEnv *env, jclass, jobject init_info,
                                                                               jbyteArray error) {
	auto ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	ii->has_error = true;
	ii->error = error_str;
}

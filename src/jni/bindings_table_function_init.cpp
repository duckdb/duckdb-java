#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

static duckdb_init_info init_info_buf_to_init_info(JNIEnv *env, jobject init_info_buf) {
	if (init_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function init info buffer");
		return nullptr;
	}
	auto init_info = reinterpret_cast<duckdb_init_info>(env->GetDirectBufferAddress(init_info_buf));
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

	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *bind_data = duckdb_init_get_bind_data(ii);
	if (bind_data == nullptr) {
		return nullptr;
	}

	GlobalRefHolder *holder = reinterpret_cast<GlobalRefHolder *>(bind_data);
	return holder->global_ref;
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

	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	auto init_data_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, init_data));
	if (init_data_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified init data");
		return;
	}

	duckdb_init_set_init_data(ii, init_data_holder.release(), GlobalRefHolder::destroy);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_get_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1column_1count(JNIEnv *env, jclass,
                                                                                        jobject init_info) {

	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t count = duckdb_init_get_column_count(ii);
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

	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t column_index_idx = jlong_to_idx(env, column_index);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t idx = duckdb_init_get_column_index(ii, column_index_idx);
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

	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	idx_t mt = jlong_to_idx(env, max_threads);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_init_set_max_threads(ii, mt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_init_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1error(JNIEnv *env, jclass, jobject init_info,
                                                                               jbyteArray error) {
	duckdb_init_info ii = init_info_buf_to_init_info(env, init_info);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_init_set_error(ii, error_str.c_str());
}

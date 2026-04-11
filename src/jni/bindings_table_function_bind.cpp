#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

static duckdb_bind_info bind_info_buf_to_bind_info(JNIEnv *env, jobject bind_info_buf) {
	if (bind_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function bind info buffer");
		return nullptr;
	}
	auto bind_info = reinterpret_cast<duckdb_bind_info>(env->GetDirectBufferAddress(bind_info_buf));
	if (bind_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function bind info");
		return nullptr;
	}
	return bind_info;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_add_result_column
 * Signature: (Ljava/nio/ByteBuffer;[BLjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1add_1result_1column(JNIEnv *env, jclass,
                                                                                        jobject bind_info,
                                                                                        jbyteArray name,
                                                                                        jobject logical_type) {

	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name_str.empty()) {
		env->ThrowNew(J_SQLException, "Specified result column name must be not empty");
		return;
	}
	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_bind_add_result_column(bi, name_str.c_str(), lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_parameter_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter_1count(JNIEnv *env, jclass,
                                                                                           jobject bind_info) {

	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	idx_t count = duckdb_bind_get_parameter_count(bi);

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_parameter
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter(JNIEnv *env, jclass,
                                                                                      jobject bind_info, jlong index) {

	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_value val = duckdb_bind_get_parameter(bi, index_idx);

	return make_ptr_buf(env, val);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_named_parameter
 * Signature: (Ljava/nio/ByteBuffer;[B)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1named_1parameter(JNIEnv *env, jclass,
                                                                                             jobject bind_info,
                                                                                             jbyteArray name) {

	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_value val = duckdb_bind_get_named_parameter(bi, name_str.c_str());

	return make_ptr_buf(env, val);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_set_bind_data
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1bind_1data(JNIEnv *env, jclass,
                                                                                    jobject bind_info,
                                                                                    jobject bind_data) {

	if (bind_data == nullptr) {
		return;
	}

	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	auto bind_data_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, bind_data));
	if (bind_data_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified bind data");
		return;
	}

	duckdb_bind_set_bind_data(bi, bind_data_holder.release(), GlobalRefHolder::destroy);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_set_cardinality
 * Signature: (Ljava/nio/ByteBuffer;JZ)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1cardinality(JNIEnv *env, jclass,
                                                                                     jobject bind_info,
                                                                                     jlong cardinality,
                                                                                     jboolean is_exact) {
	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	idx_t cardinality_idx = jlong_to_idx(env, cardinality);
	if (env->ExceptionCheck()) {
		return;
	}
	bool exact = !!is_exact;

	duckdb_bind_set_cardinality(bi, cardinality_idx, exact);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1error(JNIEnv *env, jclass, jobject bind_info,
                                                                               jbyteArray error) {
	duckdb_bind_info bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_bind_set_error(bi, error_str.c_str());
}

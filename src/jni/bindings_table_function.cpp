#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "udf_registration.hpp"
#include "util.hpp"

static duckdb_table_function table_function_buf_to_table_function(JNIEnv *env, jobject table_function_buf) {
	if (table_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function buffer");
		return nullptr;
	}
	auto table_function = reinterpret_cast<duckdb_table_function>(env->GetDirectBufferAddress(table_function_buf));
	if (table_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function");
		return nullptr;
	}
	return table_function;
}

static duckdb_bind_info bind_info_buf_to_bind_info(JNIEnv *env, jobject bind_info_buf) {
	if (bind_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid bind info buffer");
		return nullptr;
	}
	auto bind_info = reinterpret_cast<duckdb_bind_info>(env->GetDirectBufferAddress(bind_info_buf));
	if (bind_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid bind info");
		return nullptr;
	}
	return bind_info;
}

static duckdb_init_info init_info_buf_to_init_info(JNIEnv *env, jobject init_info_buf) {
	if (init_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid init info buffer");
		return nullptr;
	}
	auto init_info = reinterpret_cast<duckdb_init_info>(env->GetDirectBufferAddress(init_info_buf));
	if (init_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid init info");
		return nullptr;
	}
	return init_info;
}

static duckdb_function_info function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info buffer");
		return nullptr;
	}
	auto function_info = reinterpret_cast<duckdb_function_info>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info");
		return nullptr;
	}
	return function_info;
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1table_1function(JNIEnv *env, jclass) {
	return make_ptr_buf(env, duckdb_create_table_function());
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1table_1function(JNIEnv *env, jclass,
                                                                                       jobject table_function) {
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_destroy_table_function(&tf);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1name(JNIEnv *env, jclass,
                                                                                         jobject table_function,
                                                                                         jbyteArray name) {
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto name_string = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_table_function_set_name(tf, name_string.c_str());
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1add_1parameter(JNIEnv *env, jclass,
                                                                                              jobject table_function,
                                                                                              jobject logical_type) {
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_table_function_add_parameter(tf, lt);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1supports_1projection_1pushdown(
    JNIEnv *env, jclass, jobject table_function, jboolean pushdown) {
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_table_function_supports_projection_pushdown(tf, pushdown);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1table_1function(JNIEnv *env, jclass,
                                                                                        jobject connection,
                                                                                        jobject table_function) {
	auto conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return -1;
	}
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return -1;
	}
	return static_cast<jint>(duckdb_register_table_function(conn, tf));
}

extern "C" JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1table_1function_1java(
    JNIEnv *env, jclass, jobject connection, jbyteArray name, jobject callback, jobjectArray parameter_logical_types,
    jboolean supports_projection_pushdown, jint max_threads, jboolean thread_safe) {
	try {
		_duckdb_jdbc_register_table_function(env, nullptr, connection, name, callback, parameter_logical_types,
		                                     supports_projection_pushdown, max_threads, thread_safe);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}

extern "C" JNIEXPORT void JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1register_1table_1function_1java_1with_1function(
    JNIEnv *env, jclass, jobject connection, jobject table_function, jobject callback,
    jobjectArray parameter_logical_types, jint max_threads, jboolean thread_safe) {
	try {
		_duckdb_jdbc_register_table_function_on_function(env, nullptr, connection, table_function, callback,
		                                                 parameter_logical_types, max_threads, thread_safe);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter_1count(JNIEnv *env, jclass,
                                                                                           jobject bind_info_buf) {
	auto info = bind_info_buf_to_bind_info(env, bind_info_buf);
	if (env->ExceptionCheck()) {
		return -1;
	}
	return static_cast<jlong>(duckdb_bind_get_parameter_count(info));
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter(JNIEnv *env, jclass,
                                                                                      jobject bind_info_buf,
                                                                                      jlong index) {
	auto info = bind_info_buf_to_bind_info(env, bind_info_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	auto idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	auto value = duckdb_bind_get_parameter(info, idx);
	return make_ptr_buf(env, value);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1add_1result_1column(JNIEnv *env, jclass,
                                                                                        jobject bind_info_buf,
                                                                                        jbyteArray name,
                                                                                        jobject logical_type) {
	auto info = bind_info_buf_to_bind_info(env, bind_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	auto name_string = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_bind_add_result_column(info, name_string.c_str(), lt);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1bind_1data(JNIEnv *env, jclass,
                                                                                    jobject bind_info_buf,
                                                                                    jobject bind_data_buf) {
	auto info = bind_info_buf_to_bind_info(env, bind_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (bind_data_buf == nullptr) {
		duckdb_bind_set_bind_data(info, nullptr, nullptr);
		return;
	}
	void *bind_data = env->GetDirectBufferAddress(bind_data_buf);
	duckdb_bind_set_bind_data(info, bind_data, nullptr);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1error(JNIEnv *env, jclass,
                                                                               jobject bind_info_buf,
                                                                               jbyteArray error) {
	auto info = bind_info_buf_to_bind_info(env, bind_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	auto error_string = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_bind_set_error(info, error_string.c_str());
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1init_1data(JNIEnv *env, jclass,
                                                                                    jobject init_info_buf,
                                                                                    jobject init_data_buf) {
	auto info = init_info_buf_to_init_info(env, init_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (init_data_buf == nullptr) {
		duckdb_init_set_init_data(info, nullptr, nullptr);
		return;
	}
	void *init_data = env->GetDirectBufferAddress(init_data_buf);
	duckdb_init_set_init_data(info, init_data, nullptr);
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1column_1count(JNIEnv *env, jclass,
                                                                                        jobject init_info_buf) {
	auto info = init_info_buf_to_init_info(env, init_info_buf);
	if (env->ExceptionCheck()) {
		return -1;
	}
	return static_cast<jlong>(duckdb_init_get_column_count(info));
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1get_1column_1index(JNIEnv *env, jclass,
                                                                                        jobject init_info_buf,
                                                                                        jlong column_index) {
	auto info = init_info_buf_to_init_info(env, init_info_buf);
	if (env->ExceptionCheck()) {
		return -1;
	}
	auto idx = jlong_to_idx(env, column_index);
	if (env->ExceptionCheck()) {
		return -1;
	}
	return static_cast<jlong>(duckdb_init_get_column_index(info, idx));
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1max_1threads(JNIEnv *env, jclass,
                                                                                      jobject init_info_buf,
                                                                                      jlong max_threads) {
	auto info = init_info_buf_to_init_info(env, init_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_init_set_max_threads(info, static_cast<idx_t>(max_threads));
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1init_1set_1error(JNIEnv *env, jclass,
                                                                               jobject init_info_buf,
                                                                               jbyteArray error) {
	auto info = init_info_buf_to_init_info(env, init_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	auto error_string = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_init_set_error(info, error_string.c_str());
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1bind_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info_buf) {
	auto info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	return make_ptr_buf(env, duckdb_function_get_bind_data(info));
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1init_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info_buf) {
	auto info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	return make_ptr_buf(env, duckdb_function_get_init_data(info));
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1local_1init_1data(
    JNIEnv *env, jclass, jobject function_info_buf) {
	auto info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	return make_ptr_buf(env, duckdb_function_get_local_init_data(info));
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1set_1error(JNIEnv *env, jclass,
                                                                                   jobject function_info_buf,
                                                                                   jbyteArray error) {
	auto info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	auto error_string = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_function_set_error(info, error_string.c_str());
}

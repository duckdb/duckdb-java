#include "bindings.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "scalar_functions.hpp"
#include "util.hpp"

static duckdb_scalar_function scalar_function_buf_to_scalar_function(JNIEnv *env, jobject scalar_function_buf) {

	if (scalar_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function buffer");
		return nullptr;
	}

	duckdb_scalar_function scalar_function =
	    reinterpret_cast<duckdb_scalar_function>(env->GetDirectBufferAddress(scalar_function_buf));
	if (scalar_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function");
		return nullptr;
	}

	return scalar_function;
}

static duckdb_function_info function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info buffer");
		return nullptr;
	}

	auto function_info = reinterpret_cast<duckdb_function_info>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info");
		return nullptr;
	}
	return function_info;
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1scalar_1function(JNIEnv *env, jclass) {
	return make_ptr_buf(env, duckdb_create_scalar_function());
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1scalar_1function(JNIEnv *env, jclass,
                                                                                        jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_destroy_scalar_function(&function);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1name(JNIEnv *env, jclass,
                                                                                          jobject scalar_function,
                                                                                          jbyteArray name) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function name");
		return;
	}
	auto function_name = jbyteArray_to_string(env, name);
	duckdb_scalar_function_set_name(function, function_name.c_str());
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1add_1parameter(JNIEnv *env, jclass,
                                                                                               jobject scalar_function,
                                                                                               jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_add_parameter(function, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1return_1type(
    JNIEnv *env, jclass, jobject scalar_function, jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_return_type(function, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1varargs(JNIEnv *env, jclass,
                                                                                             jobject scalar_function,
                                                                                             jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_varargs(function, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1volatile(JNIEnv *env, jclass,
                                                                                              jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_volatile(function);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1special_1handling(
    JNIEnv *env, jclass, jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_special_handling(function);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1scalar_1function(JNIEnv *env, jclass,
                                                                                         jobject connection,
                                                                                         jobject scalar_function) {
	auto conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(DuckDBError);
	}
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(DuckDBError);
	}
	return static_cast<jint>(duckdb_register_scalar_function(conn, function));
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1function(
    JNIEnv *env, jclass, jobject scalar_function_buf, jobject function_j) {
	try {
		scalar_function_set_function(env, scalar_function_buf, function_j);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1error(JNIEnv *env, jclass,
                                                                                           jobject function_info_buf,
                                                                                           jbyteArray error) {
	auto function_info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (error == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function error");
		return;
	}
	auto error_message = jbyteArray_to_string(env, error);
	duckdb_scalar_function_set_error(function_info, error_message.c_str());
}

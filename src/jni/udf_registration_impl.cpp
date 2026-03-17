extern "C" {
#include "duckdb.h"
}

#include "duckdb.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "udf_callbacks.hpp"
#include "udf_registration_internal.hpp"
#include "udf_types.hpp"
#include "util.hpp"

#include <string>
#include <vector>

using namespace duckdb;

static JavaVM *resolve_java_vm(JNIEnv *env) {
	JavaVM *vm = nullptr;
	if (env->GetJavaVM(&vm) != JNI_OK || vm == nullptr) {
		env->ThrowNew(J_SQLException, "Failed to resolve JavaVM for UDF registration");
		return nullptr;
	}
	return vm;
}

static duckdb_scalar_function scalar_function_buf_to_scalar_function(JNIEnv *env, jobject scalar_function_buf) {
	if (scalar_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function buffer");
		return nullptr;
	}

	auto scalar_function = reinterpret_cast<duckdb_scalar_function>(env->GetDirectBufferAddress(scalar_function_buf));
	if (scalar_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function");
		return nullptr;
	}

	return scalar_function;
}

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

static void register_scalar_udf_on_function(JNIEnv *env, duckdb_connection conn, duckdb_scalar_function scalar_function,
                                            jobject callback, jobjectArray argument_logical_types_j,
                                            jobject return_logical_type_j, jboolean return_null_on_exception,
                                            jboolean var_args, JavaVM *vm) {
	if (argument_logical_types_j == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null argument types");
		return;
	}
	auto arg_count = env->GetArrayLength(argument_logical_types_j);
	if (arg_count < 0) {
		env->ThrowNew(J_SQLException, "Invalid scalar UDF argument count");
		return;
	}
	if (var_args && arg_count != 1) {
		env->ThrowNew(J_SQLException, "Scalar UDF varargs registration expects exactly one argument logical type");
		return;
	}

	std::vector<duckdb_logical_type> arg_types;
	arg_types.reserve(static_cast<size_t>(arg_count));
	std::vector<duckdb_type> arg_type_tags;
	arg_type_tags.reserve(static_cast<size_t>(arg_count));
	auto destroy_arg_types = [&arg_types]() {
		for (auto &arg_type : arg_types) {
			duckdb_destroy_logical_type(&arg_type);
		}
	};
	for (jsize i = 0; i < arg_count; i++) {
		auto argument_type_obj = env->GetObjectArrayElement(argument_logical_types_j, i);
		if (env->ExceptionCheck() || !argument_type_obj) {
			destroy_arg_types();
			env->ThrowNew(J_SQLException, "Invalid scalar UDF argument logical type descriptor");
			return;
		}

		std::string logical_type_error;
		auto arg_logical_type = create_table_logical_type_from_java(env, argument_type_obj, logical_type_error);
		delete_local_ref(env, argument_type_obj);
		if (env->ExceptionCheck() || !arg_logical_type) {
			destroy_arg_types();
			if (logical_type_error.empty()) {
				logical_type_error = "Unsupported scalar UDF argument logical type";
			}
			env->ThrowNew(J_SQLException, logical_type_error.c_str());
			return;
		}

		auto arg_type_id = duckdb_get_type_id(arg_logical_type);
		if (!is_supported_scalar_udf_type(arg_type_id)) {
			duckdb_destroy_logical_type(&arg_logical_type);
			destroy_arg_types();
			env->ThrowNew(J_SQLException, UNSUPPORTED_SCALAR_UDF_TYPE_ERROR);
			return;
		}
		arg_types.push_back(arg_logical_type);
		arg_type_tags.push_back(arg_type_id);
	}

	if (return_logical_type_j == nullptr) {
		destroy_arg_types();
		env->ThrowNew(J_SQLException, "Invalid null return type");
		return;
	}

	std::string return_type_error;
	auto return_type = create_table_logical_type_from_java(env, return_logical_type_j, return_type_error);
	if (env->ExceptionCheck() || !return_type) {
		destroy_arg_types();
		if (return_type_error.empty()) {
			return_type_error = "Unsupported scalar UDF return logical type";
		}
		env->ThrowNew(J_SQLException, return_type_error.c_str());
		return;
	}
	auto return_type_tag = duckdb_get_type_id(return_type);
	if (!is_supported_scalar_udf_type(return_type_tag)) {
		destroy_arg_types();
		duckdb_destroy_logical_type(&return_type);
		env->ThrowNew(J_SQLException, UNSUPPORTED_SCALAR_UDF_TYPE_ERROR);
		return;
	}

	auto callback_data = new JavaScalarUdfCallbackData();
	callback_data->vm = vm;
	callback_data->callback_ref = env->NewGlobalRef(callback);
	callback_data->return_null_on_exception = return_null_on_exception;
	callback_data->var_args = var_args;
	callback_data->argument_types = std::move(arg_type_tags);
	callback_data->var_args_type =
	    callback_data->argument_types.empty() ? DUCKDB_TYPE_INVALID : callback_data->argument_types[0];
	callback_data->return_type = return_type_tag;
	if (!callback_data->callback_ref) {
		delete callback_data;
		destroy_arg_types();
		duckdb_destroy_logical_type(&return_type);
		throw InvalidInputException("Failed to create global ref for Java scalar UDF callback");
	}

	if (var_args) {
		duckdb_scalar_function_set_varargs(scalar_function, arg_types[0]);
	} else {
		for (auto &arg_type : arg_types) {
			duckdb_scalar_function_add_parameter(scalar_function, arg_type);
		}
	}
	duckdb_scalar_function_set_return_type(scalar_function, return_type);
	duckdb_scalar_function_set_extra_info(scalar_function, callback_data, destroy_java_scalar_udf_callback_data);
	duckdb_scalar_function_set_function(scalar_function, java_scalar_udf_callback);

	auto register_state = duckdb_register_scalar_function(conn, scalar_function);

	destroy_arg_types();
	duckdb_destroy_logical_type(&return_type);

	if (register_state != DuckDBSuccess) {
		throw InvalidInputException("Failed to register Java scalar UDF");
	}
}

static void register_table_function_on_function(JNIEnv *env, duckdb_connection conn, duckdb_table_function table_fn,
                                                jobject callback, jobjectArray parameter_types_j, jint max_threads,
                                                jboolean thread_safe, JavaVM *vm) {
	if (parameter_types_j == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null table function parameter types");
		return;
	}

	auto parameter_count = env->GetArrayLength(parameter_types_j);
	std::vector<duckdb_logical_type> parameter_logical_types;
	parameter_logical_types.reserve(static_cast<size_t>(parameter_count));
	for (jsize i = 0; i < parameter_count; i++) {
		auto parameter_type_obj = env->GetObjectArrayElement(parameter_types_j, i);
		if (env->ExceptionCheck() || !parameter_type_obj) {
			for (auto &parameter_logical_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&parameter_logical_type);
			}
			env->ThrowNew(J_SQLException, "Invalid table function parameter logical type descriptor");
			return;
		}
		std::string logical_type_error;
		auto parameter_logical_type = create_table_logical_type_from_java(env, parameter_type_obj, logical_type_error);
		delete_local_ref(env, parameter_type_obj);
		if (env->ExceptionCheck() || !parameter_logical_type) {
			for (auto &existing_parameter_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&existing_parameter_type);
			}
			if (logical_type_error.empty()) {
				logical_type_error = "Unsupported table function parameter logical type";
			}
			env->ThrowNew(J_SQLException, logical_type_error.c_str());
			return;
		}
		std::string support_error;
		if (!is_supported_table_bind_parameter_logical_type(parameter_logical_type, support_error)) {
			duckdb_destroy_logical_type(&parameter_logical_type);
			for (auto &existing_parameter_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&existing_parameter_type);
			}
			if (support_error.empty()) {
				support_error = UNSUPPORTED_TABLE_FUNCTION_PARAMETER_TYPE_ERROR;
			}
			env->ThrowNew(J_SQLException, support_error.c_str());
			return;
		}
		duckdb_table_function_add_parameter(table_fn, parameter_logical_type);
		parameter_logical_types.push_back(parameter_logical_type);
	}

	auto callback_data = new JavaTableFunctionCallbackData();
	callback_data->vm = vm;
	callback_data->callback_ref = env->NewGlobalRef(callback);
	callback_data->thread_safe = thread_safe;
	callback_data->max_threads = static_cast<idx_t>(max_threads < 1 ? 1 : max_threads);
	callback_data->parameter_logical_types = std::move(parameter_logical_types);
	if (!callback_data->callback_ref) {
		for (auto &parameter_logical_type : callback_data->parameter_logical_types) {
			duckdb_destroy_logical_type(&parameter_logical_type);
		}
		delete callback_data;
		throw InvalidInputException("Failed to create global ref for Java table function callback");
	}
	duckdb_table_function_set_extra_info(table_fn, callback_data, destroy_java_table_function_callback_data);
	duckdb_table_function_set_bind(table_fn, java_table_function_bind_callback);
	duckdb_table_function_set_init(table_fn, java_table_function_init_callback);
	duckdb_table_function_set_function(table_fn, java_table_function_main_callback);

	auto register_state = duckdb_register_table_function(conn, table_fn);
	if (register_state != DuckDBSuccess) {
		throw InvalidInputException("Failed to register Java table function");
	}
}

void duckdb_jdbc_register_scalar_udf_impl(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray name_j,
                                          jobject callback, jobjectArray argument_logical_types_j,
                                          jobject return_logical_type_j, jboolean special_handling,
                                          jboolean return_null_on_exception, jboolean deterministic,
                                          jboolean var_args) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto vm = resolve_java_vm(env);
	if (env->ExceptionCheck() || vm == nullptr) {
		return;
	}

	auto udf_name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}

	auto scalar_function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(scalar_function, udf_name.c_str());
	if (special_handling) {
		duckdb_scalar_function_set_special_handling(scalar_function);
	}
	if (!deterministic) {
		duckdb_scalar_function_set_volatile(scalar_function);
	}
	try {
		register_scalar_udf_on_function(env, conn, scalar_function, callback, argument_logical_types_j,
		                                return_logical_type_j, return_null_on_exception, var_args, vm);
	} catch (...) {
		duckdb_destroy_scalar_function(&scalar_function);
		throw;
	}
	duckdb_destroy_scalar_function(&scalar_function);
}

void duckdb_jdbc_register_scalar_udf_on_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf,
                                                      jobject scalar_function_buf, jobject callback,
                                                      jobjectArray argument_logical_types_j,
                                                      jobject return_logical_type_j, jboolean return_null_on_exception,
                                                      jboolean var_args) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto vm = resolve_java_vm(env);
	if (env->ExceptionCheck() || vm == nullptr) {
		return;
	}
	auto scalar_function = scalar_function_buf_to_scalar_function(env, scalar_function_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	register_scalar_udf_on_function(env, conn, scalar_function, callback, argument_logical_types_j,
	                                return_logical_type_j, return_null_on_exception, var_args, vm);
}

void duckdb_jdbc_register_table_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray name_j,
                                              jobject callback, jobjectArray parameter_types_j,
                                              jboolean supports_projection_pushdown, jint max_threads,
                                              jboolean thread_safe) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto vm = resolve_java_vm(env);
	if (env->ExceptionCheck() || vm == nullptr) {
		return;
	}
	auto fn_name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}
	auto table_fn = duckdb_create_table_function();
	duckdb_table_function_set_name(table_fn, fn_name.c_str());
	if (supports_projection_pushdown) {
		duckdb_table_function_supports_projection_pushdown(table_fn, true);
	}
	try {
		register_table_function_on_function(env, conn, table_fn, callback, parameter_types_j, max_threads, thread_safe,
		                                    vm);
	} catch (...) {
		duckdb_destroy_table_function(&table_fn);
		throw;
	}
	duckdb_destroy_table_function(&table_fn);
	if (env->ExceptionCheck()) {
		return;
	}
}

void duckdb_jdbc_register_table_function_on_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf,
                                                          jobject table_function_buf, jobject callback,
                                                          jobjectArray parameter_types_j, jint max_threads,
                                                          jboolean thread_safe) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto vm = resolve_java_vm(env);
	if (env->ExceptionCheck() || vm == nullptr) {
		return;
	}
	auto table_fn = table_function_buf_to_table_function(env, table_function_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	register_table_function_on_function(env, conn, table_fn, callback, parameter_types_j, max_threads, thread_safe, vm);
}

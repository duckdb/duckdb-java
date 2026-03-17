extern "C" {
#include "duckdb.h"
}

#include "refs.hpp"
#include "udf_callbacks.hpp"
#include "udf_table_bind_conversion.hpp"
#include "udf_types.hpp"
#include "util.hpp"

#include <cstring>
#include <string>
#include <vector>

static jobject create_scalar_udf_input_reader(JNIEnv *env, duckdb_vector vector, duckdb_type type, idx_t row_count,
                                              jlong validity_size, std::vector<jobject> &local_refs) {
	auto spec = find_udf_type_spec(type);
	if (!spec || !spec->udf_vector_supported) {
		env->ThrowNew(J_SQLException, "Unsupported scalar UDF type");
		return nullptr;
	}

	auto validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(vector));
	auto validity_buf = validity_ptr ? env->NewDirectByteBuffer(validity_ptr, validity_size) : nullptr;
	if (validity_buf) {
		local_refs.push_back(validity_buf);
	}

	jobject data_buf = nullptr;
	jobject vector_ref_buf = nullptr;
	if (spec->requires_vector_ref) {
		vector_ref_buf = env->NewDirectByteBuffer(vector, 0);
		local_refs.push_back(vector_ref_buf);
	} else {
		auto data_ptr = duckdb_vector_get_data(vector);
		data_buf = env->NewDirectByteBuffer(
		    data_ptr, static_cast<jlong>(row_count * static_cast<idx_t>(spec->fixed_width_bytes)));
		local_refs.push_back(data_buf);
	}

	auto reader = env->NewObject(J_UdfNativeReader, J_UdfNativeReader_init, static_cast<jint>(type), data_buf,
	                             vector_ref_buf, validity_buf, static_cast<jint>(row_count));
	local_refs.push_back(reader);
	return reader;
}

static jobject create_scalar_udf_output_writer(JNIEnv *env, duckdb_vector vector, duckdb_type type, idx_t row_count,
                                               jlong validity_size, std::vector<jobject> &local_refs) {
	auto spec = find_udf_type_spec(type);
	if (!spec || !spec->udf_vector_supported) {
		env->ThrowNew(J_SQLException, "Unsupported scalar UDF output type");
		return nullptr;
	}

	auto validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(vector));
	auto validity_buf = validity_ptr ? env->NewDirectByteBuffer(validity_ptr, validity_size) : nullptr;
	if (validity_buf) {
		local_refs.push_back(validity_buf);
	}

	jobject data_buf = nullptr;
	jobject vector_ref_buf = nullptr;
	if (spec->requires_vector_ref) {
		vector_ref_buf = env->NewDirectByteBuffer(vector, 0);
		local_refs.push_back(vector_ref_buf);
	} else {
		auto data_ptr = duckdb_vector_get_data(vector);
		data_buf = env->NewDirectByteBuffer(
		    data_ptr, static_cast<jlong>(row_count * static_cast<idx_t>(spec->fixed_width_bytes)));
		local_refs.push_back(data_buf);
	}

	auto writer = env->NewObject(J_UdfScalarWriter, J_UdfScalarWriter_init, static_cast<jint>(type), data_buf,
	                             vector_ref_buf, validity_buf, static_cast<jint>(row_count));
	local_refs.push_back(writer);
	return writer;
}

void destroy_java_scalar_udf_callback_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaScalarUdfCallbackData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->callback_ref) {
		delete_global_ref(env, data->callback_ref);
	}
	delete data;
}

void destroy_java_table_function_callback_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionCallbackData *>(ptr);
	for (auto &logical_type : data->parameter_logical_types) {
		if (logical_type) {
			duckdb_destroy_logical_type(&logical_type);
		}
	}
	data->parameter_logical_types.clear();
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->callback_ref) {
		delete_global_ref(env, data->callback_ref);
	}
	delete data;
}

void destroy_java_table_function_bind_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionBindData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->bind_result_ref) {
		delete_global_ref(env, data->bind_result_ref);
	}
	delete data;
}

void destroy_java_table_function_init_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionInitData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->state_ref) {
		delete_global_ref(env, data->state_ref);
	}
	delete data;
}

void java_scalar_udf_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	auto data = reinterpret_cast<JavaScalarUdfCallbackData *>(duckdb_scalar_function_get_extra_info(info));
	if (!data) {
		duckdb_scalar_function_set_error(info, "Missing callback state for Java scalar UDF");
		return;
	}

	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_scalar_function_set_error(info, "Failed to acquire JNIEnv for Java scalar UDF callback");
		return;
	}

	auto row_count = duckdb_data_chunk_get_size(input);
	auto arg_count = duckdb_data_chunk_get_column_count(input);
	if ((!data->var_args && data->argument_types.size() != arg_count) ||
	    (data->var_args && data->argument_types.size() != 1)) {
		duckdb_scalar_function_set_error(info, "Scalar UDF argument mismatch");
		return;
	}
	duckdb_vector_ensure_validity_writable(output);
	auto output_validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(output));

	auto validity_size = static_cast<jlong>(((row_count + 63) / 64) * sizeof(uint64_t));
	auto args = env->NewObjectArray(arg_count, J_UdfReader, nullptr);
	std::vector<jobject> local_refs;
	local_refs.push_back(args);
	for (idx_t arg_idx = 0; arg_idx < arg_count; arg_idx++) {
		auto input_vector = duckdb_data_chunk_get_vector(input, arg_idx);
		auto argument_type = data->var_args ? data->var_args_type : data->argument_types[arg_idx];
		auto input_reader =
		    create_scalar_udf_input_reader(env, input_vector, argument_type, row_count, validity_size, local_refs);
		env->SetObjectArrayElement(args, static_cast<jsize>(arg_idx), input_reader);
	}
	if (env->ExceptionCheck()) {
		duckdb_scalar_function_set_error(info, "Failed to materialize scalar UDF input readers");
		delete_local_refs(env, local_refs);
		return;
	}
	auto output_writer =
	    create_scalar_udf_output_writer(env, output, data->return_type, row_count, validity_size, local_refs);
	if (env->ExceptionCheck()) {
		duckdb_scalar_function_set_error(info, "Failed to materialize scalar UDF output writer");
		delete_local_refs(env, local_refs);
		return;
	}

	env->CallVoidMethod(data->callback_ref, J_ScalarUdf_apply, nullptr, args, output_writer,
	                    static_cast<jint>(row_count));

	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		if (data->return_null_on_exception) {
			if (output_validity_ptr) {
				std::memset(output_validity_ptr, 0, static_cast<size_t>(validity_size));
			}
			if (exception) {
				delete_local_ref(env, exception);
			}
		} else {
			std::string error = "Exception in Java scalar UDF callback";
			if (exception) {
				auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
				if (message != nullptr) {
					error = jstring_to_string(env, message);
					delete_local_ref(env, message);
				}
				delete_local_ref(env, exception);
			}
			duckdb_scalar_function_set_error(info, error.c_str());
		}
	}

	delete_local_refs(env, local_refs);
}

void java_table_function_bind_callback(duckdb_bind_info info) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_bind_get_extra_info(info));
	if (!callback_data) {
		duckdb_bind_set_error(info, "Missing callback state for Java table function");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_bind_set_error(info, "Failed to acquire JNIEnv for Java table bind callback");
		return;
	}

	auto parameter_count = duckdb_bind_get_parameter_count(info);
	if (callback_data->parameter_logical_types.size() != parameter_count) {
		duckdb_bind_set_error(info, "Table function parameter count mismatch");
		return;
	}
	auto parameters = env->NewObjectArray(static_cast<jsize>(parameter_count), J_Object, nullptr);
	std::vector<jobject> bind_local_refs;
	bind_local_refs.push_back(parameters);
	for (idx_t i = 0; i < parameter_count; i++) {
		auto val = duckdb_bind_get_parameter(info, i);
		std::string parameter_error;
		auto param_obj = table_bind_parameter_to_java(env, val, callback_data->parameter_logical_types[i],
		                                              bind_local_refs, parameter_error);
		duckdb_destroy_value(&val);
		if ((!parameter_error.empty() && param_obj == nullptr) || env->ExceptionCheck()) {
			if (parameter_error.empty()) {
				parameter_error = "Failed to materialize table function bind parameter";
			}
			duckdb_bind_set_error(info, parameter_error.c_str());
			delete_local_refs(env, bind_local_refs);
			return;
		}
		env->SetObjectArrayElement(parameters, static_cast<jsize>(i), param_obj);
		if (env->ExceptionCheck()) {
			duckdb_bind_set_error(info, "Failed to pass table function bind parameters to Java");
			delete_local_refs(env, bind_local_refs);
			return;
		}
	}

	auto bind_result = env->CallObjectMethod(callback_data->callback_ref, J_TableFunction_bind, nullptr, parameters);
	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		std::string error = "Exception in Java table function bind callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
		}
		duckdb_bind_set_error(info, error.c_str());
		delete_local_refs(env, bind_local_refs);
		return;
	}
	if (bind_result == nullptr) {
		duckdb_bind_set_error(info, "Java table function bind returned null");
		delete_local_refs(env, bind_local_refs);
		return;
	}

	auto column_names =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnNames));
	auto column_types =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnTypes));
	auto column_logical_types =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnLogicalTypes));
	if (env->ExceptionCheck() || column_names == nullptr || column_types == nullptr) {
		duckdb_bind_set_error(info, "Invalid Java table bind result");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	auto name_count = env->GetArrayLength(column_names);
	auto type_count = env->GetArrayLength(column_types);
	if (name_count != type_count) {
		duckdb_bind_set_error(info, "Java table bind result has mismatched schema lengths");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	if (column_logical_types != nullptr && env->GetArrayLength(column_logical_types) != name_count) {
		duckdb_bind_set_error(info, "Java table bind result has mismatched logical schema lengths");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	for (jsize i = 0; i < name_count; i++) {
		auto name_j = reinterpret_cast<jstring>(env->GetObjectArrayElement(column_names, i));
		auto type_obj = env->GetObjectArrayElement(column_types, i);
		if (!name_j || !type_obj) {
			delete_local_ref(env, name_j);
			delete_local_ref(env, type_obj);
			duckdb_bind_set_error(info, "Unsupported column descriptor in Java table bind result");
			delete_local_ref(env, column_names);
			delete_local_ref(env, column_types);
			delete_local_ref(env, column_logical_types);
			delete_local_ref(env, bind_result);
			delete_local_refs(env, bind_local_refs);
			return;
		}
		auto name = jstring_to_string(env, name_j);
		duckdb_logical_type logical_type = nullptr;
		if (column_logical_types != nullptr) {
			auto logical_type_obj = env->GetObjectArrayElement(column_logical_types, i);
			std::string logical_error;
			logical_type = create_table_logical_type_from_java(env, logical_type_obj, logical_error);
			delete_local_ref(env, logical_type_obj);
			if (env->ExceptionCheck() || !logical_type) {
				if (logical_error.empty()) {
					logical_error = "Unsupported logical type in Java table bind result";
				}
				duckdb_bind_set_error(info, logical_error.c_str());
				delete_local_ref(env, name_j);
				delete_local_ref(env, type_obj);
				delete_local_ref(env, column_names);
				delete_local_ref(env, column_types);
				delete_local_ref(env, column_logical_types);
				delete_local_ref(env, bind_result);
				delete_local_refs(env, bind_local_refs);
				return;
			}
		} else {
			duckdb_type duck_type = DUCKDB_TYPE_INVALID;
			if (!table_column_type_from_java(env, type_obj, duck_type)) {
				duckdb_bind_set_error(info, "Unsupported column type in Java table bind result");
				delete_local_ref(env, name_j);
				delete_local_ref(env, type_obj);
				delete_local_ref(env, column_names);
				delete_local_ref(env, column_types);
				delete_local_ref(env, bind_result);
				delete_local_refs(env, bind_local_refs);
				return;
			}
			logical_type = create_udf_logical_type(duck_type);
		}
		duckdb_bind_add_result_column(info, name.c_str(), logical_type);
		duckdb_destroy_logical_type(&logical_type);
		delete_local_ref(env, name_j);
		delete_local_ref(env, type_obj);
	}

	auto bind_data = new JavaTableFunctionBindData();
	bind_data->vm = callback_data->vm;
	bind_data->bind_result_ref = env->NewGlobalRef(bind_result);
	if (!bind_data->bind_result_ref) {
		delete bind_data;
		duckdb_bind_set_error(info, "Failed to create global ref for Java table bind state");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	duckdb_bind_set_bind_data(info, bind_data, destroy_java_table_function_bind_data);

	delete_local_ref(env, column_names);
	delete_local_ref(env, column_types);
	delete_local_ref(env, column_logical_types);
	delete_local_ref(env, bind_result);
	delete_local_refs(env, bind_local_refs);
}

void java_table_function_init_callback(duckdb_init_info info) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_init_get_extra_info(info));
	auto bind_data = reinterpret_cast<JavaTableFunctionBindData *>(duckdb_init_get_bind_data(info));
	if (!callback_data || !bind_data) {
		duckdb_init_set_error(info, "Missing callback/bind state for Java table function init");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_init_set_error(info, "Failed to acquire JNIEnv for Java table init callback");
		return;
	}

	auto projected_column_count = duckdb_init_get_column_count(info);
	auto projected_column_indexes = env->NewIntArray(static_cast<jsize>(projected_column_count));
	if (!projected_column_indexes) {
		duckdb_init_set_error(info, "Failed to allocate projected column index array");
		return;
	}
	std::vector<jint> projected_columns;
	projected_columns.reserve(projected_column_count);
	for (idx_t i = 0; i < projected_column_count; i++) {
		projected_columns.push_back(static_cast<jint>(duckdb_init_get_column_index(info, i)));
	}
	if (!projected_columns.empty()) {
		env->SetIntArrayRegion(projected_column_indexes, 0, static_cast<jsize>(projected_columns.size()),
		                       projected_columns.data());
	}
	auto init_ctx = env->NewObject(J_TableInitContext, J_TableInitContext_init, projected_column_indexes);
	delete_local_ref(env, projected_column_indexes);
	if (env->ExceptionCheck() || !init_ctx) {
		duckdb_init_set_error(info, "Failed to construct Java table init context");
		return;
	}

	auto state =
	    env->CallObjectMethod(callback_data->callback_ref, J_TableFunction_init, init_ctx, bind_data->bind_result_ref);
	delete_local_ref(env, init_ctx);
	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		std::string error = "Exception in Java table function init callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
		}
		duckdb_init_set_error(info, error.c_str());
		return;
	}
	if (state == nullptr) {
		duckdb_init_set_error(info, "Java table function init returned null");
		return;
	}
	auto init_data = new JavaTableFunctionInitData();
	init_data->vm = callback_data->vm;
	init_data->state_ref = env->NewGlobalRef(state);
	if (!init_data->state_ref) {
		delete init_data;
		duckdb_init_set_error(info, "Failed to create global ref for Java table init state");
		delete_local_ref(env, state);
		return;
	}
	duckdb_init_set_init_data(info, init_data, destroy_java_table_function_init_data);
	if (callback_data->thread_safe) {
		duckdb_init_set_max_threads(info, callback_data->max_threads < 1 ? 1 : callback_data->max_threads);
	} else {
		duckdb_init_set_max_threads(info, 1);
	}

	delete_local_ref(env, state);
}

void java_table_function_main_callback(duckdb_function_info info, duckdb_data_chunk output) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_function_get_extra_info(info));
	auto init_data = reinterpret_cast<JavaTableFunctionInitData *>(duckdb_function_get_init_data(info));
	if (!callback_data || !init_data) {
		duckdb_function_set_error(info, "Missing callback/init state for Java table function");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_function_set_error(info, "Failed to acquire JNIEnv for Java table function callback");
		return;
	}

	auto row_capacity = duckdb_vector_size();
	auto output_ref = env->NewDirectByteBuffer(output, 0);
	if (!output_ref || env->ExceptionCheck()) {
		if (env->ExceptionCheck()) {
			env->ExceptionClear();
		}
		duckdb_function_set_error(info, "Failed to materialize Java table function output chunk");
		return;
	}

	auto out_appender = env->NewObject(J_UdfOutputAppender, J_UdfOutputAppender_init, output_ref);
	if (!out_appender || env->ExceptionCheck()) {
		if (env->ExceptionCheck()) {
			env->ExceptionClear();
		}
		delete_local_ref(env, output_ref);
		duckdb_function_set_error(info, "Failed to initialize Java table function output appender");
		return;
	}

	auto produced =
	    env->CallIntMethod(callback_data->callback_ref, J_TableFunction_produce, init_data->state_ref, out_appender);

	jthrowable callback_exception = nullptr;
	if (env->ExceptionCheck()) {
		callback_exception = env->ExceptionOccurred();
		env->ExceptionClear();
	}

	env->CallVoidMethod(out_appender, J_UdfOutputAppender_close);
	jthrowable close_exception = nullptr;
	if (env->ExceptionCheck()) {
		close_exception = env->ExceptionOccurred();
		env->ExceptionClear();
	}

	if (callback_exception || close_exception) {
		auto exception = callback_exception ? callback_exception : close_exception;
		std::string error = "Exception in Java table function callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
			if (exception == close_exception) {
				close_exception = nullptr;
			}
		}
		duckdb_function_set_error(info, error.c_str());
	} else {
		if (produced < 0) {
			produced = 0;
		}
		if (produced > static_cast<jint>(row_capacity)) {
			produced = static_cast<jint>(row_capacity);
		}
		duckdb_data_chunk_set_size(output, static_cast<idx_t>(produced));
	}

	if (close_exception) {
		delete_local_ref(env, close_exception);
	}
	delete_local_ref(env, out_appender);
	delete_local_ref(env, output_ref);
}

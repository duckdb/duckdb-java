
#include "bindings_table_function.hpp"

#include "refs.hpp"
#include "util.hpp"

#include <algorithm>
#include <cctype>

/*
 * The table function as it is configured from Java.
 *
 * In C API v1 a table function is created without a connection, the C API v2 requires the function to be
 * created with a factory of the database it is going to be registered in. So the configuration is collected
 * here and the C API v2 function is created when the function is registered.
 */
struct TableFunctionSpec {
	std::string name;
	std::vector<spec_logical_type> parameters;
	std::vector<std::pair<std::string, spec_logical_type>> named_parameters;
	std::unique_ptr<GlobalRefHolder> callback;
	bool has_bind = false;
	bool has_init = false;
	bool has_local_init = false;
	bool has_function = false;
	bool projection_pushdown = false;
};

static bool names_equal_case_insensitive(const std::string &a, const std::string &b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char ca, char cb) {
		       return std::tolower(static_cast<unsigned char>(ca)) == std::tolower(static_cast<unsigned char>(cb));
	       });
}

static std::string quote_identifier(const std::string &name) {
	std::string res = "\"";
	for (char c : name) {
		if (c == '"') {
			res += '"';
		}
		res += c;
	}
	res += '"';
	return res;
}

static std::string value_to_sql_string(duckdb_v2_value_handle value) {
	bool is_null = false;
	duckdb_v2_value_is_null(value, &is_null, nullptr);
	if (is_null) {
		return "NULL";
	}
	idx_t length = 0;
	if (duckdb_v2_value_to_string(value, nullptr, 0, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	std::string text;
	text.resize(length);
	if (duckdb_v2_value_to_string(value, &text[0], length + 1, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	v2_logical_type type;
	DUCKDB_V2_LOGICAL_TYPE_ID type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	if (duckdb_v2_value_get_logical_type(value, type.out(), nullptr) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_get_id(type.get(), &type_id, nullptr);
	}
	if (type_id != DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR) {
		return text;
	}
	std::string res = "'";
	for (char c : text) {
		if (c == '\'') {
			res += '\'';
		}
		res += c;
	}
	res += '\'';
	return res;
}

static std::string logical_type_to_string(duckdb_v2_logical_type_handle type) {
	idx_t length = 0;
	if (duckdb_v2_logical_type_to_text(type, nullptr, 0, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	std::string text;
	text.resize(length);
	if (duckdb_v2_logical_type_to_text(type, &text[0], length + 1, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	return text;
}

static std::string cast_error_message(const v2_error &err) {
	std::string msg = err.raw_message();
	static const std::string prefix = "Failed to cast value: ";
	if (msg.compare(0, prefix.size(), prefix) == 0) {
		msg = msg.substr(prefix.size());
	}
	return msg;
}

static bool logical_types_equal(duckdb_v2_logical_type_handle a, duckdb_v2_logical_type_handle b) {
	bool equal = false;
	if (duckdb_v2_logical_type_is_equal(a, b, &equal, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	return equal;
}

/*
 * The named parameters of a C API v1 table function are optional and are cast to their declared types. In the C API
 * v2 the named parameters without a default value are required, so the named parameters are received with the
 * "**kwargs" parameter and are checked and cast here, the same way as it is done for C API v1 functions.
 * Returns false and sets the error on failure.
 */
static bool resolve_named_parameters(TableFunctionBindInfoHolder &info, duckdb_v2_error_info_handle *err) {
	idx_t positional_fixed = 0;
	idx_t positional_variadic = 0;
	idx_t named_fixed = 0;
	idx_t named_variadic = 0;
	v2_error call_err;
	if (duckdb_v2_function_bind_get_arg_count(info.info, &positional_fixed, &positional_variadic, &named_fixed,
	                                          &named_variadic, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return false;
	}
	info.positional_count = positional_fixed + positional_variadic;
	idx_t total = info.positional_count + named_fixed + named_variadic;
	auto &declared = info.user_data.named_parameters;
	for (idx_t i = info.positional_count; i < total; i++) {
		const char *name_ptr = nullptr;
		idx_t name_length = 0;
		if (duckdb_v2_function_bind_get_arg_name(info.info, i, &name_ptr, &name_length, call_err.out()) !=
		    DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return false;
		}
		std::string name(name_ptr, name_length);
		auto it =
		    std::find_if(declared.begin(), declared.end(), [&](const std::pair<std::string, spec_logical_type> &p) {
			    return names_equal_case_insensitive(p.first, name);
		    });
		if (it == declared.end()) {
			set_callback_error(err, DUCKDB_V2_ERROR_QUERY_BINDER,
			                   "Invalid named parameter " + quote_identifier(name) + " for function " +
			                       info.user_data.name);
			return false;
		}
		for (auto &passed : info.named_parameters) {
			if (names_equal_case_insensitive(passed.first, it->first)) {
				set_callback_error(err, DUCKDB_V2_ERROR_QUERY_BINDER,
				                   "Named parameter " + quote_identifier(it->first) +
				                       " was passed more than once in function call to " + info.user_data.name);
				return false;
			}
		}
		v2_value passed_value;
		v2_logical_type passed_type;
		if (duckdb_v2_function_bind_get_arg_value(info.info, i, passed_value.out(), call_err.out()) !=
		        DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_value_get_logical_type(passed_value.get(), passed_type.out(), call_err.out()) !=
		        DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return false;
		}
		if (logical_types_equal(passed_type.get(), it->second.get())) {
			info.named_parameters.emplace_back(it->first, std::move(passed_value));
			continue;
		}
		duckdb_v2_factory_handle factory = nullptr;
		v2_value value;
		if (duckdb_v2_context_get_factory(info.context, &factory, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return false;
		}
		if (duckdb_v2_value_cast(factory, passed_value.get(), it->second.get(), value.out(), call_err.out()) !=
		    DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID,
			                   "Could not cast value " + value_to_sql_string(passed_value.get()) +
			                       " to named parameter " + quote_identifier(it->first) + " of type " +
			                       logical_type_to_string(it->second.get()) + ": " + cast_error_message(call_err));
			return false;
		}
		info.named_parameters.emplace_back(it->first, std::move(value));
	}
	return true;
}

template <class INFO, class OBJ>
static bool call_table_function_java(GlobalRefHolder *callback_holder, jmethodID method, INFO *info,
                                     const char *info_buf_error, OBJ *output, std::string &error) {
	AttachedJNIEnv attached = callback_holder->attach_current_thread();
	if (attached.env == nullptr) {
		error = "Unable to attach JNI environment";
		return false;
	}
	jobject info_buf = make_ptr_buf(attached.env, info);
	if (info_buf == nullptr) {
		error = info_buf_error;
		return false;
	}
	LocalRefHolder info_buf_holder(attached.env, info_buf);
	if (output == nullptr) {
		attached.env->CallVoidMethod(callback_holder->global_ref, method, info_buf);
	} else {
		jobject output_buf = make_ptr_buf(attached.env, output);
		if (output_buf == nullptr) {
			error = "Unable to create output buffer";
			return false;
		}
		LocalRefHolder output_buf_holder(attached.env, output_buf);
		attached.env->CallVoidMethod(callback_holder->global_ref, method, info_buf, output_buf);
	}
	if (attached.env->ExceptionCheck()) {
		error = "Java callback system error";
		return false;
	}
	if (info->has_error) {
		error = info->error;
		return false;
	}
	return true;
}

static void table_function_bind(duckdb_v2_function_bind_info_handle info,
                                duckdb_v2_table_function_bind_info_handle result, duckdb_v2_context_handle context,
                                duckdb_v2_error_info_handle *err) {
	void *user_data_ptr = nullptr;
	if (duckdb_v2_function_bind_get_user_data(info, &user_data_ptr, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    user_data_ptr == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_QUERY_BINDER, "Table function callback not found");
		return;
	}
	auto &user_data = *reinterpret_cast<TableFunctionUserData *>(user_data_ptr);
	TableFunctionBindInfoHolder bind_info(info, result, context, user_data);
	if (!resolve_named_parameters(bind_info, err)) {
		return;
	}

	auto callback_holder = user_data.callback.get();
	if (callback_holder == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_QUERY_BINDER, "Table function callback not found");
		return;
	}
	std::string error;
	if (!call_table_function_java(callback_holder, J_DuckDBTableFunctionWrapper_executeBind, &bind_info,
	                              "Unable to create bind info buffer", static_cast<void *>(nullptr), error)) {
		// same as in C API v1, the bind errors are raised as BinderException
		set_callback_error(err, DUCKDB_V2_ERROR_QUERY_BINDER, error);
		return;
	}

	// the bind data is always set, it is empty when it was not set from Java
	GlobalRefHolder *bind_data = bind_info.bind_data ? bind_info.bind_data->release() : nullptr;
	duckdb_v2_opaque opaque = {new TableFunctionState(bind_data), TableFunctionState::destroy, nullptr};
	v2_error call_err;
	if (duckdb_v2_function_bind_set_bind_data(info, &opaque, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
	}
}

static void table_function_init(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                                duckdb_v2_error_info_handle *err) {
	void *user_data_ptr = nullptr;
	duckdb_v2_table_function_init_global_get_user_data(info, &user_data_ptr, nullptr);
	auto callback_holder =
	    user_data_ptr != nullptr ? reinterpret_cast<TableFunctionUserData *>(user_data_ptr)->callback.get() : nullptr;
	if (callback_holder == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Table function callback not found");
		return;
	}
	TableFunctionInitInfoHolder init_info;
	init_info.global_info = info;
	std::string error;
	if (!call_table_function_java(callback_holder, J_DuckDBTableFunctionWrapper_executeGlobalInit, &init_info,
	                              "Unable to create global init info buffer", static_cast<void *>(nullptr), error)) {
		// same as in C API v1, the init errors are raised as InvalidInputException
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, error);
		return;
	}

	// the init data is always set, it is empty when it was not set from Java
	GlobalRefHolder *init_data = init_info.init_data ? init_info.init_data->release() : nullptr;
	duckdb_v2_opaque opaque = {new TableFunctionState(init_data), TableFunctionState::destroy, nullptr};
	v2_error call_err;
	if (duckdb_v2_table_function_init_global_set_global_state(info, &opaque, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
	}
}

static void table_function_local_init(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle,
                                      duckdb_v2_error_info_handle *err) {
	void *user_data_ptr = nullptr;
	duckdb_v2_table_function_init_local_get_user_data(info, &user_data_ptr, nullptr);
	auto callback_holder =
	    user_data_ptr != nullptr ? reinterpret_cast<TableFunctionUserData *>(user_data_ptr)->callback.get() : nullptr;
	if (callback_holder == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Table function callback not found");
		return;
	}
	TableFunctionInitInfoHolder init_info;
	init_info.local_info = info;
	std::string error;
	if (!call_table_function_java(callback_holder, J_DuckDBTableFunctionWrapper_executeLocalInit, &init_info,
	                              "Unable to create local init info buffer", static_cast<void *>(nullptr), error)) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, error);
		return;
	}

	GlobalRefHolder *init_data = init_info.init_data ? init_info.init_data->release() : nullptr;
	duckdb_v2_opaque opaque = {new TableFunctionState(init_data), TableFunctionState::destroy, nullptr};
	v2_error call_err;
	if (duckdb_v2_table_function_init_local_set_local_state(info, &opaque, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
	}
}

static void table_function_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                                duckdb_v2_error_info_handle *err) {
	void *user_data_ptr = nullptr;
	duckdb_v2_table_function_exec_get_user_data(info, &user_data_ptr, nullptr);
	if (user_data_ptr == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Table function callback not found");
		return;
	}
	auto &user_data = *reinterpret_cast<TableFunctionUserData *>(user_data_ptr);
	auto callback_holder = user_data.callback.get();
	if (callback_holder == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Table function callback not found");
		return;
	}
	// the output chunk is borrowed
	duckdb_v2_data_chunk_handle output = nullptr;
	v2_error call_err;
	if (duckdb_v2_table_function_exec_get_output_chunk(info, &output, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	TableFunctionInfoHolder function_info(info, user_data.has_local_init);
	std::string error;
	if (!call_table_function_java(callback_holder, J_DuckDBTableFunctionWrapper_executeFunction, &function_info,
	                              "Unable to create function info buffer", output, error)) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, error);
	}
}

/*
 * Creates the C API v2 function from the specified configuration and registers it, the checks are the same as in
 * C API v1.
 */
static jint register_table_function(duckdb_v2_connection_handle conn, TableFunctionSpec &spec) {
	if (spec.name.empty() || !spec.has_bind || !spec.has_init || !spec.has_function) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	for (auto &param : spec.parameters) {
		if (!param.is_valid()) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}
	for (auto &param : spec.named_parameters) {
		if (!param.second.is_valid()) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}

	duckdb_v2_factory_handle factory = nullptr;
	if (duckdb_v2_connection_get_factory(conn, &factory, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	v2_table_function function;
	if (duckdb_v2_table_function_create(factory, function.out(), nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_name(function.get(), spec.name.data(), spec.name.size(), nullptr) !=
	        DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	duckdb_v2_function_signature_handle signature = nullptr;
	if (duckdb_v2_table_function_get_signature(function.get(), &signature, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	// C API v1 declares no parameter names, the parameters are positional-only and are named "colN"
	for (size_t i = 0; i < spec.parameters.size(); i++) {
		std::string param_name = "col" + std::to_string(i);
		if (duckdb_v2_function_signature_add_parameter(
		        signature, param_name.data(), param_name.size(), spec.parameters[i].get(), nullptr,
		        DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}
	// named parameters are checked and cast in bind, see resolve_named_parameters
	if (!spec.named_parameters.empty()) {
		v2_logical_type any_type;
		if (duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_ANY, nullptr, nullptr, nullptr, 0,
		                                          any_type.out(), nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_function_signature_add_parameter(signature, "options", 7, any_type.get(), nullptr,
		                                               DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC,
		                                               nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}
	if (duckdb_v2_table_function_set_bind_callback(function.get(), table_function_bind, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_init_global_callback(function.get(), table_function_init, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    (spec.has_local_init && duckdb_v2_table_function_set_init_local_callback(
	                                function.get(), table_function_local_init, nullptr) != DUCKDB_V2_ERROR_NONE) ||
	    duckdb_v2_table_function_set_exec_callback(function.get(), table_function_exec, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_projection_pushdown(function.get(), spec.projection_pushdown, nullptr) !=
	        DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	// same as the "extra info" in C API v1, the callback is owned by the registered function
	auto user_data = std::unique_ptr<TableFunctionUserData>(new TableFunctionUserData());
	user_data->name = spec.name;
	user_data->callback = std::move(spec.callback);
	for (auto &param : spec.named_parameters) {
		user_data->named_parameters.emplace_back(param.first, spec_logical_type(param.second.get()));
	}
	user_data->has_local_init = spec.has_local_init;
	duckdb_v2_opaque opaque = {user_data.get(),
	                           [](void *ptr) { delete reinterpret_cast<TableFunctionUserData *>(ptr); }, nullptr};
	if (duckdb_v2_table_function_set_user_data(function.get(), &opaque, nullptr) != DUCKDB_V2_ERROR_NONE) {
		spec.callback = std::move(user_data->callback);
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	// the function owns the user data now
	user_data.release();

	if (duckdb_v2_connection_register_table_function(conn, function.get(), nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	return static_cast<jint>(CAPI_STATE_SUCCESS);
}

static TableFunctionSpec *table_function_buf_to_table_function(JNIEnv *env, jobject table_function_buf) {
	if (table_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function buffer");
		return nullptr;
	}

	auto table_function = reinterpret_cast<TableFunctionSpec *>(env->GetDirectBufferAddress(table_function_buf));
	if (table_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function");
		return nullptr;
	}
	return table_function;
}

static TableFunctionInfoHolder *function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info buffer");
		return nullptr;
	}

	auto function_info = reinterpret_cast<TableFunctionInfoHolder *>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info");
		return nullptr;
	}
	return function_info;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_table_function
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1table_1function(JNIEnv *env, jclass) {

	auto tf = new TableFunctionSpec();
	jobject buf = make_ptr_buf(env, tf);
	if (buf == nullptr) {
		delete tf;
	}
	return buf;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_table_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1table_1function(JNIEnv *env, jclass,
                                                                                       jobject table_function) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	delete tf;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_name
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1name(JNIEnv *env, jclass,
                                                                                         jobject table_function,
                                                                                         jbyteArray name) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name_str.empty()) {
		env->ThrowNew(J_SQLException, "Specified function name must be not empty");
		return;
	}

	tf->name = name_str;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_add_parameter
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
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

	tf->parameters.emplace_back(lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_add_named_parameter
 * Signature: (Ljava/nio/ByteBuffer;[BLjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1add_1named_1parameter(
    JNIEnv *env, jclass, jobject table_function, jbyteArray name, jobject logical_type) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name_str.empty()) {
		env->ThrowNew(J_SQLException, "Specified parameter name must be not empty");
		return;
	}
	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	// same as in C API v1, the first declaration of a named parameter is used
	for (auto &param : tf->named_parameters) {
		if (names_equal_case_insensitive(param.first, name_str)) {
			return;
		}
	}
	tf->named_parameters.emplace_back(name_str, spec_logical_type(lt));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_extra_info
 * Signature: (Ljava/nio/ByteBuffer;Lorg/duckdb/DuckDBTableFunctionWrapper;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1extra_1info(JNIEnv *env, jclass,
                                                                                                jobject table_function,
                                                                                                jobject callback) {

	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Specified callback must be not null");
		return;
	}

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	auto callback_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, callback));
	if (callback_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified table function callback");
		return;
	}

	tf->callback = std::move(callback_holder);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_bind
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1bind(JNIEnv *env, jclass,
                                                                                         jobject table_function) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	tf->has_bind = true;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_init
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1init(JNIEnv *env, jclass,
                                                                                         jobject table_function) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	tf->has_init = true;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_local_init
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1local_1init(JNIEnv *env, jclass, jobject table_function) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	tf->has_local_init = true;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1function(JNIEnv *env, jclass,
                                                                                             jobject table_function) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	tf->has_function = true;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_supports_projection_pushdown
 * Signature: (Ljava/nio/ByteBuffer;Z)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1supports_1projection_1pushdown(
    JNIEnv *env, jclass, jobject table_function, jboolean pushdown) {

	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	bool pushdown_flag = !!pushdown;
	tf->projection_pushdown = pushdown_flag;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_register_table_function
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1table_1function(JNIEnv *env, jclass,
                                                                                        jobject connection,
                                                                                        jobject table_function) {

	auto conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	auto tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	return register_table_function(conn, *tf);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_bind_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1bind_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info) {

	auto fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *bind_data = nullptr;
	v2_error err;
	if (duckdb_v2_table_function_exec_get_bind_data(fi->info, &bind_data, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return TableFunctionState::get_object(bind_data);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_init_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1init_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info) {

	auto fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *global_init_data = nullptr;
	v2_error err;
	if (duckdb_v2_table_function_exec_get_global_state(fi->info, &global_init_data, err.out()) !=
	    DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return TableFunctionState::get_object(global_init_data);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_local_init_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1local_1init_1data(JNIEnv *env, jclass, jobject function_info) {

	auto fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (!fi->has_local_init) {
		return nullptr;
	}
	void *local_init_data = nullptr;
	v2_error err;
	if (duckdb_v2_table_function_exec_get_local_state(fi->info, &local_init_data, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return TableFunctionState::get_object(local_init_data);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1set_1error(JNIEnv *env, jclass,
                                                                                   jobject function_info,
                                                                                   jbyteArray error) {

	auto fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return;
	}

	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	fi->has_error = true;
	fi->error = error_str;
}

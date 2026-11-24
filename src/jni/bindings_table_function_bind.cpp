#include "bindings_table_function.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <algorithm>
#include <cctype>

static TableFunctionBindInfoHolder *bind_info_buf_to_bind_info(JNIEnv *env, jobject bind_info_buf) {
	if (bind_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function bind info buffer");
		return nullptr;
	}
	auto bind_info = reinterpret_cast<TableFunctionBindInfoHolder *>(env->GetDirectBufferAddress(bind_info_buf));
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

	auto bi = bind_info_buf_to_bind_info(env, bind_info);
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
	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	// same as in C API v1, INVALID and ANY types are ignored
	auto type_id = logical_type_to_capi(lt);
	if (type_id == capi_type::INVALID || type_id == capi_type::ANY) {
		return;
	}
	v2_error err;
	if (duckdb_v2_table_function_bind_add_result_column(bi->result, name_str.data(), name_str.size(), lt, err.out()) !=
	    DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_parameter_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter_1count(JNIEnv *env, jclass,
                                                                                           jobject bind_info) {

	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return 0;
	}

	// the positional parameters
	idx_t count = bi->positional_count;

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_parameter
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1parameter(JNIEnv *env, jclass,
                                                                                      jobject bind_info, jlong index) {

	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (index_idx >= bi->positional_count) {
		return nullptr;
	}
	duckdb_v2_value_handle val = nullptr;
	v2_error err;
	if (duckdb_v2_function_bind_get_arg_value(bi->info, index_idx, &val, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}
	jobject buf = make_ptr_buf(env, val);
	if (buf == nullptr) {
		duckdb_v2_value_destroy(&val);
	}
	return buf;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_get_named_parameter
 * Signature: (Ljava/nio/ByteBuffer;[B)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1get_1named_1parameter(JNIEnv *env, jclass,
                                                                                             jobject bind_info,
                                                                                             jbyteArray name) {

	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	// same as in C API v1, the named parameters that were not passed are reported as absent
	for (auto &param : bi->named_parameters) {
		auto &param_name = param.first;
		bool equal =
		    param_name.size() == name_str.size() &&
		    std::equal(param_name.begin(), param_name.end(), name_str.begin(), [](char a, char b) {
			    return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
		    });
		if (!equal) {
			continue;
		}
		// a copy of the value is passed to Java
		auto factory = default_factory();
		v2_logical_type type;
		duckdb_v2_value_handle val = nullptr;
		v2_error err;
		if (duckdb_v2_value_get_logical_type(param.second.get(), type.out(), err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_value_cast(factory, param.second.get(), type.get(), &val, err.out()) != DUCKDB_V2_ERROR_NONE) {
			throw_sql_exception(env, err);
			return nullptr;
		}
		jobject buf = make_ptr_buf(env, val);
		if (buf == nullptr) {
			duckdb_v2_value_destroy(&val);
		}
		return buf;
	}
	return nullptr;
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

	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	auto bind_data_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, bind_data));
	if (bind_data_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified bind data");
		return;
	}

	bi->bind_data = std::unique_ptr<TableFunctionState>(new TableFunctionState(bind_data_holder.release()));
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
	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	idx_t cardinality_idx = jlong_to_idx(env, cardinality);
	if (env->ExceptionCheck()) {
		return;
	}
	bool exact = !!is_exact;

	v2_error err;
	if (duckdb_v2_table_function_bind_set_cardinality(bi->result, cardinality_idx, exact, err.out()) !=
	    DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_bind_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1bind_1set_1error(JNIEnv *env, jclass, jobject bind_info,
                                                                               jbyteArray error) {
	auto bi = bind_info_buf_to_bind_info(env, bind_info);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	bi->has_error = true;
	bi->error = error_str;
}

#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstdint>
#include <cstring>
#include <vector>

void throw_sql_exception(JNIEnv *env, const v2_error &error) {
	if (!env->ExceptionCheck()) {
		env->ThrowNew(J_SQLException, error.message().c_str());
	}
}

duckdb_v2_logical_type_handle logical_type_buf_to_logical_type(JNIEnv *env, jobject logical_type_buf) {

	if (logical_type_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid logical type buffer");
		return nullptr;
	}

	auto logical_type = reinterpret_cast<duckdb_v2_logical_type_handle>(env->GetDirectBufferAddress(logical_type_buf));
	if (logical_type == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid logical type");
		return nullptr;
	}

	return logical_type;
}

jobject make_logical_type_buf(JNIEnv *env, duckdb_v2_logical_type_handle logical_type) {
	if (logical_type == nullptr) {
		return nullptr;
	}
	jobject buf = make_ptr_buf(env, logical_type);
	if (buf == nullptr) {
		destroy_logical_type(logical_type);
	}
	return buf;
}

/*
 * Reads the parameter (the name and the value) of the specified type, returns false on failure.
 */
static bool get_type_param(duckdb_v2_logical_type_handle lt, idx_t index, std::string *name, v2_value *value) {
	const char *name_ptr = nullptr;
	idx_t name_length = 0;
	v2_value param_value;
	if (duckdb_v2_logical_type_get_param(lt, index, &name_ptr, &name_length, param_value.out(), nullptr) !=
	    DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	if (name != nullptr) {
		*name = std::string(name_ptr, name_length);
	}
	if (value != nullptr) {
		*value = std::move(param_value);
	}
	return true;
}

static idx_t get_type_param_count(duckdb_v2_logical_type_handle lt) {
	idx_t count = 0;
	if (duckdb_v2_logical_type_get_param_count(lt, &count, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return 0;
	}
	return count;
}

/*
 * Reads the width (index 0) or the scale (index 1) parameter of a DECIMAL type.
 */
static uint32_t get_decimal_param(duckdb_v2_logical_type_handle lt, idx_t index) {
	v2_value value;
	if (!get_type_param(lt, index, nullptr, &value)) {
		return 0;
	}
	uint32_t res = 0;
	if (duckdb_v2_value_get_uint(value.get(), &res, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return 0;
	}
	return res;
}

/*
 * The width and the scale of a DECIMAL are 32-bit unsigned, the values that do not fit a Java int are clamped.
 */
static jint decimal_param_to_jint(uint32_t param) {
	return param > static_cast<uint32_t>(INT32_MAX) ? INT32_MAX : static_cast<jint>(param);
}

/*
 * Creates a type from type ID with the specified parameter values, takes ownership of the values.
 */
static duckdb_v2_logical_type_handle create_type(duckdb_v2_factory_handle factory, DUCKDB_V2_LOGICAL_TYPE_ID type_id,
                                                 std::vector<std::string> names, std::vector<v2_value> values) {
	std::vector<const char *> names_ptrs;
	std::vector<idx_t> names_lengths;
	std::vector<duckdb_v2_value_handle> values_handles;
	for (size_t i = 0; i < values.size(); i++) {
		if (!values[i]) {
			return nullptr;
		}
		names_ptrs.push_back(i < names.size() ? names[i].c_str() : "");
		names_lengths.push_back(i < names.size() ? names[i].size() : 0);
		values_handles.push_back(values[i].get());
	}
	duckdb_v2_logical_type_handle res = nullptr;
	auto state = duckdb_v2_factory_create_type_from_id(
	    factory, type_id, values.empty() ? nullptr : names_ptrs.data(), values.empty() ? nullptr : names_lengths.data(),
	    values.empty() ? nullptr : values_handles.data(), static_cast<idx_t>(values.size()), &res, nullptr);
	if (state != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return res;
}

static v2_value create_type_value(duckdb_v2_factory_handle factory, duckdb_v2_logical_type_handle lt) {
	v2_value res;
	duckdb_v2_value_create_type(factory, lt, res.out(), nullptr);
	return res;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_logical_type
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1logical_1type(JNIEnv *env, jclass, jint type) {

	// same as in C API v1, types that require parameters cannot be created from type ID alone and
	// are reported as INVALID, INVALID type is represented with a placeholder handle
	DUCKDB_V2_LOGICAL_TYPE_ID type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	if (!type_id_from_capi(static_cast<int32_t>(type), type_id)) {
		return make_logical_type_buf(env, invalid_logical_type());
	}
	switch (type_id) {
	case DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
	case DUCKDB_V2_LOGICAL_TYPE_ID_ENUM:
	case DUCKDB_V2_LOGICAL_TYPE_ID_LIST:
	case DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
	case DUCKDB_V2_LOGICAL_TYPE_ID_MAP:
	case DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY:
	case DUCKDB_V2_LOGICAL_TYPE_ID_UNION:
		return make_logical_type_buf(env, invalid_logical_type());
	default:
		break;
	}

	auto factory = default_factory();
	duckdb_v2_logical_type_handle lt = nullptr;
	DUCKDB_V2_ERROR state = DUCKDB_V2_ERROR_NONE;
	const char *name = nullptr;
	switch (type_id) {
	case DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT:
		name = "VARIANT";
		break;
	case DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY:
		name = "GEOMETRY";
		break;
	case DUCKDB_V2_LOGICAL_TYPE_ID_GEOGRAPHY:
		name = "GEOGRAPHY";
		break;
	default:
		break;
	}
	if (name != nullptr) {
		// these types cannot be created from type ID without parameters, their default variants
		// are created from their names
		state = duckdb_v2_factory_create_type_from_text(factory, name, std::strlen(name), &lt, nullptr);
	} else {
		state = duckdb_v2_factory_create_type_from_id(factory, type_id, nullptr, nullptr, nullptr, 0, &lt, nullptr);
	}
	if (state != DUCKDB_V2_ERROR_NONE) {
		return make_logical_type_buf(env, invalid_logical_type());
	}
	return make_logical_type_buf(env, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_decimal_type
 * Signature: (II)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1decimal_1type(JNIEnv *env, jclass, jint width,
                                                                                       jint scale) {

	if (width < 1) {
		env->ThrowNew(J_SQLException, "Invalid decimal width: expected at least 1");
		return nullptr;
	}
	if (scale < 0 || scale > width) {
		env->ThrowNew(J_SQLException, "Invalid decimal scale: expected 0..width");
		return nullptr;
	}
	auto factory = default_factory();
	std::vector<v2_value> values(2);
	duckdb_v2_value_create_uint(factory, static_cast<uint32_t>(width), values[0].out(), nullptr);
	duckdb_v2_value_create_uint(factory, static_cast<uint32_t>(scale), values[1].out(), nullptr);
	auto lt = create_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL, {}, std::move(values));
	return make_logical_type_buf(env, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_type_id
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1type_1id(JNIEnv *env, jclass, jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return 1;
	}

	return static_cast<jint>(logical_type_to_capi(lt));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_width
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1width(JNIEnv *env, jclass,
                                                                             jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::DECIMAL) {
		return 0;
	}
	return decimal_param_to_jint(get_decimal_param(lt, 0));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_scale
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1scale(JNIEnv *env, jclass,
                                                                             jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::DECIMAL) {
		return 0;
	}
	return decimal_param_to_jint(get_decimal_param(lt, 1));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_internal_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1internal_1type(JNIEnv *env, jclass,
                                                                                      jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::DECIMAL) {
		return static_cast<jint>(capi_type::INVALID);
	}
	// The DECIMAL storage tiers, determined by the width
	uint32_t width = get_decimal_param(lt, 0);
	if (width == 0) {
		return static_cast<jint>(capi_type::INVALID);
	}
	if (width <= 4) {
		return static_cast<jint>(capi_type::SMALLINT);
	}
	if (width <= 9) {
		return static_cast<jint>(capi_type::INTEGER);
	}
	if (width <= 18) {
		return static_cast<jint>(capi_type::BIGINT);
	}
	if (width <= 38) {
		return static_cast<jint>(capi_type::HUGEINT);
	}
	// decimals wider than 38 digits store their unscaled value as BIGNUM storage
	return static_cast<jint>(capi_type::BIGNUM);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_list_type
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1list_1type(JNIEnv *env, jclass,
                                                                                    jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (lt == invalid_logical_type()) {
		return nullptr;
	}

	auto factory = default_factory();
	std::vector<v2_value> values;
	values.emplace_back(create_type_value(factory, lt));
	auto list_type = create_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_LIST, {}, std::move(values));
	return make_logical_type_buf(env, list_type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_array_type
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1array_1type(JNIEnv *env, jclass,
                                                                                     jobject logical_type,
                                                                                     jlong array_size) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t size_idx = jlong_to_idx(env, array_size);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (lt == invalid_logical_type()) {
		return nullptr;
	}

	auto factory = default_factory();
	std::vector<v2_value> values(2);
	values[0] = create_type_value(factory, lt);
	duckdb_v2_value_create_ubigint(factory, static_cast<uint64_t>(size_idx), values[1].out(), nullptr);
	auto array_type = create_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY, {}, std::move(values));
	return make_logical_type_buf(env, array_type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_struct_type
 * Signature: ([Ljava/nio/ByteBuffer;[[B)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1struct_1type(JNIEnv *env, jclass,
                                                                                      jobjectArray member_types,
                                                                                      jobjectArray member_names) {

	if (member_types == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid member types array");
		return nullptr;
	}
	if (member_names == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid member names array");
		return nullptr;
	}

	size_t member_count = static_cast<size_t>(env->GetArrayLength(member_types));
	size_t names_count = static_cast<size_t>(env->GetArrayLength(member_names));
	if (member_count != names_count) {
		env->ThrowNew(J_SQLException, "Invalid member names array size");
		return nullptr;
	}

	std::vector<duckdb_v2_logical_type_handle> mt_vec;
	mt_vec.reserve(member_count);

	for (size_t i = 0; i < member_count; i++) {
		jobject lt_buf = env->GetObjectArrayElement(member_types, i);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		if (nullptr == lt_buf) {
			env->ThrowNew(J_SQLException, "Invalid null type specified");
			return nullptr;
		}
		auto lt = logical_type_buf_to_logical_type(env, lt_buf);
		env->DeleteLocalRef(lt_buf);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		mt_vec.push_back(lt);
	}

	std::vector<std::string> names_vec;
	names_vec.reserve(member_count);

	for (size_t i = 0; i < member_count; i++) {
		jbyteArray jba = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(member_names, i));
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		if (nullptr == jba) {
			env->ThrowNew(J_SQLException, "Invalid null name specified");
			return nullptr;
		}
		std::string str = jbyteArray_to_string(env, jba);
		env->DeleteLocalRef(jba);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		names_vec.emplace_back(std::move(str));
	}

	auto factory = default_factory();
	std::vector<v2_value> values;
	values.reserve(member_count);
	bool unnamed = true;
	for (size_t i = 0; i < member_count; i++) {
		if (mt_vec[i] == invalid_logical_type()) {
			return nullptr;
		}
		if (!names_vec[i].empty()) {
			unnamed = false;
		}
		values.emplace_back(create_type_value(factory, mt_vec[i]));
	}
	// same as in C API v1, an unnamed (empty member names) struct is a TUPLE
	auto type_id = unnamed && member_count > 0 ? DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE : DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
	auto struct_type = create_type(factory, type_id, std::move(names_vec), std::move(values));
	return make_logical_type_buf(env, struct_type);
}

/*
 * Returns the number of struct children, the same way as C API v1, that considers the hidden "tag" field
 * of a UNION as its first child.
 */
static idx_t struct_type_child_count(duckdb_v2_logical_type_handle lt) {
	switch (logical_type_to_capi(lt)) {
	case capi_type::STRUCT:
		return get_type_param_count(lt);
	case capi_type::UNION:
		return get_type_param_count(lt) + 1;
	default:
		return 0;
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_struct_type_child_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1struct_1type_1child_1count(JNIEnv *env, jclass,
                                                                                          jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = struct_type_child_count(lt);

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_struct_type_child_name
 * Signature: (Ljava/nio/ByteBuffer;J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1struct_1type_1child_1name(JNIEnv *env, jclass,
                                                                                              jobject logical_type,
                                                                                              jlong index) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t count = struct_type_child_count(lt);
	if (index_idx >= count) {
		env->ThrowNew(J_SQLException, "Invalid struct field index specified");
		return nullptr;
	}

	std::string name;
	if (logical_type_to_capi(lt) == capi_type::UNION) {
		// the hidden "tag" field of a UNION has an empty name
		if (index_idx > 0 && !get_type_param(lt, index_idx - 1, &name, nullptr)) {
			return nullptr;
		}
	} else if (!get_type_param(lt, index_idx, &name, nullptr)) {
		return nullptr;
	}

	return make_jbyteArray(env, name.data(), static_cast<idx_t>(name.length()));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_array_type_array_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1array_1type_1array_1size(JNIEnv *env, jclass,
                                                                                        jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::ARRAY) {
		return 0;
	}
	v2_value value;
	uint64_t size = 0;
	if (!get_type_param(lt, 1, nullptr, &value) ||
	    duckdb_v2_value_get_ubigint(value.get(), &size, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return 0;
	}

	return static_cast<jlong>(size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_enum_internal_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1enum_1internal_1type(JNIEnv *env, jclass,
                                                                                   jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::ENUM) {
		return static_cast<jint>(capi_type::INVALID);
	}
	// The ENUM storage tiers, determined by the dictionary size
	idx_t size = get_type_param_count(lt);
	if (size <= 255) {
		return static_cast<jint>(capi_type::UTINYINT);
	}
	if (size <= 65535) {
		return static_cast<jint>(capi_type::USMALLINT);
	}
	return static_cast<jint>(capi_type::UINTEGER);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_enum_dictionary_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1enum_1dictionary_1size(JNIEnv *env, jclass,
                                                                                      jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	if (logical_type_to_capi(lt) != capi_type::ENUM) {
		return 0;
	}
	idx_t size = get_type_param_count(lt);

	return static_cast<jlong>(size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_enum_dictionary_value
 * Signature: (Ljava/nio/ByteBuffer;J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1enum_1dictionary_1value(JNIEnv *env, jclass,
                                                                                            jobject logical_type,
                                                                                            jlong index) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (logical_type_to_capi(lt) != capi_type::ENUM) {
		return nullptr;
	}

	idx_t size = get_type_param_count(lt);
	if (index_idx >= size) {
		env->ThrowNew(J_SQLException, "Invalid enum field index specified");
		return nullptr;
	}

	v2_value value;
	const char *str = nullptr;
	idx_t len = 0;
	if (!get_type_param(lt, index_idx, nullptr, &value) ||
	    duckdb_v2_value_get_varchar(value.get(), &str, &len, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}

	return make_jbyteArray(env, str, len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_logical_type
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1logical_1type(JNIEnv *env, jclass,
                                                                                     jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	destroy_logical_type(lt);
}

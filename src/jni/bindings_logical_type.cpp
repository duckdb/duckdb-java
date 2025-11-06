#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstring>
#include <vector>

duckdb_logical_type logical_type_buf_to_logical_type(JNIEnv *env, jobject logical_type_buf) {

	if (logical_type_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid logical type buffer");
		return nullptr;
	}

	duckdb_logical_type logical_type =
	    reinterpret_cast<duckdb_logical_type>(env->GetDirectBufferAddress(logical_type_buf));
	if (logical_type == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid logical type");
		return nullptr;
	}

	return logical_type;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_logical_type
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1logical_1type(JNIEnv *env, jclass, jint type) {

	duckdb_type dt = static_cast<duckdb_type>(type);

	duckdb_logical_type lt = duckdb_create_logical_type(dt);

	return make_ptr_buf(env, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_type_id
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1type_1id(JNIEnv *env, jclass, jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return 1;
	}

	duckdb_type type_id = duckdb_get_type_id(lt);

	return static_cast<jint>(type_id);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_width
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1width(JNIEnv *env, jclass,
                                                                             jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	uint8_t width = duckdb_decimal_width(lt);

	return static_cast<jint>(width);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_scale
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1scale(JNIEnv *env, jclass,
                                                                             jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	uint8_t scale = duckdb_decimal_scale(lt);

	return static_cast<jint>(scale);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_decimal_internal_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1decimal_1internal_1type(JNIEnv *env, jclass,
                                                                                      jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_type type_id = duckdb_decimal_internal_type(lt);

	return static_cast<jint>(type_id);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_list_type
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1list_1type(JNIEnv *env, jclass,
                                                                                    jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_logical_type list_type = duckdb_create_list_type(lt);

	return make_ptr_buf(env, list_type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_array_type
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1array_1type(JNIEnv *env, jclass,
                                                                                     jobject logical_type,
                                                                                     jlong array_size) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t size_idx = jlong_to_idx(env, array_size);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_logical_type array_type = duckdb_create_array_type(lt, size_idx);

	return make_ptr_buf(env, array_type);
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

	std::vector<duckdb_logical_type> mt_vec;
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
		duckdb_logical_type lt = logical_type_buf_to_logical_type(env, lt_buf);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		mt_vec.push_back(lt);
	}

	std::vector<std::string> names_vec;
	names_vec.reserve(member_count);
	std::vector<const char *> names_cstr_vec;
	names_cstr_vec.reserve(member_count);

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
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		names_vec.emplace_back(std::move(str));
		std::string &str_ref = names_vec.back();
		names_cstr_vec.emplace_back(str_ref.c_str());
	}

	duckdb_logical_type struct_type =
	    duckdb_create_struct_type(mt_vec.data(), names_cstr_vec.data(), static_cast<idx_t>(member_count));

	return make_ptr_buf(env, struct_type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_struct_type_child_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1struct_1type_1child_1count(JNIEnv *env, jclass,
                                                                                          jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = duckdb_struct_type_child_count(lt);

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

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t count = duckdb_struct_type_child_count(lt);
	if (index_idx >= count) {
		env->ThrowNew(J_SQLException, "Invalid struct field index specified");
		return nullptr;
	}

	auto name_ptr = varchar_ptr(duckdb_struct_type_child_name(lt, index_idx), varchar_deleter);
	if (name_ptr.get() == nullptr) {
		return nullptr;
	}

	idx_t len = static_cast<idx_t>(std::strlen(name_ptr.get()));

	return make_jbyteArray(env, name_ptr.get(), len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_array_type_array_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1array_1type_1array_1size(JNIEnv *env, jclass,
                                                                                        jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t size = duckdb_array_type_array_size(lt);

	return static_cast<jlong>(size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_enum_internal_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1enum_1internal_1type(JNIEnv *env, jclass,
                                                                                   jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_type type_id = duckdb_enum_internal_type(lt);

	return static_cast<jint>(type_id);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_enum_dictionary_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1enum_1dictionary_1size(JNIEnv *env, jclass,
                                                                                      jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t size = duckdb_enum_dictionary_size(lt);

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

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t index_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t size = duckdb_enum_dictionary_size(lt);
	if (index_idx >= size) {
		env->ThrowNew(J_SQLException, "Invalid enum field index specified");
		return nullptr;
	}

	auto name_ptr = varchar_ptr(duckdb_enum_dictionary_value(lt, index_idx), varchar_deleter);
	if (name_ptr.get() == nullptr) {
		return nullptr;
	}

	idx_t len = static_cast<idx_t>(std::strlen(name_ptr.get()));

	return make_jbyteArray(env, name_ptr.get(), len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_logical_type
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1logical_1type(JNIEnv *env, jclass,
                                                                                     jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_logical_type(&lt);
}

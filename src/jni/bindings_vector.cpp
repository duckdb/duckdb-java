#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <vector>

static duckdb_vector vector_buf_to_vector(JNIEnv *env, jobject vector_buf) {

	if (vector_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector buffer");
		return nullptr;
	}

	duckdb_vector vector = reinterpret_cast<duckdb_vector>(env->GetDirectBufferAddress(vector_buf));

	if (vector == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector");
		return nullptr;
	}

	return vector;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_vector
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1vector(JNIEnv *env, jclass,
                                                                                jobject logical_type) {

	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t cap = duckdb_vector_size();

	duckdb_vector vec = duckdb_create_vector(lt, cap);

	return make_ptr_buf(env, vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_vector
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1vector(JNIEnv *env, jclass, jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_vector(&vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_column_type
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1column_1type(JNIEnv *env, jclass,
                                                                                           jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_logical_type lt = duckdb_vector_get_column_type(vec);

	return make_ptr_buf(env, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1data(JNIEnv *env, jclass, jobject vector,
                                                                                   jlong size_bytes) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t vector_size = jlong_to_idx(env, size_bytes);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *data = duckdb_vector_get_data(vec);

	if (data != nullptr) {
		return make_data_buf(env, data, vector_size);
	}

	return nullptr;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_validity
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1validity(JNIEnv *env, jclass,
                                                                                       jobject vector,
                                                                                       jlong array_size) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	uint64_t *mask = duckdb_vector_get_validity(vec);
	idx_t vec_len = duckdb_vector_size();
	idx_t mask_len = vec_len * sizeof(uint64_t) * array_size / 64;

	return make_data_buf(env, mask, mask_len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_ensure_validity_writable
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1ensure_1validity_1writable(JNIEnv *env, jclass,
                                                                                                 jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_vector_ensure_validity_writable(vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_assign_string_element_len
 * Signature: (Ljava/nio/ByteBuffer;J[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1assign_1string_1element_1len(JNIEnv *env, jclass,
                                                                                                   jobject vector,
                                                                                                   jlong index,
                                                                                                   jbyteArray str) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return;
	}
	auto str_ptr = make_jbyteArray_ptr(env, str);
	if (env->ExceptionCheck()) {
		return;
	}

	idx_t len = static_cast<idx_t>(env->GetArrayLength(str));

	duckdb_vector_assign_string_element_len(vec, idx, str_ptr.get(), len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                          jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_vector res = duckdb_list_vector_get_child(vec);

	return make_ptr_buf(env, res);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_get_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1get_1size(JNIEnv *env, jclass,
                                                                                       jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t size = duckdb_list_vector_get_size(vec);

	return uint64_to_jlong(size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_set_size
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1set_1size(JNIEnv *env, jclass,
                                                                                      jobject vector, jlong size) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t size_idx = jlong_to_idx(env, size);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_list_vector_set_size(vec, size_idx);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_reserve
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1reserve(JNIEnv *env, jclass, jobject vector,
                                                                                    jlong capacity) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t cap = jlong_to_idx(env, capacity);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_list_vector_reserve(vec, cap);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_struct_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1struct_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                            jobject vector,
                                                                                            jlong index) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_vector res = duckdb_struct_vector_get_child(vec, idx);

	return make_ptr_buf(env, res);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_array_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1array_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                           jobject vector) {

	duckdb_vector vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_vector res = duckdb_array_vector_get_child(vec);

	return make_ptr_buf(env, res);
}

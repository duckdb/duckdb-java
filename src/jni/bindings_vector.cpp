#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstring>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

/*
 * Vectors are passed to Java as borrowed C API v2 handles.
 */
static duckdb_v2_vector_handle vector_buf_to_vector(JNIEnv *env, jobject vector_buf) {

	if (vector_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector buffer");
		return nullptr;
	}

	auto vector = reinterpret_cast<duckdb_v2_vector_handle>(env->GetDirectBufferAddress(vector_buf));

	if (vector == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector");
		return nullptr;
	}

	return vector;
}

/*
 * In the C API v2 vectors are always borrowed from data chunks, standalone vectors created with
 * duckdb_create_vector are backed by single-column data chunks that are kept here until the vector is destroyed.
 */
static std::mutex standalone_vectors_lock;
static std::unordered_map<duckdb_v2_vector_handle, v2_data_chunk> standalone_vectors;

/*
 * Same as in C API v1, returns the data of FLAT and CONSTANT vectors, nullptr otherwise.
 */
static void *vector_get_data(duckdb_v2_vector_handle vec) {
	void *data = nullptr;
	if (duckdb_v2_vector_get_data_mutable(vec, &data, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return data;
}

/*
 * Same as in C API v1, returns the validity mask of FLAT and CONSTANT vectors, the mask can be nullptr if all rows are
 * valid. Returns nullptr for other vector types.
 */
static uint64_t *vector_get_validity(duckdb_v2_vector_handle vec) {
	DUCKDB_V2_VECTOR_TYPE vector_type = DUCKDB_V2_VECTOR_TYPE_OTHER;
	if (duckdb_v2_vector_get_vector_type(vec, &vector_type, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	if (vector_type != DUCKDB_V2_VECTOR_TYPE_FLAT && vector_type != DUCKDB_V2_VECTOR_TYPE_CONSTANT) {
		return nullptr;
	}
	duckdb_v2_vector_view view;
	if (duckdb_v2_vector_get_view(vec, &view, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return const_cast<uint64_t *>(view.validity);
}

static DUCKDB_V2_LOGICAL_TYPE_ID vector_get_type_id(duckdb_v2_vector_handle vec) {
	DUCKDB_V2_LOGICAL_TYPE_ID type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	v2_logical_type lt;
	if (duckdb_v2_vector_get_logical_type(vec, lt.out(), nullptr) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_get_id(lt.get(), &type_id, nullptr);
	}
	return type_id;
}

/*
 * Same as duckdb_unsafe_vector_assign_string_element_len in C API v1, the string is not validated.
 */
static bool vector_assign_string_unsafe(duckdb_v2_vector_handle vec, idx_t index, const char *str, idx_t len,
                                        v2_error &err) {
	void *data = nullptr;
	duckdb_v2_arena_handle arena = nullptr;
	if (duckdb_v2_vector_get_data_mutable(vec, &data, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_arena(vec, &arena, err.out()) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	return make_v2_bytes(arena, str, len, reinterpret_cast<duckdb_v2_bytes *>(data)[index]);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_string
 * Signature: (Ljava/nio/ByteBuffer;J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1string(JNIEnv *env, jclass,
                                                                                        jobject vector_data,
                                                                                        jlong row) {

	if (vector_data == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector data buffer");
		return nullptr;
	}
	auto data = reinterpret_cast<duckdb_v2_bytes *>(env->GetDirectBufferAddress(vector_data));
	if (data == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector data");
		return nullptr;
	}
	idx_t row_idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	auto &string_value = data[row_idx];
	auto string_len = v2_bytes_length(string_value);
	auto string_ptr = v2_bytes_data(string_value);
	return make_jbyteArray(env, string_ptr, string_len);
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1string__Ljava_nio_ByteBuffer_2J(JNIEnv *env, jclass clazz,
                                                                                    jobject vector_data, jlong row) {
	return Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1string(env, clazz, vector_data, row);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_vector
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1vector(JNIEnv *env, jclass,
                                                                                jobject logical_type) {

	auto lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (lt == invalid_logical_type()) {
		return nullptr;
	}

	// the chunk is created with the standard vector size capacity
	duckdb_v2_logical_type_handle types[] = {lt};
	v2_data_chunk chunk;
	if (duckdb_v2_data_chunk_create(default_factory(), types, 1, chunk.out(), nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	duckdb_v2_vector_handle vec = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk.get(), 0, &vec, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	{
		std::lock_guard<std::mutex> guard(standalone_vectors_lock);
		standalone_vectors.emplace(vec, std::move(chunk));
	}
	return make_ptr_buf(env, vec);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_vector
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1vector(JNIEnv *env, jclass, jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}

	v2_data_chunk chunk;
	{
		std::lock_guard<std::mutex> guard(standalone_vectors_lock);
		auto it = standalone_vectors.find(vec);
		if (it == standalone_vectors.end()) {
			return;
		}
		chunk = std::move(it->second);
		standalone_vectors.erase(it);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_column_type
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1column_1type(JNIEnv *env, jclass,
                                                                                           jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_v2_logical_type_handle lt = nullptr;
	v2_error err;
	if (duckdb_v2_vector_get_logical_type(vec, &lt, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return make_logical_type_buf(env, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1data(JNIEnv *env, jclass, jobject vector,
                                                                                   jlong size_bytes) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t vector_size = jlong_to_idx(env, size_bytes);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *data = vector_get_data(vec);

	if (data != nullptr) {
		return make_data_buf(env, data, vector_size);
	}

	return nullptr;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_data_zeroed
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1data_1zeroed(JNIEnv *env, jclass,
                                                                                           jobject vector,
                                                                                           jlong size_bytes) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t vector_size = jlong_to_idx(env, size_bytes);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *data = vector_get_data(vec);

	if (data == nullptr) {
		return nullptr;
	}

	std::memset(data, '\0', vector_size);

	return make_data_buf(env, data, vector_size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_get_validity
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1get_1validity(JNIEnv *env, jclass,
                                                                                       jobject vector,
                                                                                       jlong vector_size_elems) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t vector_size = jlong_to_idx(env, vector_size_elems);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	uint64_t *mask = vector_get_validity(vec);
	idx_t vector_size_rounded = vector_size;
	if (vector_size % 64 != 0) {
		vector_size_rounded += 64 - (vector_size % 64);
	}
	idx_t mask_len = vector_size_rounded * sizeof(uint64_t) / 64;

	return make_data_buf(env, mask, mask_len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_ensure_validity_writable
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1ensure_1validity_1writable(JNIEnv *env, jclass,
                                                                                                 jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}

	uint64_t *validity = nullptr;
	v2_error err;
	if (duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
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

	auto vec = vector_buf_to_vector(env, vector);
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

	// same as in C API v1, invalid UTF-8 strings in VARCHAR vectors are set to NULL
	if (vector_get_type_id(vec) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR) {
		if (duckdb_v2_validate_utf8(str_ptr.get(), len, nullptr) != DUCKDB_V2_ERROR_NONE) {
			uint64_t *validity = nullptr;
			v2_error err;
			if (duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, err.out()) != DUCKDB_V2_ERROR_NONE) {
				throw_sql_exception(env, err);
				return;
			}
			validity[idx / 64] &= ~(static_cast<uint64_t>(1) << (idx % 64));
			return;
		}
	}
	v2_error err;
	if (!vector_assign_string_unsafe(vec, idx, str_ptr.get(), len, err)) {
		throw_sql_exception(env, err);
	}
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_assign_string_elements
 * Signature: (Ljava/nio/ByteBuffer;J[B[JI)V
 *
 * Batch variant of duckdb_vector_assign_string_element_len: assigns count
 * concatenated UTF-8 strings starting at row index, in a single JNI call.
 * lengths[i] >= 0 marks the byte length of element i, a negative length marks
 * a NULL element. The complete payload is validated once before the loop uses
 * the unsafe assignment primitive for each valid element.
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1assign_1string_1elements(
    JNIEnv *env, jclass, jobject vector, jlong index, jbyteArray data, jlongArray lengths, jint data_length) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return;
	}
	auto payload = make_jbyteArray_ptr(env, data);
	if (env->ExceptionCheck()) {
		return;
	}
	if (lengths == nullptr || data_length < 0 || data_length > env->GetArrayLength(data)) {
		env->ThrowNew(J_SQLException, "Invalid string batch length");
		return;
	}
	jint count = env->GetArrayLength(lengths);
	idx_t row_idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return;
	}
	jlong *lens = env->GetLongArrayElements(lengths, nullptr);
	if (lens == nullptr) {
		return;
	}
	jlong payload_length = 0;
	for (jint i = 0; i < count; i++) {
		jlong len = lens[i];
		if (len < 0 || len <= data_length - payload_length) {
			if (len >= 0) {
				payload_length += len;
			}
			continue;
		}
		env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
		env->ThrowNew(J_SQLException, "Invalid string batch lengths");
		return;
	}
	if (payload_length != data_length) {
		env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
		env->ThrowNew(J_SQLException, "String batch length does not match payload");
		return;
	}
	if (duckdb_v2_validate_utf8(payload.get(), static_cast<idx_t>(payload_length), nullptr) != DUCKDB_V2_ERROR_NONE) {
		env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
		env->ThrowNew(J_SQLException, "Invalid UTF-8 string batch");
		return;
	}
	uint64_t *validity = nullptr;
	if (duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    validity == nullptr) {
		env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
		env->ThrowNew(J_SQLException, "Invalid validity mask");
		return;
	}
	void *data_ptr = nullptr;
	duckdb_v2_arena_handle arena = nullptr;
	v2_error err;
	if (duckdb_v2_vector_get_data_mutable(vec, &data_ptr, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_arena(vec, &arena, err.out()) != DUCKDB_V2_ERROR_NONE) {
		env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
		throw_sql_exception(env, err);
		return;
	}
	auto elements = reinterpret_cast<duckdb_v2_bytes *>(data_ptr);
	const char *p = payload.get();
	for (jint i = 0; i < count; i++) {
		jlong len = lens[i];
		idx_t row = row_idx + static_cast<idx_t>(i);
		if (len < 0) {
			validity[row / 64] &= ~(static_cast<uint64_t>(1) << (row % 64));
			continue;
		}
		if (!make_v2_bytes(arena, p, static_cast<idx_t>(len), elements[row])) {
			env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
			env->ThrowNew(J_SQLException, "Unable to allocate string");
			return;
		}
		p += len;
		validity[row / 64] |= static_cast<uint64_t>(1) << (row % 64);
	}
	env->ReleaseLongArrayElements(lengths, lens, JNI_ABORT);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                          jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_v2_vector_handle res = nullptr;
	v2_error err;
	if (duckdb_v2_vector_get_child(vec, 0, &res, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return make_ptr_buf(env, res);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_get_size
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1get_1size(JNIEnv *env, jclass,
                                                                                       jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}

	// the size of a list is the size of its child vector
	duckdb_v2_vector_handle child = nullptr;
	idx_t size = 0;
	v2_error err;
	if (duckdb_v2_vector_get_child(vec, 0, &child, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_size(child, &size, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return -1;
	}

	return uint64_to_jlong(size);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_set_size
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1set_1size(JNIEnv *env, jclass,
                                                                                      jobject vector, jlong size) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t size_idx = jlong_to_idx(env, size);
	if (env->ExceptionCheck()) {
		return -1;
	}

	// sets the size of the child vector, reserving the space for it if necessary
	duckdb_v2_vector_handle child = nullptr;
	if (duckdb_v2_vector_get_child(vec, 0, &child, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_set_size(child, size_idx, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	return static_cast<jint>(CAPI_STATE_SUCCESS);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_list_vector_reserve
 * Signature: (Ljava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1list_1vector_1reserve(JNIEnv *env, jclass, jobject vector,
                                                                                    jlong capacity) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t cap = jlong_to_idx(env, capacity);
	if (env->ExceptionCheck()) {
		return -1;
	}

	// The C API v2 does not have a reserve-only call, the child vector is resized to the requested capacity,
	// that reserves the space for it, and then its size is restored
	duckdb_v2_vector_handle child = nullptr;
	idx_t size = 0;
	if (duckdb_v2_vector_get_child(vec, 0, &child, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_size(child, &size, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	if (cap > size) {
		if (duckdb_v2_vector_set_size(child, cap, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_set_size(child, size, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}

	return static_cast<jint>(CAPI_STATE_SUCCESS);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_struct_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1struct_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                            jobject vector,
                                                                                            jlong index) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_v2_vector_handle res = nullptr;
	v2_error err;
	if (duckdb_v2_vector_get_child(vec, idx, &res, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return make_ptr_buf(env, res);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_array_vector_get_child
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1array_1vector_1get_1child(JNIEnv *env, jclass,
                                                                                           jobject vector) {

	auto vec = vector_buf_to_vector(env, vector);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_v2_vector_handle res = nullptr;
	v2_error err;
	if (duckdb_v2_vector_get_child(vec, 0, &res, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}

	return make_ptr_buf(env, res);
}

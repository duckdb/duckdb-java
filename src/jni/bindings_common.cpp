#include "bindings.hpp"

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_size
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1size(JNIEnv *, jclass) {

	// The C API v2 does not expose the vector size constant directly, it is the capacity of a newly created chunk
	static const idx_t vector_size = [] {
		idx_t capacity = 0;
		auto factory = default_factory();
		v2_logical_type type;
		if (duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, nullptr, nullptr, nullptr,
		                                          0, type.out(), nullptr) != DUCKDB_V2_ERROR_NONE) {
			return capacity;
		}
		duckdb_v2_logical_type_handle types[] = {type.get()};
		v2_data_chunk chunk;
		if (duckdb_v2_data_chunk_create(factory, types, 1, chunk.out(), nullptr) != DUCKDB_V2_ERROR_NONE) {
			return capacity;
		}
		duckdb_v2_data_chunk_get_capacity(chunk.get(), &capacity, nullptr);
		return capacity;
	}();

	return static_cast<jlong>(vector_size);
}

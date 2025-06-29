#include "bindings.hpp"

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_vector_size
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1vector_1size(JNIEnv *, jclass) {

	idx_t vector_size = duckdb_vector_size();

	return static_cast<jlong>(vector_size);
}

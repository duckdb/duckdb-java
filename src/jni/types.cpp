
#include "types.hpp"

#include "util.hpp"

#include <string>
#include <vector>

static const std::vector<std::string> SPATIAL_STRUCT_TYPES = {"POINT_2D", "POINT_3D", "POINT_4D", "BOX_2D", "BOX_2DF"};
static const std::vector<std::string> SPATIAL_LIST_TYPES = {"LINESTRING_2D", "POLYGON_2D"};
static const std::vector<std::string> SPATIAL_BLOB_TYPES = {"GEOMETRY", "WKB_BLOB"};

duckdb::LogicalTypeId type_id_from_type(const duckdb::LogicalType &type) {
	if (!type.HasAlias()) {
		return type.id();
	}

	std::string alias = type.GetAlias();

	if (vector_contains(SPATIAL_STRUCT_TYPES, alias)) {
		return duckdb::LogicalTypeId::STRUCT;
	}

	if (vector_contains(SPATIAL_LIST_TYPES, alias)) {
		return duckdb::LogicalTypeId::LIST;
	}

	if (vector_contains(SPATIAL_BLOB_TYPES, alias)) {
		return duckdb::LogicalTypeId::BLOB;
	}

	return duckdb::LogicalTypeId::UNKNOWN;
}

#include "types.hpp"

#include <string>
#include <vector>

std::string type_to_jduckdb_type(duckdb::LogicalType logical_type) {
	switch (logical_type.id()) {
	case duckdb::LogicalTypeId::DECIMAL: {

		uint8_t width = 0;
		uint8_t scale = 0;
		logical_type.GetDecimalProperties(width, scale);
		std::string width_scale = std::to_string(width) + std::string(";") + std::to_string(scale);

		auto physical_type = logical_type.InternalType();
		switch (physical_type) {
		case duckdb::PhysicalType::INT16: {
			std::string res = std::string("DECIMAL16;") + width_scale;
			return res;
		}
		case duckdb::PhysicalType::INT32: {
			std::string res = std::string("DECIMAL32;") + width_scale;
			return res;
		}
		case duckdb::PhysicalType::INT64: {
			std::string res = std::string("DECIMAL64;") + width_scale;
			return res;
		}
		case duckdb::PhysicalType::INT128: {
			std::string res = std::string("DECIMAL128;") + width_scale;
			return res;
		}
		default:
			return std::string("no physical type found");
		}
	} break;
	default:
		// JSON requires special handling because it is mapped
		// to JsonNode class
		if (logical_type.IsJSONType()) {
			return logical_type.GetAlias();
		}
		return duckdb::EnumUtil::ToString(logical_type.id());
	}
}

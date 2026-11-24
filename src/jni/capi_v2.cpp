#include "capi_v2.hpp"

#include <cstring>

capi_type type_id_to_capi(DUCKDB_V2_LOGICAL_TYPE_ID type_id) {
	switch (type_id) {
	case DUCKDB_V2_LOGICAL_TYPE_ID_INVALID:
	case DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN:
		return capi_type::INVALID;
	case DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
		return capi_type::BOOLEAN;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
		return capi_type::TINYINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
		return capi_type::SMALLINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
		return capi_type::INTEGER;
	case DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
		return capi_type::BIGINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
		return capi_type::UTINYINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
		return capi_type::USMALLINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
		return capi_type::UINTEGER;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
		return capi_type::UBIGINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
		return capi_type::HUGEINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
		return capi_type::UHUGEINT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
		return capi_type::FLOAT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
		return capi_type::DOUBLE;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
		return capi_type::TIMESTAMP;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
		return capi_type::TIMESTAMP_TZ;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
		return capi_type::TIMESTAMP_TZ_NS;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
		return capi_type::TIMESTAMP_S;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
		return capi_type::TIMESTAMP_MS;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
		return capi_type::TIMESTAMP_NS;
	case DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
		return capi_type::DATE;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
		return capi_type::TIME;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ:
		return capi_type::TIME_TZ;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
		return capi_type::TIME_NS;
	case DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
		return capi_type::VARCHAR;
	case DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
		return capi_type::BLOB;
	case DUCKDB_V2_LOGICAL_TYPE_ID_BIT:
		return capi_type::BIT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM:
		return capi_type::BIGNUM;
	case DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
		return capi_type::INTERVAL;
	case DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
		return capi_type::DECIMAL;
	case DUCKDB_V2_LOGICAL_TYPE_ID_ENUM:
		return capi_type::ENUM;
	case DUCKDB_V2_LOGICAL_TYPE_ID_LIST:
		return capi_type::LIST;
	case DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
		return capi_type::STRUCT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_MAP:
		return capi_type::MAP;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UNION:
		return capi_type::UNION;
	case DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
		return capi_type::UUID;
	case DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY:
		return capi_type::ARRAY;
	case DUCKDB_V2_LOGICAL_TYPE_ID_ANY:
		return capi_type::ANY;
	case DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL:
		return capi_type::SQLNULL;
	case DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY:
		return capi_type::GEOMETRY;
	case DUCKDB_V2_LOGICAL_TYPE_ID_GEOGRAPHY:
		return capi_type::GEOGRAPHY;
	case DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT:
		return capi_type::VARIANT;
	case DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE:
		// TUPLE is an unnamed struct - reported as STRUCT, same as in C API v1 (child names are empty)
		return capi_type::STRUCT;
	default:
		return capi_type::INVALID;
	}
}

bool type_id_from_capi(int32_t capi_type_id, DUCKDB_V2_LOGICAL_TYPE_ID &type_id) {
	switch (static_cast<capi_type>(capi_type_id)) {
	case capi_type::BOOLEAN:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN;
		return true;
	case capi_type::TINYINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT;
		return true;
	case capi_type::SMALLINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT;
		return true;
	case capi_type::INTEGER:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
		return true;
	case capi_type::BIGINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT;
		return true;
	case capi_type::UTINYINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT;
		return true;
	case capi_type::USMALLINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT;
		return true;
	case capi_type::UINTEGER:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER;
		return true;
	case capi_type::UBIGINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT;
		return true;
	case capi_type::HUGEINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT;
		return true;
	case capi_type::UHUGEINT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT;
		return true;
	case capi_type::FLOAT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT;
		return true;
	case capi_type::DOUBLE:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE;
		return true;
	case capi_type::TIMESTAMP:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP;
		return true;
	case capi_type::TIMESTAMP_TZ:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ;
		return true;
	case capi_type::TIMESTAMP_TZ_NS:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS;
		return true;
	case capi_type::TIMESTAMP_S:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC;
		return true;
	case capi_type::TIMESTAMP_MS:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS;
		return true;
	case capi_type::TIMESTAMP_NS:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS;
		return true;
	case capi_type::DATE:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_DATE;
		return true;
	case capi_type::TIME:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIME;
		return true;
	case capi_type::TIME_TZ:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ;
		return true;
	case capi_type::TIME_NS:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS;
		return true;
	case capi_type::VARCHAR:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;
		return true;
	case capi_type::BLOB:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_BLOB;
		return true;
	case capi_type::BIT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_BIT;
		return true;
	case capi_type::BIGNUM:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;
		return true;
	case capi_type::INTERVAL:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL;
		return true;
	case capi_type::DECIMAL:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;
		return true;
	case capi_type::ENUM:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_ENUM;
		return true;
	case capi_type::LIST:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_LIST;
		return true;
	case capi_type::STRUCT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
		return true;
	case capi_type::MAP:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_MAP;
		return true;
	case capi_type::UNION:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UNION;
		return true;
	case capi_type::UUID:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_UUID;
		return true;
	case capi_type::ARRAY:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY;
		return true;
	case capi_type::ANY:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_ANY;
		return true;
	case capi_type::GEOMETRY:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY;
		return true;
	case capi_type::GEOGRAPHY:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_GEOGRAPHY;
		return true;
	case capi_type::VARIANT:
		type_id = DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT;
		return true;
	default:
		// INVALID, SQLNULL, STRING_LITERAL, INTEGER_LITERAL cannot be created with the C API v2
		return false;
	}
}

v2_error::~v2_error() {
	if (handle != nullptr) {
		duckdb_v2_error_info_destroy(&handle);
	}
}

duckdb_v2_error_info_handle *v2_error::out() {
	return &handle;
}

DUCKDB_V2_ERROR v2_error::code() const {
	DUCKDB_V2_ERROR code = DUCKDB_V2_ERROR_API;
	if (handle != nullptr) {
		duckdb_v2_error_info_get_code(handle, &code);
	}
	return code;
}

std::string v2_error::message() const {
	if (handle == nullptr) {
		return "Unknown error";
	}
	const char *text = nullptr;
	idx_t text_length = 0;
	if (duckdb_v2_error_info_get_text(handle, &text, &text_length) != DUCKDB_V2_ERROR_NONE || text == nullptr) {
		return "Unknown error";
	}
	return std::string(text, text_length);
}

std::string v2_error::raw_message() const {
	if (handle != nullptr) {
		const char *text = nullptr;
		idx_t text_length = 0;
		if (duckdb_v2_error_info_get_raw_text(handle, &text, &text_length) == DUCKDB_V2_ERROR_NONE && text != nullptr &&
		    text_length > 0) {
			return std::string(text, text_length);
		}
	}
	return message();
}

duckdb_v2_factory_handle default_factory() {
	// Intentionally leaked: it must stay valid while types and chunks created with it are in use, including
	// at the time of the static destruction.
	static duckdb_v2_factory_handle factory = [] {
		duckdb_v2_environment_handle env = nullptr;
		if (duckdb_v2_environment_create(&env, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<duckdb_v2_factory_handle>(nullptr);
		}
		const char *option_names[] = {"threads"};
		idx_t option_names_lengths[] = {7};
		const char *option_values[] = {"1"};
		idx_t option_values_lengths[] = {1};
		duckdb_v2_instance_handle instance = nullptr;
		if (duckdb_v2_instance_create_with_options(env, option_names, option_names_lengths, option_values,
		                                           option_values_lengths, 1, &instance,
		                                           nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<duckdb_v2_factory_handle>(nullptr);
		}
		duckdb_v2_factory_handle res = nullptr;
		if (duckdb_v2_instance_get_factory(instance, &res, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<duckdb_v2_factory_handle>(nullptr);
		}
		return res;
	}();
	return factory;
}

static char invalid_logical_type_placeholder = 0;

duckdb_v2_logical_type_handle invalid_logical_type() {
	return reinterpret_cast<duckdb_v2_logical_type_handle>(&invalid_logical_type_placeholder);
}

capi_type logical_type_to_capi(duckdb_v2_logical_type_handle logical_type) {
	if (logical_type == nullptr || logical_type == invalid_logical_type()) {
		return capi_type::INVALID;
	}
	DUCKDB_V2_LOGICAL_TYPE_ID type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	if (duckdb_v2_logical_type_get_id(logical_type, &type_id, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return capi_type::INVALID;
	}
	return type_id_to_capi(type_id);
}

duckdb_v2_logical_type_handle copy_logical_type(duckdb_v2_logical_type_handle logical_type) {
	if (logical_type == nullptr || logical_type == invalid_logical_type()) {
		return nullptr;
	}
	duckdb_v2_logical_type_handle res = nullptr;
	if (duckdb_v2_logical_type_copy(logical_type, &res, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return res;
}

void destroy_logical_type(duckdb_v2_logical_type_handle logical_type) {
	if (logical_type == nullptr || logical_type == invalid_logical_type()) {
		return;
	}
	duckdb_v2_logical_type_destroy(&logical_type);
}

void set_callback_error(duckdb_v2_error_info_handle *err, DUCKDB_V2_ERROR code, const std::string &message) {
	if (err == nullptr || *err == nullptr) {
		return;
	}
	duckdb_v2_error_info_set_code(*err, code);
	duckdb_v2_error_info_set_text(*err, message.data(), message.size());
}

bool make_v2_bytes(duckdb_v2_arena_handle arena, const char *data, idx_t len, duckdb_v2_bytes &out) {
	std::memset(&out, 0, sizeof(out));
	if (len > UINT32_MAX) {
		return false;
	}
	if (len <= DUCKDB_V2_BYTES_INLINE_LENGTH) {
		out.value.inlined.length = static_cast<uint32_t>(len);
		if (len > 0) {
			std::memcpy(out.value.inlined.inlined, data, len);
		}
		return true;
	}
	uint8_t *ptr = nullptr;
	if (duckdb_v2_arena_allocate(arena, len, &ptr, nullptr) != DUCKDB_V2_ERROR_NONE || ptr == nullptr) {
		return false;
	}
	std::memcpy(ptr, data, len);
	out.value.pointer.length = static_cast<uint32_t>(len);
	std::memcpy(out.value.pointer.prefix, data, sizeof(out.value.pointer.prefix));
	out.value.pointer.ptr = reinterpret_cast<char *>(ptr);
	return true;
}

const char *v2_bytes_data(const duckdb_v2_bytes &bytes) {
	return bytes.value.inlined.length <= DUCKDB_V2_BYTES_INLINE_LENGTH ? bytes.value.inlined.inlined
	                                                                   : bytes.value.pointer.ptr;
}

idx_t v2_bytes_length(const duckdb_v2_bytes &bytes) {
	return bytes.value.inlined.length;
}

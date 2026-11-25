#include "types.hpp"

#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <limits>
#include <string>
#include <vector>

using duckdb::cxx::Factory;
using duckdb::cxx::LogicalTypeId;
using duckdb::cxx::Value;

std::string type_id_to_string(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::TYPE:
		return "TYPE";
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP WITH TIME ZONE";
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "TIMESTAMPTZ_NS";
	case LogicalTypeId::TIME_TZ:
		return "TIME WITH TIME ZONE";
	case LogicalTypeId::TIME_NS:
		return "TIME_NS";
	case LogicalTypeId::BIT:
		return "BIT";
	case LogicalTypeId::BIGNUM:
		return "BIGNUM";
	case LogicalTypeId::UHUGEINT:
		return "UHUGEINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::UNION:
		return "UNION";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	case LogicalTypeId::VARIANT:
		return "VARIANT";
	case LogicalTypeId::TUPLE:
		return "TUPLE";
	default:
		return "INVALID";
	}
}

std::string type_to_jduckdb_type(const duckdb::cxx::LogicalType &logical_type) {
	switch (logical_type.GetTypeId()) {
	case LogicalTypeId::DECIMAL: {
		uint32_t width = logical_type.GetDecimalWidth();
		uint32_t scale = logical_type.GetDecimalScale();
		std::string width_scale = std::to_string(width) + std::string(";") + std::to_string(scale);

		switch (logical_type.GetDecimalInternalTypeId()) {
		case LogicalTypeId::SMALLINT:
			return std::string("DECIMAL16;") + width_scale;
		case LogicalTypeId::INTEGER:
			return std::string("DECIMAL32;") + width_scale;
		case LogicalTypeId::BIGINT:
			return std::string("DECIMAL64;") + width_scale;
		case LogicalTypeId::HUGEINT:
			return std::string("DECIMAL128;") + width_scale;
		default:
			return std::string("no physical type found");
		}
	} break;
	default:
		// JSON requires special handling because it is mapped
		// to JsonNode class
		if (logical_type.GetName() == "JSON") {
			return std::string(logical_type.GetName());
		}
		return type_id_to_string(logical_type.GetTypeId());
	}
}

//! A NULL of the type that is cast to any other type
static Value create_null_value(Factory &factory) {
	return Value::CreateNull(factory, factory.ParseType("NULL"));
}

Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal, Factory &factory) {
	jint precision = env->CallIntMethod(decimal, J_BigDecimal_precision);
	check_java_exception_and_rethrow(env);
	jint scale = env->CallIntMethod(decimal, J_BigDecimal_scale);
	check_java_exception_and_rethrow(env);

	// Java BigDecimal type can have scale that exceeds the precision
	// Which our DECIMAL type does not support (assert(width >= scale))
	if (scale > precision) {
		precision = scale;
	}

	// DECIMAL scale is unsigned, so negative values are not supported
	if (scale < 0) {
		throw JdbcException("Invalid Input Error: Converting from a BigDecimal with negative scale is not supported");
	}

	if (precision <= 18) { // normal sizes -> avoid string processing
		jobject no_point_dec = env->CallObjectMethod(decimal, J_BigDecimal_scaleByPowTen, scale);
		check_java_exception_and_rethrow(env);
		jlong result = env->CallLongMethod(no_point_dec, J_BigDecimal_longValue);
		check_java_exception_and_rethrow(env);
		return Value::CreateDecimal(factory, duckdb::cxx::int128_t {static_cast<uint64_t>(result), result < 0 ? -1 : 0},
		                            static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
	} else if (precision <= 38) { // larger than int64 -> get string and cast
		jobject str_val = env->CallObjectMethod(decimal, J_BigDecimal_toPlainString);
		check_java_exception_and_rethrow(env);
		auto str = jstring_to_string(env, static_cast<jstring>(str_val));
		auto val = Value::Create(factory, duckdb::cxx::varchar_t(str));
		auto type = factory.ParseType("DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")");
		return val.Cast(factory, type);
	}
	// the precision is not supported, same as a NULL value
	return create_null_value(factory);
}

static Value create_value_from_hugeint(JNIEnv *env, jobject hugeint, Factory &factory) {
	jlong lower = env->GetLongField(hugeint, J_HugeInt_lower);
	jlong upper = env->GetLongField(hugeint, J_HugeInt_upper);
	return Value::Create(factory, duckdb::cxx::int128_t {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)});
}

static Value create_value_from_uuid(JNIEnv *env, jobject param, Factory &factory) {
	jlong most_significant = env->CallLongMethod(param, J_UUID_getMostSignificantBits);
	check_java_exception_and_rethrow(env);
	// Account for the following logic in UUID::FromString:
	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	most_significant ^= (std::numeric_limits<int64_t>::min)();
	jlong least_significant = env->CallLongMethod(param, J_UUID_getLeastSignificantBits);
	check_java_exception_and_rethrow(env);
	duckdb::cxx::int128_t storage {static_cast<uint64_t>(least_significant), static_cast<int64_t>(most_significant)};
	return Value::Create(factory, duckdb::cxx::uuid_t(storage));
}

static Value create_value_from_map(JNIEnv *env, jobject param, Factory &factory) {
	jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(param, J_DuckMap_getSQLTypeName));
	check_java_exception_and_rethrow(env);
	auto typeName = jstring_to_string(env, jstr);

	auto type = factory.ParseType(typeName);
	auto key_type = type.GetMapKeyType();
	auto value_type = type.GetMapValueType();

	auto entrySet = env->CallObjectMethod(param, J_Map_entrySet);
	check_java_exception_and_rethrow(env);
	auto iterator = env->CallObjectMethod(entrySet, J_Set_iterator);
	check_java_exception_and_rethrow(env);
	std::vector<std::pair<Value, Value>> entries;
	while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
		check_java_exception_and_rethrow(env);
		auto entry = env->CallObjectMethod(iterator, J_Iterator_next);
		check_java_exception_and_rethrow(env);

		auto key = env->CallObjectMethod(entry, J_Entry_getKey);
		check_java_exception_and_rethrow(env);
		auto value = env->CallObjectMethod(entry, J_Entry_getValue);
		check_java_exception_and_rethrow(env);

		entries.emplace_back(to_duckdb_value(env, key, factory).Cast(factory, key_type),
		                     to_duckdb_value(env, value, factory).Cast(factory, value_type));
	}
	check_java_exception_and_rethrow(env);

	if (entries.empty()) {
		return Value::CreateMap(factory, key_type, value_type);
	}
	return Value::CreateMap(factory, entries);
}

static Value create_value_from_struct(JNIEnv *env, jobject param, Factory &factory) {
	jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(param, J_Struct_getSQLTypeName));
	check_java_exception_and_rethrow(env);
	auto typeName = jstring_to_string(env, jstr);

	auto type = factory.ParseType(typeName);

	jobjectArray jvalues = reinterpret_cast<jobjectArray>(env->CallObjectMethod(param, J_Struct_getAttributes));
	check_java_exception_and_rethrow(env);

	int size = env->GetArrayLength(jvalues);

	std::vector<std::pair<std::string, Value>> values;

	for (int i = 0; i < size; i++) {
		auto name = type.GetStructChildName(static_cast<idx_t>(i));
		auto jvalue = env->GetObjectArrayElement(jvalues, i);
		auto value = to_duckdb_value(env, jvalue, factory);
		env->DeleteLocalRef(jvalue);
		values.emplace_back(name, value.Cast(factory, type.GetStructChildType(static_cast<idx_t>(i))));
	}

	return Value::CreateStruct(factory, values);
}

static Value create_value_from_array(JNIEnv *env, jobject param, Factory &factory) {
	jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(param, J_Array_getBaseTypeName));
	check_java_exception_and_rethrow(env);
	auto typeName = jstring_to_string(env, jstr);
	jobjectArray jvalues = reinterpret_cast<jobjectArray>(env->CallObjectMethod(param, J_Array_getArray));
	check_java_exception_and_rethrow(env);
	int size = env->GetArrayLength(jvalues);

	auto type = factory.ParseType(typeName);

	std::vector<Value> values;
	for (int i = 0; i < size; i++) {
		auto jvalue = env->GetObjectArrayElement(jvalues, i);
		auto value = to_duckdb_value(env, jvalue, factory);
		env->DeleteLocalRef(jvalue);
		values.emplace_back(value.Cast(factory, type));
	}

	if (values.empty()) {
		return Value::CreateList(factory, type);
	}
	return Value::CreateList(factory, values);
}

Value to_duckdb_value(JNIEnv *env, jobject param, Factory &factory) {
	param = env->CallStaticObjectMethod(J_Timestamp, J_Timestamp_valueOf, param);
	check_java_exception_and_rethrow(env);

	if (param == nullptr) {
		return create_null_value(factory);
	} else if (env->IsInstanceOf(param, J_Bool)) {
		jboolean val = env->CallBooleanMethod(param, J_Bool_booleanValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<bool>(val));
	} else if (env->IsInstanceOf(param, J_Byte)) {
		jbyte val = env->CallByteMethod(param, J_Byte_byteValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<int8_t>(val));
	} else if (env->IsInstanceOf(param, J_Short)) {
		jshort val = env->CallShortMethod(param, J_Short_shortValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<int16_t>(val));
	} else if (env->IsInstanceOf(param, J_Int)) {
		jint val = env->CallIntMethod(param, J_Int_intValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<int32_t>(val));
	} else if (env->IsInstanceOf(param, J_Long)) {
		jlong val = env->CallLongMethod(param, J_Long_longValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<int64_t>(val));
	} else if (env->IsInstanceOf(param, J_HugeInt)) {
		return create_value_from_hugeint(env, param, factory);
	} else if (env->IsInstanceOf(param, J_TimestampTZ)) { // Check for subclass before superclass!
		jlong val = env->CallLongMethod(param, J_TimestampTZ_getMicrosEpoch);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, duckdb::cxx::timestamp_tz_t {static_cast<int64_t>(val)});
	} else if (env->IsInstanceOf(param, J_DuckDBDate)) {
		jlong val = env->CallLongMethod(param, J_DuckDBDate_getDaysSinceEpoch);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, duckdb::cxx::date_t {static_cast<int32_t>(val)});
	} else if (env->IsInstanceOf(param, J_DuckDBTime)) {
		jlong val = env->CallLongMethod(param, J_Timestamp_getMicrosEpoch);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, duckdb::cxx::dtime_t {static_cast<int64_t>(val)});
	} else if (env->IsInstanceOf(param, J_TimestampNS)) { // Check for subclass before superclass!
		jlong val = env->CallLongMethod(param, J_TimestampNS_getNanosEpoch);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, duckdb::cxx::timestamp_ns_t {static_cast<int64_t>(val)});
	} else if (env->IsInstanceOf(param, J_Timestamp)) {
		jlong val = env->CallLongMethod(param, J_Timestamp_getMicrosEpoch);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, duckdb::cxx::timestamp_t {static_cast<int64_t>(val)});
	} else if (env->IsInstanceOf(param, J_Float)) {
		jfloat val = env->CallFloatMethod(param, J_Float_floatValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<float>(val));
	} else if (env->IsInstanceOf(param, J_Double)) {
		jdouble val = env->CallDoubleMethod(param, J_Double_doubleValue);
		check_java_exception_and_rethrow(env);
		return Value::Create(factory, static_cast<double>(val));
	} else if (env->IsInstanceOf(param, J_BigDecimal)) {
		return create_value_from_bigdecimal(env, param, factory);
	} else if (env->IsInstanceOf(param, J_String)) {
		auto param_string = jstring_to_string(env, (jstring)param);
		return Value::Create(factory, duckdb::cxx::varchar_t(param_string));
	} else if (env->IsInstanceOf(param, J_ByteArray)) {
		return Value::Create(factory, duckdb::cxx::blob_t(jbyteArray_to_string(env, (jbyteArray)param)));
	} else if (env->IsInstanceOf(param, J_UUID)) {
		return create_value_from_uuid(env, param, factory);
	} else if (env->IsInstanceOf(param, J_DuckMap)) {
		return create_value_from_map(env, param, factory);
	} else if (env->IsInstanceOf(param, J_Struct)) {
		return create_value_from_struct(env, param, factory);
	} else if (env->IsInstanceOf(param, J_Array)) {
		return create_value_from_array(env, param, factory);
	} else {
		throw JdbcException("Invalid Input Error: Unsupported parameter type");
	}
}

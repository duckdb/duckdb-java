#include "types.hpp"

#include "refs.hpp"
#include "util.hpp"

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

duckdb::Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal) {
	jint precision = env->CallIntMethod(decimal, J_BigDecimal_precision);
	jint scale = env->CallIntMethod(decimal, J_BigDecimal_scale);

	// Java BigDecimal type can have scale that exceeds the precision
	// Which our DECIMAL type does not support (assert(width >= scale))
	if (scale > precision) {
		precision = scale;
	}

	// DECIMAL scale is unsigned, so negative values are not supported
	if (scale < 0) {
		throw duckdb::InvalidInputException("Converting from a BigDecimal with negative scale is not supported");
	}

	duckdb::Value val;

	if (precision <= 18) { // normal sizes -> avoid string processing
		jobject no_point_dec = env->CallObjectMethod(decimal, J_BigDecimal_scaleByPowTen, scale);
		jlong result = env->CallLongMethod(no_point_dec, J_BigDecimal_longValue);
		val = duckdb::Value::DECIMAL((int64_t)result, (uint8_t)precision, (uint8_t)scale);
	} else if (precision <= 38) { // larger than int64 -> get string and cast
		jobject str_val = env->CallObjectMethod(decimal, J_BigDecimal_toPlainString);
		auto *str_char = env->GetStringUTFChars((jstring)str_val, 0);
		val = duckdb::Value(str_char);
		val = val.DefaultCastAs(duckdb::LogicalType::DECIMAL(precision, scale));
		env->ReleaseStringUTFChars((jstring)str_val, str_char);
	}

	return val;
}

static duckdb::Value create_value_from_hugeint(JNIEnv *env, jobject hugeint) {
	jlong lower = env->GetLongField(hugeint, J_HugeInt_lower);
	jlong upper = env->GetLongField(hugeint, J_HugeInt_upper);
	duckdb::hugeint_t hi(upper, lower);
	return duckdb::Value::HUGEINT(std::move(hi));
}

static duckdb::Value create_value_from_uuid(JNIEnv *env, jobject param) {
	jlong most_significant = env->CallLongMethod(param, J_UUID_getMostSignificantBits);
	// Account for the following logic in UUID::FromString:
	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	most_significant ^= (std::numeric_limits<int64_t>::min)();
	jlong least_significant = env->CallLongMethod(param, J_UUID_getLeastSignificantBits);
	duckdb::hugeint_t hi = duckdb::hugeint_t(most_significant, least_significant);
	return duckdb::Value::UUID(std::move(hi));
}

static duckdb::Value create_value_from_map(JNIEnv *env, jobject param, duckdb::ClientContext &context) {
	auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_DuckMap_getSQLTypeName));

	duckdb::LogicalType type;
	context.RunFunctionInTransaction([&]() { type = duckdb::TransformStringToLogicalType(typeName, context); });

	auto entrySet = env->CallObjectMethod(param, J_Map_entrySet);
	auto iterator = env->CallObjectMethod(entrySet, J_Set_iterator);
	duckdb::vector<duckdb::Value> entries;
	while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
		auto entry = env->CallObjectMethod(iterator, J_Iterator_next);

		auto key = env->CallObjectMethod(entry, J_Entry_getKey);
		auto value = env->CallObjectMethod(entry, J_Entry_getValue);
		D_ASSERT(key);
		D_ASSERT(value);

		entries.push_back(duckdb::Value::STRUCT(
		    {{"key", to_duckdb_value(env, key, context)}, {"value", to_duckdb_value(env, value, context)}}));
	}

	return duckdb::Value::MAP(duckdb::ListType::GetChildType(type), entries);
}

static duckdb::Value create_value_from_struct(JNIEnv *env, jobject param, duckdb::ClientContext &context) {
	auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_Struct_getSQLTypeName));

	duckdb::LogicalType type;
	context.RunFunctionInTransaction([&]() { type = TransformStringToLogicalType(typeName, context); });

	auto jvalues = (jobjectArray)env->CallObjectMethod(param, J_Struct_getAttributes);

	int size = env->GetArrayLength(jvalues);

	duckdb::child_list_t<duckdb::Value> values;

	for (int i = 0; i < size; i++) {
		auto name = duckdb::StructType::GetChildName(type, i);

		auto value = env->GetObjectArrayElement(jvalues, i);

		values.emplace_back(name, to_duckdb_value(env, value, context));
	}

	return duckdb::Value::STRUCT(std::move(values));
}

static duckdb::Value create_value_from_array(JNIEnv *env, jobject param, duckdb::ClientContext &context) {
	auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_Array_getBaseTypeName));
	auto jvalues = (jobjectArray)env->CallObjectMethod(param, J_Array_getArray);
	int size = env->GetArrayLength(jvalues);

	duckdb::LogicalType type;
	context.RunFunctionInTransaction([&]() { type = TransformStringToLogicalType(typeName, context); });

	duckdb::vector<duckdb::Value> values;
	for (int i = 0; i < size; i++) {
		auto value = env->GetObjectArrayElement(jvalues, i);

		values.emplace_back(to_duckdb_value(env, value, context));
	}

	return (duckdb::Value::LIST(type, values));
}

duckdb::Value to_duckdb_value(JNIEnv *env, jobject param, duckdb::ClientContext &context) {
	param = env->CallStaticObjectMethod(J_Timestamp, J_Timestamp_valueOf, param);

	if (param == nullptr) {
		return (duckdb::Value());
	} else if (env->IsInstanceOf(param, J_Bool)) {
		return (duckdb::Value::BOOLEAN(env->CallBooleanMethod(param, J_Bool_booleanValue)));
	} else if (env->IsInstanceOf(param, J_Byte)) {
		return (duckdb::Value::TINYINT(env->CallByteMethod(param, J_Byte_byteValue)));
	} else if (env->IsInstanceOf(param, J_Short)) {
		return (duckdb::Value::SMALLINT(env->CallShortMethod(param, J_Short_shortValue)));
	} else if (env->IsInstanceOf(param, J_Int)) {
		return (duckdb::Value::INTEGER(env->CallIntMethod(param, J_Int_intValue)));
	} else if (env->IsInstanceOf(param, J_Long)) {
		return (duckdb::Value::BIGINT(env->CallLongMethod(param, J_Long_longValue)));
	} else if (env->IsInstanceOf(param, J_HugeInt)) {
		return create_value_from_hugeint(env, param);
	} else if (env->IsInstanceOf(param, J_TimestampTZ)) { // Check for subclass before superclass!
		return (duckdb::Value::TIMESTAMPTZ(
		    (duckdb::timestamp_tz_t)env->CallLongMethod(param, J_TimestampTZ_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_DuckDBDate)) {
		return (duckdb::Value::DATE((duckdb::date_t)env->CallLongMethod(param, J_DuckDBDate_getDaysSinceEpoch)));
	} else if (env->IsInstanceOf(param, J_DuckDBTime)) {
		return (duckdb::Value::TIME((duckdb::dtime_t)env->CallLongMethod(param, J_Timestamp_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_Timestamp)) {
		return (duckdb::Value::TIMESTAMP((duckdb::timestamp_t)env->CallLongMethod(param, J_Timestamp_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_Float)) {
		return (duckdb::Value::FLOAT(env->CallFloatMethod(param, J_Float_floatValue)));
	} else if (env->IsInstanceOf(param, J_Double)) {
		return (duckdb::Value::DOUBLE(env->CallDoubleMethod(param, J_Double_doubleValue)));
	} else if (env->IsInstanceOf(param, J_BigDecimal)) {
		return create_value_from_bigdecimal(env, param);
	} else if (env->IsInstanceOf(param, J_String)) {
		auto param_string = jstring_to_string(env, (jstring)param);
		return (duckdb::Value(param_string));
	} else if (env->IsInstanceOf(param, J_ByteArray)) {
		return (duckdb::Value::BLOB_RAW(byte_array_to_string(env, (jbyteArray)param)));
	} else if (env->IsInstanceOf(param, J_UUID)) {
		return create_value_from_uuid(env, param);
	} else if (env->IsInstanceOf(param, J_DuckMap)) {
		return create_value_from_map(env, param, context);
	} else if (env->IsInstanceOf(param, J_Struct)) {
		return create_value_from_struct(env, param, context);
	} else if (env->IsInstanceOf(param, J_Array)) {
		return create_value_from_array(env, param, context);
	} else {
		throw duckdb::InvalidInputException("Unsupported parameter type");
	}
}

#include "config.hpp"

#include "duckdb/common/virtual_file_system.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <stdexcept>
#include <string>

static duckdb::Value jobj_to_value(JNIEnv *env, const std::string &key, jobject jval) {
	// On the right in comments are all types that are currently present
	// in DuckDB config.
	if (nullptr == jval) {
		return duckdb::Value();

	} else if (env->IsInstanceOf(jval, J_Bool)) { // BOOLEAN
		jboolean val = env->CallBooleanMethod(jval, J_Bool_booleanValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::BOOLEAN(val);

	} else if (env->IsInstanceOf(jval, J_Byte)) { // UBIGINT
		jbyte val = env->CallByteMethod(jval, J_Byte_byteValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::TINYINT(val);

	} else if (env->IsInstanceOf(jval, J_Short)) { // UBIGINT
		jshort val = env->CallShortMethod(jval, J_Short_shortValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::SMALLINT(val);

	} else if (env->IsInstanceOf(jval, J_Int)) { // UBIGINT
		jint val = env->CallIntMethod(jval, J_Int_intValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::INTEGER(val);

	} else if (env->IsInstanceOf(jval, J_Long)) { // UBIGINT
		jlong val = env->CallLongMethod(jval, J_Long_longValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::BIGINT(val);

	} else if (env->IsInstanceOf(jval, J_Float)) { // FLOAT
		jfloat val = env->CallFloatMethod(jval, J_Float_floatValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::FLOAT(val);

	} else if (env->IsInstanceOf(jval, J_Double)) { // DOUBLE
		jdouble val = env->CallDoubleMethod(jval, J_Double_doubleValue);
		check_java_exception_and_rethrow(env);
		return duckdb::Value::DOUBLE(val);

	} else if (env->IsInstanceOf(jval, J_String)) { // VARCHAR
		std::string val = jstring_to_string(env, reinterpret_cast<jstring>(jval));
		return duckdb::Value(val);

	} else if (env->IsInstanceOf(jval, J_List)) { // VARCHAR[]
		jobject iterator = env->CallObjectMethod(jval, J_List_iterator);
		check_java_exception_and_rethrow(env);

		duckdb::vector<duckdb::Value> vec;
		while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
			check_java_exception_and_rethrow(env);
			jobject list_entry = env->CallObjectMethod(iterator, J_Iterator_next);
			check_java_exception_and_rethrow(env);
			// all list entries are coalesced to string
			jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(list_entry, J_Object_toString));
			check_java_exception_and_rethrow(env);
			std::string sval = jstring_to_string(env, jstr);
			duckdb::Value val(std::move(sval));
			vec.push_back(std::move(val));
		}
		return duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(vec));

	} else {
		// coalesce to string the entry with an unknown type
		jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(jval, J_Object_toString));
		check_java_exception_and_rethrow(env);
		std::string str = jstring_to_string(env, jstr);
		return duckdb::Value(str);
	}
}

std::unique_ptr<duckdb::DBConfig> create_db_config(JNIEnv *env, jboolean read_only, jobject java_config) {
	auto config = std::unique_ptr<duckdb::DBConfig>(new duckdb::DBConfig());
	// Required for setting like 'allowed_directories' that use
	// file separator when checking the property value.
	config->file_system = duckdb::make_uniq<duckdb::VirtualFileSystem>();
	config->SetOptionByName("duckdb_api", "java");
	config->AddExtensionOption(
	    "jdbc_stream_results",
	    "Whether to stream results. Only one ResultSet on a connection can be open at once when true",
	    duckdb::LogicalType::BOOLEAN);
	if (read_only) {
		config->options.access_mode = duckdb::AccessMode::READ_ONLY;
	}
	jobject entry_set = env->CallObjectMethod(java_config, J_Map_entrySet);
	check_java_exception_and_rethrow(env);
	jobject iterator = env->CallObjectMethod(entry_set, J_Set_iterator);
	check_java_exception_and_rethrow(env);

	while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
		check_java_exception_and_rethrow(env);
		jobject pair = env->CallObjectMethod(iterator, J_Iterator_next);
		check_java_exception_and_rethrow(env);
		jobject key = env->CallObjectMethod(pair, J_Entry_getKey);
		check_java_exception_and_rethrow(env);
		jobject value = env->CallObjectMethod(pair, J_Entry_getValue);
		check_java_exception_and_rethrow(env);

		jstring key_jstr = reinterpret_cast<jstring>(env->CallObjectMethod(key, J_Object_toString));
		check_java_exception_and_rethrow(env);
		std::string key_str = jstring_to_string(env, key_jstr);

		duckdb::Value dvalue = jobj_to_value(env, key_str, value);

		try {
			config->SetOptionByName(key_str, dvalue);
		} catch (const std::exception &e) {
			duckdb::ErrorData error(e);
			throw duckdb::CatalogException("Failed to set configuration option \"%s\", error: %s", key_str,
			                               error.RawMessage());
		}
	}

	return config;
}

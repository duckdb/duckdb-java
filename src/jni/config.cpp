#include "config.hpp"

#include "arrow.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>
#include <vector>

static const char *const JDBC_STREAM_RESULTS_OPTION = "jdbc_stream_results";

//! The options that need a running instance, they cannot be given at startup
static bool is_runtime_option(const std::string &name) {
	static const std::unordered_set<std::string> runtime_options = {
	    "allowed_directories", "allowed_paths",   "disabled_filesystems", "enable_logging",    "logging_level",
	    "logging_mode",        "logging_storage", "enabled_log_types",    "disabled_log_types"};
	std::string lower = name;
	std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
	return runtime_options.count(lower) > 0;
}

static std::string quote_list_entry(const std::string &entry) {
	std::string res = "'";
	for (char c : entry) {
		if (c == '\'') {
			res += "''";
		} else {
			res += c;
		}
	}
	res += "'";
	return res;
}

static std::string format_double(double value) {
	char buf[64];
	std::snprintf(buf, sizeof(buf), "%.17g", value);
	return std::string(buf);
}

//! The text of a Java configuration value in the form that is accepted by SET, the empty optional is for nulls
static bool jobj_to_text(JNIEnv *env, jobject jval, std::string &res) {
	// On the right in comments are all types that are currently present
	// in DuckDB config.
	if (nullptr == jval) {
		return false;

	} else if (env->IsInstanceOf(jval, J_Bool)) { // BOOLEAN
		jboolean val = env->CallBooleanMethod(jval, J_Bool_booleanValue);
		check_java_exception_and_rethrow(env);
		res = val ? "true" : "false";

	} else if (env->IsInstanceOf(jval, J_Byte)) {
		jbyte val = env->CallByteMethod(jval, J_Byte_byteValue);
		check_java_exception_and_rethrow(env);
		res = std::to_string(static_cast<int>(val));

	} else if (env->IsInstanceOf(jval, J_Short)) {
		jshort val = env->CallShortMethod(jval, J_Short_shortValue);
		check_java_exception_and_rethrow(env);
		res = std::to_string(static_cast<int>(val));

	} else if (env->IsInstanceOf(jval, J_Int)) {
		jint val = env->CallIntMethod(jval, J_Int_intValue);
		check_java_exception_and_rethrow(env);
		res = std::to_string(static_cast<int>(val));

	} else if (env->IsInstanceOf(jval, J_Long)) {
		jlong val = env->CallLongMethod(jval, J_Long_longValue);
		check_java_exception_and_rethrow(env);
		res = std::to_string(static_cast<long long>(val));

	} else if (env->IsInstanceOf(jval, J_Float)) {
		jfloat val = env->CallFloatMethod(jval, J_Float_floatValue);
		check_java_exception_and_rethrow(env);
		res = format_double(static_cast<double>(val));

	} else if (env->IsInstanceOf(jval, J_Double)) {
		jdouble val = env->CallDoubleMethod(jval, J_Double_doubleValue);
		check_java_exception_and_rethrow(env);
		res = format_double(static_cast<double>(val));

	} else if (env->IsInstanceOf(jval, J_String)) {
		res = jstring_to_string(env, reinterpret_cast<jstring>(jval));

	} else if (env->IsInstanceOf(jval, J_List)) { // VARCHAR[]
		jobject iterator = env->CallObjectMethod(jval, J_List_iterator);
		check_java_exception_and_rethrow(env);

		res = "[";
		bool first = true;
		while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
			check_java_exception_and_rethrow(env);
			jobject list_entry = env->CallObjectMethod(iterator, J_Iterator_next);
			check_java_exception_and_rethrow(env);
			// all list entries are coalesced to string
			jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(list_entry, J_Object_toString));
			check_java_exception_and_rethrow(env);
			if (!first) {
				res += ", ";
			}
			first = false;
			res += quote_list_entry(jstring_to_string(env, jstr));
		}
		check_java_exception_and_rethrow(env);
		res += "]";

	} else {
		// coalesce to string the entry with an unknown type
		jstring jstr = reinterpret_cast<jstring>(env->CallObjectMethod(jval, J_Object_toString));
		check_java_exception_and_rethrow(env);
		res = jstring_to_string(env, jstr);
	}
	return true;
}

//! The database instances that are opened with the instance cache enabled, they are identified by the database path
static std::string instance_cache_key(const std::string &database) {
	std::error_code ec;
	auto path = std::filesystem::absolute(std::filesystem::path(database), ec);
	if (ec) {
		return database;
	}
	return path.lexically_normal().string();
}

static void register_stream_results_option(cxx::Instance &instance) {
	auto conn = instance.Connect();
	auto &factory = conn.GetFactory();
	auto option = cxx::CustomOption::Create(factory);
	option.SetName(JDBC_STREAM_RESULTS_OPTION)
	    .SetType(factory.CreateType(cxx::LogicalTypeId::BOOLEAN))
	    .SetDefaultValue(cxx::Value::Create(factory, false))
	    .SetDescription("Whether to stream results. Only one ResultSet on a connection can be open at once when true")
	    .SetDefaultScope(cxx::SettingScope::SESSION)
	    .SetSupportsScope(cxx::SettingScope::GLOBAL, true)
	    .SetSupportsScope(cxx::SettingScope::SESSION, true);
	conn.Register(option);
}

std::shared_ptr<InstanceHolder> open_instance(JNIEnv *env, const std::string &database, jboolean read_only,
                                              jobject java_config, bool cache_instance) {
	cxx::StartupOptions startup;
	std::vector<std::pair<std::string, std::string>> runtime_options;
	std::vector<std::pair<std::string, std::string>> compat_options;
	startup.Set("duckdb_api", "java");
	if (read_only) {
		startup.Set("access_mode", "READ_ONLY");
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
		env->DeleteLocalRef(key);
		check_java_exception_and_rethrow(env);
		std::string key_str = jstring_to_string(env, key_jstr);
		env->DeleteLocalRef(key_jstr);

		std::string value_str;
		bool has_value = jobj_to_text(env, value, value_str);
		env->DeleteLocalRef(value);
		if (!has_value) {
			continue;
		}

		if (key_str == JDBC_STREAM_RESULTS_OPTION || is_runtime_option(key_str)) {
			runtime_options.emplace_back(key_str, value_str);
		} else {
			startup.Set(key_str, value_str);
			compat_options.emplace_back(key_str, value_str);
		}
	}
	check_java_exception_and_rethrow(env);

	// settings that cannot be given at startup are written when the instance is created
	auto init = [&runtime_options](cxx::Instance &instance) {
		register_stream_results_option(instance);
		register_arrow_scan_function(instance);
		for (auto &option : runtime_options) {
			try {
				instance.GetConfig().SetOption(option.first, option.second, cxx::SettingScope::GLOBAL);
			} catch (const std::exception &e) {
				throw JdbcException("Catalog Error: Failed to set configuration option \"" + option.first +
				                    "\", error: " + e.what());
			}
		}
	};

	bool should_cache = cache_instance && database != ":memory:" && !database.empty();
	std::shared_ptr<InstanceHolder> holder;
	if (should_cache) {
		try {
			holder = std::make_shared<InstanceHolder>(cxx::Instance::OpenShared(database, startup, init));
		} catch (const cxx::Exception &e) {
			throw JdbcException(std::string("Connection Error: ") + e.what());
		}
		holder->key = instance_cache_key(database);
	} else {
		auto environment = std::make_unique<cxx::Environment>();
		auto &env_ref = *environment;
		const std::string memory_prefix = ":memory:";
		if (database.size() > memory_prefix.size() && database.compare(0, memory_prefix.size(), memory_prefix) == 0) {
			// a named in-memory database
			cxx::Instance instance(env_ref, startup);
			instance.Attach(memory_prefix, database.substr(memory_prefix.size()), {}, true);
			holder = std::make_shared<InstanceHolder>(std::move(environment), std::move(instance));
		} else {
			holder =
			    std::make_shared<InstanceHolder>(std::move(environment), cxx::Instance(env_ref, database, startup));
		}
		init(holder->instance);
	}
	holder->read_only = read_only;
	std::sort(compat_options.begin(), compat_options.end());
	holder->options = std::move(compat_options);
	return holder;
}

jlong instance_address(const InstanceHolder &holder) {
	if (!holder.key.empty()) {
		return static_cast<jlong>(std::hash<std::string>()(holder.key) | 1);
	}
	return reinterpret_cast<jlong>(&holder);
}

#include "config.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "functions.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "util.hpp"

#include <cstdint>
#include <limits>

using namespace duckdb;
using namespace std;

static jint JNI_VERSION = JNI_VERSION_1_6;

void ThrowJNI(JNIEnv *env, const char *message) {
	D_ASSERT(J_SQLException);
	env->ThrowNew(J_SQLException, message);
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return JNI_ERR;
	}

	try {
		create_refs(env);
	} catch (const std::exception &e) {
		if (!env->ExceptionCheck()) {
			auto re_class = env->FindClass("java/lang/RuntimeException");
			if (nullptr != re_class) {
				env->ThrowNew(re_class, e.what());
			}
		}
		return JNI_ERR;
	}

	return JNI_VERSION;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return;
	}
	delete_global_refs(env);
}

/**
 * Associates a duckdb::Connection with a duckdb::DuckDB. The DB may be shared amongst many ConnectionHolders, but the
 * Connection is unique to this holder. Every Java DuckDBConnection has exactly 1 of these holders, and they are never
 * shared. The holder is freed when the DuckDBConnection is closed. When the last holder sharing a DuckDB is freed, the
 * DuckDB is released as well.
 */
struct ConnectionHolder {
	const duckdb::shared_ptr<duckdb::DuckDB> db;
	const duckdb::unique_ptr<duckdb::Connection> connection;

	ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> _db)
	    : db(_db), connection(make_uniq<duckdb::Connection>(*_db)) {
	}
};

/**
 * Throws a SQLException and returns nullptr if a valid Connection can't be retrieved from the buffer.
 */
static Connection *get_connection(JNIEnv *env, jobject conn_ref_buf) {
	if (!conn_ref_buf) {
		throw ConnectionException("Invalid connection");
	}
	auto conn_holder = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_holder) {
		throw ConnectionException("Invalid connection");
	}
	auto conn_ref = conn_holder->connection.get();
	if (!conn_ref || !conn_ref->context) {
		throw ConnectionException("Invalid connection");
	}

	return conn_ref;
}

//! The database instance cache, used so that multiple connections to the same file point to the same database object
duckdb::DBInstanceCache instance_cache;

jobject _duckdb_jdbc_startup(JNIEnv *env, jclass, jbyteArray database_j, jboolean read_only, jobject props) {
	auto database = byte_array_to_string(env, database_j);
	std::unique_ptr<DBConfig> config = create_db_config(env, read_only, props);
	bool cache_instance = database != ":memory:" && !database.empty();
	auto shared_db = instance_cache.GetOrCreateInstance(database, *config, cache_instance);
	auto conn_holder = new ConnectionHolder(shared_db);

	return env->NewDirectByteBuffer(conn_holder, 0);
}

jobject _duckdb_jdbc_connect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	auto config = ClientConfig::GetConfig(*conn_ref->connection->context);
	auto conn = new ConnectionHolder(conn_ref->db);
	conn->connection->context->config = config;
	return env->NewDirectByteBuffer(conn, 0);
}

jstring _duckdb_jdbc_get_schema(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();

	return env->NewStringUTF(entry.schema.c_str());
}

static void set_catalog_search_path(JNIEnv *env, jobject conn_ref_buf, CatalogSearchEntry search_entry) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}

	conn_ref->context->RunFunctionInTransaction([&]() {
		ClientData::Get(*conn_ref->context).catalog_search_path->Set(search_entry, CatalogSetPathType::SET_SCHEMA);
	});
}

void _duckdb_jdbc_set_schema(JNIEnv *env, jclass, jobject conn_ref_buf, jstring schema) {
	set_catalog_search_path(env, conn_ref_buf, CatalogSearchEntry(INVALID_CATALOG, jstring_to_string(env, schema)));
}

void _duckdb_jdbc_set_catalog(JNIEnv *env, jclass, jobject conn_ref_buf, jstring catalog) {
	set_catalog_search_path(env, conn_ref_buf, CatalogSearchEntry(jstring_to_string(env, catalog), DEFAULT_SCHEMA));
}

jstring _duckdb_jdbc_get_catalog(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();
	if (entry.catalog == INVALID_CATALOG) {
		entry.catalog = DatabaseManager::GetDefaultDatabase(*conn_ref->context);
	}

	return env->NewStringUTF(entry.catalog.c_str());
}

void _duckdb_jdbc_set_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf, jboolean auto_commit) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}
	conn_ref->context->RunFunctionInTransaction([&]() { conn_ref->SetAutoCommit(auto_commit); });
}

jboolean _duckdb_jdbc_get_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return false;
	}
	return conn_ref->IsAutoCommit();
}

void _duckdb_jdbc_interrupt(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}
	conn_ref->Interrupt();
}

void _duckdb_jdbc_disconnect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

struct StatementHolder {
	duckdb::unique_ptr<PreparedStatement> stmt;
};

#include "utf8proc_wrapper.hpp"

jobject _duckdb_jdbc_prepare(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray query_j) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto query = byte_array_to_string(env, query_j);

	auto statements = conn_ref->ExtractStatements(query.c_str());
	if (statements.empty()) {
		throw InvalidInputException("No statements to execute.");
	}

	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (idx_t i = 0; i + 1 < statements.size(); i++) {
		auto res = conn_ref->Query(std::move(statements[i]));
		if (res->HasError()) {
			res->ThrowError();
		}
	}

	auto stmt_ref = new StatementHolder();
	stmt_ref->stmt = conn_ref->Prepare(std::move(statements.back()));
	if (stmt_ref->stmt->HasError()) {
		string error_msg = string(stmt_ref->stmt->GetError());
		stmt_ref->stmt = nullptr;

		// No success, so it must be deleted
		delete stmt_ref;
		ThrowJNI(env, error_msg.c_str());

		// Just return control flow back to JVM, as an Exception is pending anyway
		return nullptr;
	}
	return env->NewDirectByteBuffer(stmt_ref, 0);
}

struct ResultHolder {
	duckdb::unique_ptr<QueryResult> res;
	duckdb::unique_ptr<DataChunk> chunk;
};

Value ToValue(JNIEnv *env, jobject param, duckdb::shared_ptr<ClientContext> context) {
	param = env->CallStaticObjectMethod(J_Timestamp, J_Timestamp_valueOf, param);

	if (param == nullptr) {
		return (Value());
	} else if (env->IsInstanceOf(param, J_Bool)) {
		return (Value::BOOLEAN(env->CallBooleanMethod(param, J_Bool_booleanValue)));
	} else if (env->IsInstanceOf(param, J_Byte)) {
		return (Value::TINYINT(env->CallByteMethod(param, J_Byte_byteValue)));
	} else if (env->IsInstanceOf(param, J_Short)) {
		return (Value::SMALLINT(env->CallShortMethod(param, J_Short_shortValue)));
	} else if (env->IsInstanceOf(param, J_Int)) {
		return (Value::INTEGER(env->CallIntMethod(param, J_Int_intValue)));
	} else if (env->IsInstanceOf(param, J_Long)) {
		return (Value::BIGINT(env->CallLongMethod(param, J_Long_longValue)));
	} else if (env->IsInstanceOf(param, J_TimestampTZ)) { // Check for subclass before superclass!
		return (Value::TIMESTAMPTZ((timestamp_tz_t)env->CallLongMethod(param, J_TimestampTZ_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_DuckDBDate)) {
		return (Value::DATE((date_t)env->CallLongMethod(param, J_DuckDBDate_getDaysSinceEpoch)));

	} else if (env->IsInstanceOf(param, J_DuckDBTime)) {
		return (Value::TIME((dtime_t)env->CallLongMethod(param, J_Timestamp_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_Timestamp)) {
		return (Value::TIMESTAMP((timestamp_t)env->CallLongMethod(param, J_Timestamp_getMicrosEpoch)));
	} else if (env->IsInstanceOf(param, J_Float)) {
		return (Value::FLOAT(env->CallFloatMethod(param, J_Float_floatValue)));
	} else if (env->IsInstanceOf(param, J_Double)) {
		return (Value::DOUBLE(env->CallDoubleMethod(param, J_Double_doubleValue)));
	} else if (env->IsInstanceOf(param, J_Decimal)) {
		Value val = create_value_from_bigdecimal(env, param);
		return (val);
	} else if (env->IsInstanceOf(param, J_String)) {
		auto param_string = jstring_to_string(env, (jstring)param);
		return (Value(param_string));
	} else if (env->IsInstanceOf(param, J_ByteArray)) {
		return (Value::BLOB_RAW(byte_array_to_string(env, (jbyteArray)param)));
	} else if (env->IsInstanceOf(param, J_UUID)) {
		auto most_significant = (jlong)env->CallObjectMethod(param, J_UUID_getMostSignificantBits);
		// Account for the following logic in UUID::FromString:
		// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
		most_significant ^= (std::numeric_limits<int64_t>::min)();
		auto least_significant = (jlong)env->CallObjectMethod(param, J_UUID_getLeastSignificantBits);
		return (Value::UUID(hugeint_t(most_significant, least_significant)));
	} else if (env->IsInstanceOf(param, J_DuckMap)) {
		auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_DuckMap_getSQLTypeName));

		LogicalType type;
		context->RunFunctionInTransaction([&]() { type = TransformStringToLogicalType(typeName, *context); });

		auto entrySet = env->CallObjectMethod(param, J_Map_entrySet);
		auto iterator = env->CallObjectMethod(entrySet, J_Set_iterator);
		duckdb::vector<Value> entries;
		while (env->CallBooleanMethod(iterator, J_Iterator_hasNext)) {
			auto entry = env->CallObjectMethod(iterator, J_Iterator_next);

			auto key = env->CallObjectMethod(entry, J_Entry_getKey);
			auto value = env->CallObjectMethod(entry, J_Entry_getValue);
			D_ASSERT(key);
			D_ASSERT(value);

			entries.push_back(
			    Value::STRUCT({{"key", ToValue(env, key, context)}, {"value", ToValue(env, value, context)}}));
		}

		return (Value::MAP(ListType::GetChildType(type), entries));

	} else if (env->IsInstanceOf(param, J_Struct)) {
		auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_Struct_getSQLTypeName));

		LogicalType type;
		context->RunFunctionInTransaction([&]() { type = TransformStringToLogicalType(typeName, *context); });

		auto jvalues = (jobjectArray)env->CallObjectMethod(param, J_Struct_getAttributes);

		int size = env->GetArrayLength(jvalues);

		child_list_t<Value> values;

		for (int i = 0; i < size; i++) {
			auto name = StructType::GetChildName(type, i);

			auto value = env->GetObjectArrayElement(jvalues, i);

			values.emplace_back(name, ToValue(env, value, context));
		}

		return (Value::STRUCT(std::move(values)));
	} else if (env->IsInstanceOf(param, J_Array)) {
		auto typeName = jstring_to_string(env, (jstring)env->CallObjectMethod(param, J_Array_getBaseTypeName));
		auto jvalues = (jobjectArray)env->CallObjectMethod(param, J_Array_getArray);
		int size = env->GetArrayLength(jvalues);

		LogicalType type;
		context->RunFunctionInTransaction([&]() { type = TransformStringToLogicalType(typeName, *context); });

		duckdb::vector<Value> values;
		for (int i = 0; i < size; i++) {
			auto value = env->GetObjectArrayElement(jvalues, i);

			values.emplace_back(ToValue(env, value, context));
		}

		return (Value::LIST(type, values));

	} else {
		throw InvalidInputException("Unsupported parameter type");
	}
}

jobject _duckdb_jdbc_execute(JNIEnv *env, jclass, jobject stmt_ref_buf, jobjectArray params) {
	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref) {
		throw InvalidInputException("Invalid statement");
	}

	auto res_ref = make_uniq<ResultHolder>();
	duckdb::vector<Value> duckdb_params;

	idx_t param_len = env->GetArrayLength(params);

	if (param_len != stmt_ref->stmt->named_param_map.size()) {
		throw InvalidInputException("Parameter count mismatch");
	}

	auto &context = stmt_ref->stmt->context;

	if (param_len > 0) {
		for (idx_t i = 0; i < param_len; i++) {
			auto param = env->GetObjectArrayElement(params, i);
			duckdb_params.push_back(ToValue(env, param, context));
		}
	}

	Value result;
	bool stream_results =
	    stmt_ref->stmt->context->TryGetCurrentSetting("jdbc_stream_results", result) ? result.GetValue<bool>() : false;

	res_ref->res = stmt_ref->stmt->Execute(duckdb_params, stream_results);
	if (res_ref->res->HasError()) {
		string error_msg = string(res_ref->res->GetError());
		res_ref->res = nullptr;
		ThrowJNI(env, error_msg.c_str());
		return nullptr;
	}
	return env->NewDirectByteBuffer(res_ref.release(), 0);
}

void _duckdb_jdbc_release(JNIEnv *env, jclass, jobject stmt_ref_buf) {
	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (stmt_ref) {
		delete stmt_ref;
	}
}

void _duckdb_jdbc_free_result(JNIEnv *env, jclass, jobject res_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (res_ref) {
		delete res_ref;
	}
}

static jobject build_meta(JNIEnv *env, size_t column_count, size_t n_param, const duckdb::vector<string> &names,
                          const duckdb::vector<LogicalType> &types, StatementProperties properties,
                          const duckdb::vector<LogicalType> &param_types) {
	auto name_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_detail_array = env->NewObjectArray(column_count, J_String, nullptr);

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		std::string col_name;
		if (types[col_idx].id() == LogicalTypeId::ENUM) {
			col_name = "ENUM";
		} else {
			col_name = types[col_idx].ToString();
		}

		env->SetObjectArrayElement(name_array, col_idx,
		                           decode_charbuffer_to_jstring(env, names[col_idx].c_str(), names[col_idx].length()));
		env->SetObjectArrayElement(type_array, col_idx, env->NewStringUTF(col_name.c_str()));
		env->SetObjectArrayElement(type_detail_array, col_idx,
		                           env->NewStringUTF(type_to_jduckdb_type(types[col_idx]).c_str()));
	}

	auto param_type_array = env->NewObjectArray(n_param, J_String, nullptr);
	auto param_type_detail_array = env->NewObjectArray(n_param, J_String, nullptr);

	for (idx_t param_idx = 0; param_idx < n_param; param_idx++) {
		std::string param_name;
		if (param_types[param_idx].id() == LogicalTypeId::ENUM) {
			param_name = "ENUM";
		} else {
			param_name = param_types[param_idx].ToString();
		}

		env->SetObjectArrayElement(param_type_array, param_idx, env->NewStringUTF(param_name.c_str()));
		env->SetObjectArrayElement(param_type_detail_array, param_idx,
		                           env->NewStringUTF(type_to_jduckdb_type(param_types[param_idx]).c_str()));
	}

	auto return_type = env->NewStringUTF(StatementReturnTypeToString(properties.return_type).c_str());

	return env->NewObject(J_DuckResultSetMeta, J_DuckResultSetMeta_init, n_param, column_count, name_array, type_array,
	                      type_detail_array, return_type, param_type_array, param_type_detail_array);
}

jobject _duckdb_jdbc_query_result_meta(JNIEnv *env, jclass, jobject res_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}
	auto &result = res_ref->res;

	auto n_param = 0; // no params now
	duckdb::vector<LogicalType> param_types(n_param);

	return build_meta(env, result->ColumnCount(), n_param, result->names, result->types, result->properties,
	                  param_types);
}

jobject _duckdb_jdbc_prepared_statement_meta(JNIEnv *env, jclass, jobject stmt_ref_buf) {

	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref || !stmt_ref->stmt || stmt_ref->stmt->HasError()) {
		throw InvalidInputException("Invalid statement");
	}

	auto &stmt = stmt_ref->stmt;
	auto n_param = stmt->named_param_map.size();
	duckdb::vector<LogicalType> param_types(n_param);
	if (n_param > 0) {
		auto expected_parameter_types = stmt->GetExpectedParameterTypes();
		for (auto &it : stmt->named_param_map) {
			param_types[it.second - 1] = expected_parameter_types[it.first];
		}
	}

	return build_meta(env, stmt->ColumnCount(), n_param, stmt->GetNames(), stmt->GetTypes(),
	                  stmt->GetStatementProperties(), param_types);
}

jobject ProcessVector(JNIEnv *env, Connection *conn_ref, Vector &vec, idx_t row_count);

jobjectArray _duckdb_jdbc_fetch(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}

	auto conn_ref = get_connection(env, conn_ref_buf);
	if (conn_ref == nullptr) {
		return nullptr;
	}

	res_ref->chunk = res_ref->res->Fetch();
	if (!res_ref->chunk) {
		res_ref->chunk = make_uniq<DataChunk>();
	}
	auto row_count = res_ref->chunk->size();
	auto vec_array = (jobjectArray)env->NewObjectArray(res_ref->chunk->ColumnCount(), J_DuckVector, nullptr);

	for (idx_t col_idx = 0; col_idx < res_ref->chunk->ColumnCount(); col_idx++) {
		auto &vec = res_ref->chunk->data[col_idx];

		auto jvec = ProcessVector(env, conn_ref, vec, row_count);

		env->SetObjectArrayElement(vec_array, col_idx, jvec);
	}

	return vec_array;
}

jobject ProcessVector(JNIEnv *env, Connection *conn_ref, Vector &vec, idx_t row_count) {
	auto type_str = env->NewStringUTF(type_to_jduckdb_type(vec.GetType()).c_str());
	// construct nullmask
	auto null_array = env->NewBooleanArray(row_count);
	jboolean *null_unique_array = env->GetBooleanArrayElements(null_array, nullptr);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		null_unique_array[row_idx] = FlatVector::IsNull(vec, row_idx);
	}
	env->ReleaseBooleanArrayElements(null_array, null_unique_array, 0);

	auto jvec = env->NewObject(J_DuckVector, J_DuckVector_init, type_str, (int)row_count, null_array);

	jobject constlen_data = nullptr;
	jobjectArray varlen_data = nullptr;

	switch (vec.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(bool));
		break;
	case LogicalTypeId::TINYINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int8_t));
		break;
	case LogicalTypeId::SMALLINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int16_t));
		break;
	case LogicalTypeId::INTEGER:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int32_t));
		break;
	case LogicalTypeId::BIGINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int64_t));
		break;
	case LogicalTypeId::UTINYINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint8_t));
		break;
	case LogicalTypeId::USMALLINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint16_t));
		break;
	case LogicalTypeId::UINTEGER:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint32_t));
		break;
	case LogicalTypeId::UBIGINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint64_t));
		break;
	case LogicalTypeId::HUGEINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
		break;
	case LogicalTypeId::UHUGEINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uhugeint_t));
		break;
	case LogicalTypeId::FLOAT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(float));
		break;
	case LogicalTypeId::DECIMAL: {
		auto physical_type = vec.GetType().InternalType();

		switch (physical_type) {
		case PhysicalType::INT16:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int16_t));
			break;
		case PhysicalType::INT32:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int32_t));
			break;
		case PhysicalType::INT64:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int64_t));
			break;
		case PhysicalType::INT128:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
			break;
		default:
			throw InternalException("Unimplemented physical type for decimal");
		}
		break;
	}
	case LogicalTypeId::DOUBLE:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(double));
		break;
	case LogicalTypeId::DATE:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(date_t));
		break;
	case LogicalTypeId::TIME:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(dtime_t));
		break;
	case LogicalTypeId::TIME_TZ:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(dtime_tz_t));
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(timestamp_t));
		break;
	case LogicalTypeId::ENUM:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto d_str = vec.GetValue(row_idx).ToString();
			jstring j_str = env->NewStringUTF(d_str.c_str());
			env->SetObjectArrayElement(varlen_data, row_idx, j_str);
		}
		break;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT: {
		varlen_data = env->NewObjectArray(row_count, J_DuckStruct, nullptr);

		auto &entries = StructVector::GetEntries(vec);
		auto columns = env->NewObjectArray(entries.size(), J_DuckVector, nullptr);
		auto names = env->NewObjectArray(entries.size(), J_String, nullptr);

		for (idx_t entry_i = 0; entry_i < entries.size(); entry_i++) {
			auto j_vec = ProcessVector(env, conn_ref, *entries[entry_i], row_count);
			env->SetObjectArrayElement(columns, entry_i, j_vec);
			env->SetObjectArrayElement(names, entry_i,
			                           env->NewStringUTF(StructType::GetChildName(vec.GetType(), entry_i).c_str()));
		}
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			env->SetObjectArrayElement(varlen_data, row_idx,
			                           env->NewObject(J_DuckStruct, J_DuckStruct_init, names, columns, row_idx,
			                                          env->NewStringUTF(vec.GetType().ToString().c_str())));
		}

		break;
	}
	case LogicalTypeId::BLOB:
		varlen_data = env->NewObjectArray(row_count, J_ByteArray, nullptr);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto &d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];

			auto j_arr = env->NewByteArray(d_str.GetSize());
			auto j_arr_el = env->GetByteArrayElements(j_arr, nullptr);
			memcpy((void *)j_arr_el, (void *)d_str.GetData(), d_str.GetSize());
			env->ReleaseByteArrayElements(j_arr, j_arr_el, 0);

			env->SetObjectArrayElement(varlen_data, row_idx, j_arr);
		}
		break;
	case LogicalTypeId::UUID:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
		break;
	case LogicalTypeId::ARRAY: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);
		auto &array_vector = ArrayVector::GetEntry(vec);
		auto total_size = row_count * ArrayType::GetSize(vec.GetType());
		auto j_vec = ProcessVector(env, conn_ref, array_vector, total_size);

		auto limit = ArrayType::GetSize(vec.GetType());

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}

			auto offset = row_idx * limit;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
		}
		break;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);

		auto list_entries = FlatVector::GetData<list_entry_t>(vec);

		auto list_size = ListVector::GetListSize(vec);
		auto &list_vector = ListVector::GetEntry(vec);
		auto j_vec = ProcessVector(env, conn_ref, list_vector, list_size);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}

			auto offset = list_entries[row_idx].offset;
			auto limit = list_entries[row_idx].length;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
		}
		break;
	}
	default: {
		Vector string_vec(LogicalType::VARCHAR);
		VectorOperations::Cast(*conn_ref->context, vec, string_vec, row_count);
		vec.ReferenceAndSetType(string_vec);
		// fall through on purpose
	}
	case LogicalTypeId::VARCHAR:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];
			auto j_str = decode_charbuffer_to_jstring(env, d_str.GetData(), d_str.GetSize());
			env->SetObjectArrayElement(varlen_data, row_idx, j_str);
		}
		break;
	}

	env->SetObjectField(jvec, J_DuckVector_constlen, constlen_data);
	env->SetObjectField(jvec, J_DuckVector_varlen, varlen_data);

	return jvec;
}

jint _duckdb_jdbc_fetch_size(JNIEnv *, jclass) {
	return STANDARD_VECTOR_SIZE;
}

jobject _duckdb_jdbc_create_appender(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray schema_name_j,
                                     jbyteArray table_name_j) {

	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}
	auto schema_name = byte_array_to_string(env, schema_name_j);
	auto table_name = byte_array_to_string(env, table_name_j);
	auto appender = new Appender(*conn_ref, schema_name, table_name);
	return env->NewDirectByteBuffer(appender, 0);
}

static Appender *get_appender(JNIEnv *env, jobject appender_ref_buf) {
	auto appender_ref = (Appender *)env->GetDirectBufferAddress(appender_ref_buf);
	if (!appender_ref) {
		throw InvalidInputException("Invalid appender");
	}
	return appender_ref;
}

void _duckdb_jdbc_appender_begin_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->BeginRow();
}

void _duckdb_jdbc_appender_end_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->EndRow();
}

void _duckdb_jdbc_appender_flush(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->Flush();
}

void _duckdb_jdbc_appender_close(JNIEnv *env, jclass, jobject appender_ref_buf) {
	auto appender = get_appender(env, appender_ref_buf);
	appender->Close();
	delete appender;
}

void _duckdb_jdbc_appender_append_boolean(JNIEnv *env, jclass, jobject appender_ref_buf, jboolean value) {
	get_appender(env, appender_ref_buf)->Append((bool)value);
}

void _duckdb_jdbc_appender_append_byte(JNIEnv *env, jclass, jobject appender_ref_buf, jbyte value) {
	get_appender(env, appender_ref_buf)->Append((int8_t)value);
}

void _duckdb_jdbc_appender_append_short(JNIEnv *env, jclass, jobject appender_ref_buf, jshort value) {
	get_appender(env, appender_ref_buf)->Append((int16_t)value);
}

void _duckdb_jdbc_appender_append_int(JNIEnv *env, jclass, jobject appender_ref_buf, jint value) {
	get_appender(env, appender_ref_buf)->Append((int32_t)value);
}

void _duckdb_jdbc_appender_append_long(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	get_appender(env, appender_ref_buf)->Append((int64_t)value);
}

void _duckdb_jdbc_appender_append_float(JNIEnv *env, jclass, jobject appender_ref_buf, jfloat value) {
	get_appender(env, appender_ref_buf)->Append((float)value);
}

void _duckdb_jdbc_appender_append_double(JNIEnv *env, jclass, jobject appender_ref_buf, jdouble value) {
	get_appender(env, appender_ref_buf)->Append((double)value);
}

void _duckdb_jdbc_appender_append_timestamp(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	timestamp_t timestamp = timestamp_t((int64_t)value);
	get_appender(env, appender_ref_buf)->Append(Value::TIMESTAMP(timestamp));
}

void _duckdb_jdbc_appender_append_decimal(JNIEnv *env, jclass, jobject appender_ref_buf, jobject value) {
	Value val = create_value_from_bigdecimal(env, value);
	get_appender(env, appender_ref_buf)->Append(val);
}

void _duckdb_jdbc_appender_append_string(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	if (env->IsSameObject(value, NULL)) {
		get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
		return;
	}

	auto string_value = byte_array_to_string(env, value);
	get_appender(env, appender_ref_buf)->Append(string_value.c_str());
}

void _duckdb_jdbc_appender_append_bytes(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	if (env->IsSameObject(value, NULL)) {
		get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
		return;
	}

	auto string_value = byte_array_to_string(env, value);
	get_appender(env, appender_ref_buf)->Append(Value::BLOB_RAW(string_value));
}

void _duckdb_jdbc_appender_append_null(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
}

jlong _duckdb_jdbc_arrow_stream(JNIEnv *env, jclass, jobject res_ref_buf, jlong batch_size) {
	if (!res_ref_buf) {
		throw InvalidInputException("Invalid result set");
	}
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}

	auto wrapper = new ResultArrowArrayStreamWrapper(std::move(res_ref->res), batch_size);
	return (jlong)&wrapper->stream;
}

class JavaArrowTabularStreamFactory {
public:
	JavaArrowTabularStreamFactory(ArrowArrayStream *stream_ptr_p) : stream_ptr(stream_ptr_p) {};

	static duckdb::unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_p, ArrowStreamParameters &parameters) {

		auto factory = (JavaArrowTabularStreamFactory *)factory_p;
		if (!factory->stream_ptr->release) {
			throw InvalidInputException("This stream has been released");
		}
		auto res = make_uniq<ArrowArrayStreamWrapper>();
		res->arrow_array_stream = *factory->stream_ptr;
		factory->stream_ptr->release = nullptr;
		return res;
	}

	static void GetSchema(uintptr_t factory_p, ArrowSchemaWrapper &schema) {
		auto factory = (JavaArrowTabularStreamFactory *)factory_p;
		auto stream_ptr = factory->stream_ptr;
		if (!stream_ptr->release) {
			throw InvalidInputException("This stream has been released");
		}
		stream_ptr->get_schema(stream_ptr, &schema.arrow_schema);
		auto error = stream_ptr->get_last_error(stream_ptr);
		if (error != nullptr) {
			throw InvalidInputException(error);
		}
	}

	ArrowArrayStream *stream_ptr;
};

void _duckdb_jdbc_arrow_register(JNIEnv *env, jclass, jobject conn_ref_buf, jlong arrow_array_stream_pointer,
                                 jbyteArray name_j) {

	auto conn = get_connection(env, conn_ref_buf);
	if (conn == nullptr) {
		return;
	}
	auto name = byte_array_to_string(env, name_j);

	auto arrow_array_stream = (ArrowArrayStream *)(uintptr_t)arrow_array_stream_pointer;

	auto factory = new JavaArrowTabularStreamFactory(arrow_array_stream);
	duckdb::vector<Value> parameters;
	parameters.push_back(Value::POINTER((uintptr_t)factory));
	parameters.push_back(Value::POINTER((uintptr_t)JavaArrowTabularStreamFactory::Produce));
	parameters.push_back(Value::POINTER((uintptr_t)JavaArrowTabularStreamFactory::GetSchema));
	conn->TableFunction("arrow_scan_dumb", parameters)->CreateView(name, true, true);
}

void _duckdb_jdbc_create_extension_type(JNIEnv *env, jclass, jobject conn_buf) {

	auto connection = get_connection(env, conn_buf);
	if (!connection) {
		return;
	}

	auto &db_instance = DatabaseInstance::GetDatabase(*connection->context);
	child_list_t<LogicalType> children = {{"hello", LogicalType::VARCHAR}, {"world", LogicalType::VARCHAR}};
	auto hello_world_type = LogicalType::STRUCT(children);
	hello_world_type.SetAlias("test_type");
	ExtensionUtil::RegisterType(db_instance, "test_type", hello_world_type);

	LogicalType byte_test_type_type = LogicalTypeId::BLOB;
	byte_test_type_type.SetAlias("byte_test_type");
	ExtensionUtil::RegisterType(db_instance, "byte_test_type", byte_test_type_type);
}

#include "arrow.hpp"
#include "config.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/capi_v2/capi_v2_result_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "util.hpp"

#include <cstdint>
#include <limits>
#include <memory>

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

//! The database instance cache, used so that multiple connections to the same file point to the same database object
duckdb::DBInstanceCache instance_cache;

jobject _duckdb_jdbc_startup(JNIEnv *env, jclass, jbyteArray database_j, jboolean read_only, jobject props,
                             jboolean cache_instance) {
	auto database = jbyteArray_to_string(env, database_j);
	std::unique_ptr<DBConfig> config = create_db_config(env, read_only, props);
	bool should_cache = cache_instance && database != ":memory:" && !database.empty();
	auto shared_db = instance_cache.GetOrCreateInstance(database, *config, should_cache);
	auto conn_ref = new ConnectionHolder(shared_db);

	return env->NewDirectByteBuffer(conn_ref, 0);
}

jobject _duckdb_jdbc_connect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto config = ClientConfig::GetConfig(*conn_ref->connection->context);
	auto conn = new ConnectionHolder(conn_ref->db);
	ClientConfig::GetConfig(*conn->connection->context) = config;
	return env->NewDirectByteBuffer(conn, 0);
}

jobject _duckdb_jdbc_create_db_ref(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto db_ref = conn_ref->create_db_ref();
	return env->NewDirectByteBuffer(db_ref, 0);
}

jlong _duckdb_jdbc_db_address(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	// the instance cache returns a new DuckDB handle for every cache hit, the DatabaseInstance is the shared object
	return (jlong)conn_ref->db->instance.get();
}

void _duckdb_jdbc_destroy_db_ref(JNIEnv *env, jclass, jobject db_ref_buf) {
	if (nullptr == db_ref_buf) {
		return;
	}
	auto db_ref = (DBHolder *)env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

jstring _duckdb_jdbc_get_schema(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();

	return env->NewStringUTF(entry.GetSchema().c_str());
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
	set_catalog_search_path(
	    env, conn_ref_buf, CatalogSearchEntry(Identifier(INVALID_CATALOG), Identifier(jstring_to_string(env, schema))));
}

void _duckdb_jdbc_set_catalog(JNIEnv *env, jclass, jobject conn_ref_buf, jstring catalog) {
	set_catalog_search_path(
	    env, conn_ref_buf, CatalogSearchEntry(Identifier(jstring_to_string(env, catalog)), Identifier(DEFAULT_SCHEMA)));
}

jstring _duckdb_jdbc_get_catalog(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();
	if (entry.GetCatalog() == INVALID_CATALOG) {
		entry.SetCatalog(DatabaseManager::GetDefaultDatabase(*conn_ref->context));
	}

	return env->NewStringUTF(entry.GetCatalog().c_str());
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

void _duckdb_jdbc_disconnect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	if (nullptr == conn_ref_buf) {
		return;
	}
	auto conn_ref = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

#include "utf8proc_wrapper.hpp"

jint _duckdb_jdbc_fetch_size(JNIEnv *, jclass) {
	return STANDARD_VECTOR_SIZE;
}

class JavaArrowScanFactory : public ArrowScanFactory {
public:
	explicit JavaArrowScanFactory(duckdb::shared_ptr<JavaRegisteredArrowStream> state_p) : state(std::move(state_p)) {
	}

	void GetSchema(ArrowSchema &schema) override {
		state->GetSchema(schema);
	}

	duckdb::unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &) override {
		return state->Take();
	}

private:
	duckdb::shared_ptr<JavaRegisteredArrowStream> state;
};

void _duckdb_jdbc_arrow_register(JNIEnv *env, jclass, jobject conn_ref_buf, jlong arrow_array_stream_pointer,
                                 jbyteArray name_j) {
	auto conn = get_connection(env, conn_ref_buf);
	if (conn == nullptr) {
		return;
	}
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto name = jbyteArray_to_string(env, name_j);
	auto arrow_array_stream = reinterpret_cast<ArrowArrayStream *>(arrow_array_stream_pointer);
	if (!arrow_array_stream || !arrow_array_stream->release) {
		throw InvalidInputException("This stream has been released");
	}

	auto state = make_shared_ptr<JavaRegisteredArrowStream>(arrow_array_stream);
	conn_holder->track_arrow_stream(state);
	auto factory = make_shared_ptr<JavaArrowScanFactory>(std::move(state));
	// construct the `arrow_scan_dumb(...)` function expression
	auto scan = make_uniq<TableFunctionRef>();
	scan->function =
	    make_uniq<FunctionExpression>("arrow_scan_dumb", duckdb::vector<duckdb::unique_ptr<ParsedExpression>>());
	scan->bind_info = std::move(factory);
	// construct the `SELECT * FROM arrow_scan_dumb(...)` statement
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(scan);
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);
	// construct the `CREATE TEMP VIEW AS` statement using the select statement
	auto info = make_uniq<CreateViewInfo>();
	info->query = std::move(select);
	info->temporary = true;
	info->SetQualifiedName(QualifiedName(Identifier(), Identifier::InvalidSchema(), Identifier(name)));
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	// execute the CREATE VIEW
	conn->Query(std::move(statement))->ThrowIfError("Failed to create view '" + name + "': ");
}

void _duckdb_jdbc_arrow_close_streams(JNIEnv *env, jclass, jobject conn_ref_buf) {
	get_connection_ref(env, conn_ref_buf)->close_arrow_streams();
}

static ProfilerPrintFormat GetProfilerPrintFormat(JNIEnv *env, jobject format) {
	jobject jname = env->CallObjectMethod(format, J_ProfilerPrintFormat_getName);
	check_java_exception_and_rethrow(env);
	const std::string name = jstring_to_string(env, static_cast<jstring>(jname));
	return ProfilerPrintFormat(name);
}

jstring _duckdb_jdbc_get_profiling_information(JNIEnv *env, jclass, jobject conn_ref_buf, jobject j_format) {
	auto connection = get_connection(env, conn_ref_buf);
	if (!connection) {
		throw InvalidInputException("Invalid connection");
	}
	auto format = GetProfilerPrintFormat(env, j_format);
	auto profiling_info = connection->GetProfilingInformation(format);
	return env->NewStringUTF(profiling_info.c_str());
}

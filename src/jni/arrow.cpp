#include "arrow.hpp"

#include "util.hpp"

#include <cerrno>
#include <cstring>
#include <mutex>
#include <optional>
#include <unordered_map>

/**
 * State of an Arrow stream that exports the chunks of a result. The result is moved out of the Java-owned holder.
 */
class JavaResultArrowStream : public ArrowStreamState {
public:
	JavaResultArrowStream(std::shared_ptr<cxx::Connection> connection_p, ResultHolder &source, idx_t batch_size)
	    : connection(std::move(connection_p)), batch_size(batch_size) {
		names = std::move(source.names);
		types = std::move(source.types);
		stream = std::move(source.stream);
		source.stream.reset();
		materialized = std::move(source.materialized);
		first_chunk = std::move(source.first_chunk);
		source.first_chunk.reset();
		std::vector<cxx::LogicalType> exporter_types;
		for (auto &type : types) {
			exporter_types.push_back(type.Copy());
		}
		exporter.emplace(connection->GetContext(), exporter_types, names, batch_size);
	}

	void Close() override {
		std::lock_guard<std::mutex> guard(lock);
		closed = true;
		exporter.reset();
		stream.reset();
		materialized.reset();
		first_chunk.reset();
	}

	//! Creates a stream that shares the state, the stream is deleted when it is released
	static ArrowArrayStream *Export(std::shared_ptr<JavaResultArrowStream> state) {
		auto exported = new Exported();
		exported->state = std::move(state);
		auto &stream = exported->stream;
		stream.private_data = exported;
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.get_last_error = GetLastError;
		stream.release = Release;
		return &stream;
	}

private:
	struct Exported {
		ArrowArrayStream stream;
		std::shared_ptr<JavaResultArrowStream> state;
	};

	static JavaResultArrowStream &GetState(ArrowArrayStream *stream) {
		return *reinterpret_cast<Exported *>(stream->private_data)->state;
	}

	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
		if (!stream->release) {
			return -1;
		}
		auto &self = GetState(stream);
		std::lock_guard<std::mutex> guard(self.lock);
		out->release = nullptr;
		try {
			if (self.closed) {
				throw JdbcException("Connection Error: Connection was closed");
			}
			self.exporter->GetSchema(*out);
		} catch (const std::exception &e) {
			self.last_error = e.what();
			return EIO;
		}
		return 0;
	}

	static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
		if (!stream->release) {
			return -1;
		}
		auto &self = GetState(stream);
		std::lock_guard<std::mutex> guard(self.lock);
		out->release = nullptr;
		try {
			if (self.closed) {
				throw JdbcException("Connection Error: Connection was closed");
			}
			while (true) {
				if (self.exporter->NextArray(*out)) {
					return 0;
				}
				if (self.finished) {
					// the end of the stream
					out->release = nullptr;
					return 0;
				}
				auto chunk = self.NextChunk();
				if (!chunk) {
					self.finished = true;
					self.exporter->Flush();
					continue;
				}
				self.exporter->Append(*chunk);
			}
		} catch (const std::exception &e) {
			self.last_error = e.what();
			return EIO;
		}
	}

	static const char *GetLastError(ArrowArrayStream *stream) {
		if (!stream->release) {
			return nullptr;
		}
		auto &self = GetState(stream);
		std::lock_guard<std::mutex> guard(self.lock);
		return self.last_error.empty() ? nullptr : self.last_error.c_str();
	}

	static void Release(ArrowArrayStream *stream) {
		if (!stream->release) {
			return;
		}
		auto exported = reinterpret_cast<Exported *>(stream->private_data);
		stream->release = nullptr;
		delete exported;
	}

	//! Loads the next chunk of the result, the chunk is empty at the end of the result
	std::optional<cxx::DataChunk> NextChunk() {
		if (materialized) {
			auto chunk = connection->GetFactory().CreateDataChunk(types);
			if (materialized->collection->Scan(*materialized->shared_state, *materialized->worker_state, chunk) &&
			    chunk.GetRowCount() > 0) {
				return std::optional<cxx::DataChunk>(std::move(chunk));
			}
			return std::nullopt;
		}
		if (first_chunk) {
			auto chunk = std::move(*first_chunk);
			first_chunk.reset();
			return std::optional<cxx::DataChunk>(std::move(chunk));
		}
		auto chunk = stream->FetchChunk();
		if (!chunk) {
			return std::nullopt;
		}
		return std::optional<cxx::DataChunk>(std::move(chunk));
	}

	std::mutex lock;
	std::shared_ptr<cxx::Connection> connection;
	std::vector<std::string> names;
	std::vector<cxx::LogicalType> types;
	std::optional<cxx::QueryResult> stream;
	std::unique_ptr<MaterializedData> materialized;
	std::optional<cxx::DataChunk> first_chunk;
	std::optional<cxx::ArrowExporter> exporter;
	idx_t batch_size;
	bool finished = false;
	bool closed = false;
	std::string last_error;
};

ArrowArrayStream *create_result_arrow_stream(ConnectionHolder &conn_holder, ResultHolder &result, idx_t batch_size) {
	auto state = std::make_shared<JavaResultArrowStream>(conn_holder.connection, result, batch_size);
	conn_holder.track_arrow_stream(state);
	return JavaResultArrowStream::Export(std::move(state));
}

namespace {

//! The name of the table function that scans a registered Arrow stream
const char *const ARROW_SCAN_FUNCTION = "__jdbc_arrow_stream_scan";

/**
 * State of an Arrow stream registered with a connection. The stream is moved out of the Java-owned struct on
 * registration, it is released when it is consumed by a scan or when the connection is closed.
 */
class JavaRegisteredArrowStream : public ArrowStreamState {
public:
	explicit JavaRegisteredArrowStream(ArrowArrayStream *source) {
		stream = *source;
		source->release = nullptr;
	}

	~JavaRegisteredArrowStream() override {
		Close();
	}

	void Close() override;

	void GetSchema(ArrowSchema &schema) {
		std::lock_guard<std::mutex> guard(lock);
		if (!stream.release) {
			throw JdbcException("Invalid Input Error: This stream has been released");
		}
		schema.release = nullptr;
		if (stream.get_schema(&stream, &schema) != 0) {
			auto error = stream.get_last_error(&stream);
			throw JdbcException(std::string("Invalid Input Error: ") + (error ? error : "unable to read the schema"));
		}
	}

	//! A stream can only be consumed by a single scan
	void Take(ArrowArrayStream &out);

	int64_t id = 0;

private:
	std::mutex lock;
	ArrowArrayStream stream;
};

//! The registered streams by the ids that the views pass to the scan function
struct ArrowStreamRegistry {
	std::mutex lock;
	int64_t next_id = 1;
	//! The streams are owned until they are consumed by a scan or closed with their connection
	std::unordered_map<int64_t, std::shared_ptr<JavaRegisteredArrowStream>> streams;

	void Add(const std::shared_ptr<JavaRegisteredArrowStream> &state) {
		std::lock_guard<std::mutex> guard(lock);
		state->id = next_id++;
		streams[state->id] = state;
	}

	std::shared_ptr<JavaRegisteredArrowStream> Get(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		auto it = streams.find(id);
		return it == streams.end() ? nullptr : it->second;
	}

	void Remove(int64_t id) {
		// the state can be destroyed here, it is destroyed after the lock is released
		std::shared_ptr<JavaRegisteredArrowStream> removed;
		{
			std::lock_guard<std::mutex> guard(lock);
			auto it = streams.find(id);
			if (it != streams.end()) {
				removed = std::move(it->second);
				streams.erase(it);
			}
		}
	}
};

ArrowStreamRegistry &arrow_stream_registry() {
	static ArrowStreamRegistry registry;
	return registry;
}

void JavaRegisteredArrowStream::Close() {
	{
		std::lock_guard<std::mutex> guard(lock);
		if (stream.release) {
			stream.release(&stream);
			stream.release = nullptr;
		}
	}
	arrow_stream_registry().Remove(id);
}

void JavaRegisteredArrowStream::Take(ArrowArrayStream &out) {
	{
		std::lock_guard<std::mutex> guard(lock);
		if (!stream.release) {
			throw JdbcException("Invalid Input Error: This stream has been released");
		}
		out = stream;
		stream.release = nullptr;
	}
	arrow_stream_registry().Remove(id);
}

//! Releases an Arrow stream that was taken from a registered stream
struct TakenArrowStream {
	ArrowArrayStream stream {};

	TakenArrowStream() = default;
	TakenArrowStream(const TakenArrowStream &) = delete;
	TakenArrowStream &operator=(const TakenArrowStream &) = delete;

	~TakenArrowStream() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct ArrowScanBindData {
	explicit ArrowScanBindData(std::shared_ptr<JavaRegisteredArrowStream> state_p) : state(std::move(state_p)) {
	}

	std::shared_ptr<JavaRegisteredArrowStream> state;
};

struct ArrowScanGlobalState {
	ArrowScanGlobalState(const std::shared_ptr<JavaRegisteredArrowStream> &state, cxx::Context &context) {
		// only the stream of the first scan can be read
		state->Take(taken.stream);
		ArrowSchema schema {};
		if (taken.stream.get_schema(&taken.stream, &schema) != 0) {
			auto error = taken.stream.get_last_error(&taken.stream);
			throw JdbcException(std::string("Invalid Input Error: ") + (error ? error : "unable to read the schema"));
		}
		try {
			// the arrays are split into chunks that fit into the output chunk
			std::vector<cxx::LogicalType> types;
			types.push_back(context.GetFactory().CreateType(cxx::LogicalTypeId::BOOLEAN));
			auto capacity = context.GetFactory().CreateDataChunk(types).GetCapacity();
			importer.emplace(context, schema, capacity);
		} catch (...) {
			if (schema.release) {
				schema.release(&schema);
			}
			throw;
		}
		if (schema.release) {
			schema.release(&schema);
		}
	}

	//! The next chunk of the stream, the chunk is empty at the end of the stream
	cxx::DataChunk NextChunk() {
		while (true) {
			auto chunk = importer->NextChunk();
			if (chunk) {
				return chunk;
			}
			if (finished) {
				return chunk;
			}
			ArrowArray array {};
			array.release = nullptr;
			if (taken.stream.get_next(&taken.stream, &array) != 0) {
				auto error = taken.stream.get_last_error(&taken.stream);
				throw JdbcException(std::string("Invalid Input Error: ") +
				                    (error ? error : "unable to read the stream"));
			}
			if (!array.release) {
				// the end of the stream
				finished = true;
				importer->Flush();
				continue;
			}
			importer->Append(array, true);
		}
	}

	TakenArrowStream taken;
	std::optional<cxx::ArrowImporter> importer;
	bool finished = false;
};

void arrow_scan_bind(cxx::TableFunction::BindInput &input) {
	auto id = input.GetConstantArgument(0).Get<int64_t>();
	auto state = arrow_stream_registry().Get(id);
	if (!state) {
		throw JdbcException("Invalid Input Error: This stream has been released");
	}
	ArrowSchema schema {};
	state->GetSchema(schema);
	try {
		cxx::ArrowImporter importer(input.GetContext(), schema);
		auto resolved = importer.GetSchema();
		if (resolved.GetFieldCount() == 0) {
			throw JdbcException("Invalid Input Error: Provided table/dataframe must have at least one column");
		}
		for (idx_t i = 0; i < resolved.GetFieldCount(); i++) {
			input.AddResultColumn(std::string(resolved.GetFieldName(i)), resolved.GetFieldType(i));
		}
	} catch (...) {
		if (schema.release) {
			schema.release(&schema);
		}
		throw;
	}
	if (schema.release) {
		schema.release(&schema);
	}
	input.SetBindData<ArrowScanBindData>(std::move(state));
}

void arrow_scan_init_global(cxx::TableFunction::InitGlobalInput &input) {
	auto &bind_data = input.GetBindData<ArrowScanBindData>();
	input.SetGlobalState<ArrowScanGlobalState>(bind_data.state, input.GetContext());
}

void arrow_scan_exec(cxx::TableFunction::ExecInput &input) {
	auto &global = input.GetGlobalState<ArrowScanGlobalState>();
	auto chunk = global.NextChunk();
	if (!chunk) {
		// an empty output chunk is the end of the scan
		return;
	}
	auto output = input.GetOutputChunk();
	for (idx_t i = 0; i < chunk.GetVectorCount(); i++) {
		auto target = output.GetVector(i);
		target.Reference(chunk.GetVector(i));
	}
}

} // namespace

//! The message of the error without the name of its type, as in "Invalid Input Error: message"
static std::string error_message_without_type(const std::string &message) {
	auto pos = message.find(" Error: ");
	if (pos == std::string::npos || pos > 40) {
		return message;
	}
	return message.substr(pos + 8);
}

void register_arrow_stream(ConnectionHolder &conn_holder, ArrowArrayStream *stream, const std::string &name) {
	auto state = std::make_shared<JavaRegisteredArrowStream>(stream);
	conn_holder.track_arrow_stream(state);
	arrow_stream_registry().Add(state);
	auto id = state->id;

	std::string quoted_name = "\"";
	for (char c : name) {
		if (c == '"') {
			quoted_name += "\"\"";
		} else {
			quoted_name += c;
		}
	}
	quoted_name += "\"";
	try {
		std::lock_guard<std::mutex> guard(*conn_holder.lock);
		conn_holder.connection
		    ->Execute("CREATE OR REPLACE TEMPORARY VIEW " + quoted_name + " AS SELECT * FROM " + ARROW_SCAN_FUNCTION +
		              "(" + std::to_string(id) + ")")
		    .Drain();
	} catch (const std::exception &e) {
		state->Close();
		throw JdbcException(std::string("Failed to create view '") + name +
		                    "': " + error_message_without_type(e.what()));
	}
	// the stream of the view that is replaced is released
	auto replaced = conn_holder.replace_registered_stream(name, state);
	if (replaced) {
		replaced->Close();
	}
}

void register_arrow_scan_function(cxx::Instance &instance) {
	auto conn = instance.Connect();
	auto &factory = conn.GetFactory();
	auto function = cxx::TableFunction::Create(factory);
	function.SetName(ARROW_SCAN_FUNCTION);
	function.WithSignature([&factory](cxx::FunctionSignature &signature) {
		signature.AddParameter("stream_id", factory.CreateType(cxx::LogicalTypeId::BIGINT));
	});
	function.SetBindCallback(arrow_scan_bind);
	function.SetInitGlobalCallback(arrow_scan_init_global);
	function.SetExecCallback(arrow_scan_exec);
	conn.Register(function);
}

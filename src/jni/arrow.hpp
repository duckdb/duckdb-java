#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "holders.hpp"

#include <mutex>

namespace duckdb {
class ArrowTypeExtensionData;
}

//! Exports the chunks of a JDBC result as an Arrow stream of record batches of at most batch_size rows.
//! The result is moved out of the ResultHolder into this state, that is shared between the exported stream
//! and the connection. Connection closes the state (releasing the result) if it is closed before the stream.
class JavaResultArrowStream : public ArrowStreamState {
public:
	JavaResultArrowStream(duckdb::unique_ptr<duckdb::QueryResult> res_p,
	                      duckdb::unique_ptr<duckdb::QueryResultStream<>> stream_p, duckdb::idx_t batch_size_p);

	void Close() override;

	//! Creates a stream that shares the state, the stream is deleted when it is released
	static ArrowArrayStream *Export(duckdb::shared_ptr<JavaResultArrowStream> state);

private:
	struct Exported {
		ArrowArrayStream stream;
		duckdb::shared_ptr<JavaResultArrowStream> state;
	};

	static JavaResultArrowStream &GetState(ArrowArrayStream *stream);
	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out);
	static int GetNext(ArrowArrayStream *stream, ArrowArray *out);
	static const char *GetLastError(ArrowArrayStream *stream);
	static void Release(ArrowArrayStream *stream);

	//! Loads the next chunk, chunk is null at the end of the result
	bool FetchChunk();

	std::mutex lock;
	duckdb::unique_ptr<duckdb::QueryResult> res;
	duckdb::unique_ptr<duckdb::QueryResultStream<>> res_stream;
	duckdb::idx_t batch_size;
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	duckdb::ClientProperties properties;
	duckdb::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_types;
	duckdb::unique_ptr<duckdb::DataChunk> chunk;
	duckdb::idx_t chunk_offset = 0;
	bool finished = false;
	bool closed = false;
	duckdb::ErrorData last_error;
};

//! Arrow stream registered with a connection. The stream is moved out of the Java-owned struct on registration,
//! it is released when it is consumed by a scan, when the view is dropped or when the connection is closed.
class JavaRegisteredArrowStream : public ArrowStreamState {
public:
	explicit JavaRegisteredArrowStream(ArrowArrayStream *source);

	~JavaRegisteredArrowStream() override;

	void Close() override;

	void GetSchema(ArrowSchema &schema);

	//! A stream can only be consumed by a single scan
	duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> Take();

private:
	std::mutex lock;
	ArrowArrayStream stream;
};

#pragma once

#include "holders.hpp"

#include <string>

/**
 * Exports the chunks of a JDBC result as an Arrow stream of record batches of at most batch_size rows. The data of
 * the result is moved out of the ResultHolder into the state of the stream, that is shared between the exported stream
 * and the connection. Connection closes the state (releasing the result) if it is closed before the stream.
 * The returned stream is owned by its consumer, it is deleted when released.
 */
ArrowArrayStream *create_result_arrow_stream(ConnectionHolder &conn_holder, ResultHolder &result, idx_t batch_size);

/**
 * Registers the Arrow stream as a view with the specified name. The stream is moved out of the Java-owned struct on
 * registration, it is released when it is consumed by a scan, when the view is dropped or when the connection is
 * closed.
 */
void register_arrow_stream(ConnectionHolder &conn_holder, ArrowArrayStream *stream, const std::string &name);

/**
 * Registers the table function that is used by the views of the registered Arrow streams, called when the instance
 * is created.
 */
void register_arrow_scan_function(cxx::Instance &instance);

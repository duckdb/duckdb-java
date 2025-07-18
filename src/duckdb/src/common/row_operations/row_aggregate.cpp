#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"

namespace duckdb {

void RowOperations::InitializeStates(TupleDataLayout &layout, Vector &addresses, const SelectionVector &sel,
                                     idx_t count) {
	if (count == 0) {
		return;
	}
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto &offsets = layout.GetOffsets();
	auto aggr_idx = layout.ColumnCount();

	for (const auto &aggr : layout.GetAggregates()) {
		if (sel.IsSet()) {
			for (idx_t i = 0; i < count; ++i) {
				aggr.function.initialize(aggr.function, pointers[sel.get_index_unsafe(i)] + offsets[aggr_idx]);
			}
		} else {
			for (idx_t i = 0; i < count; ++i) {
				aggr.function.initialize(aggr.function, pointers[i] + offsets[aggr_idx]);
			}
		}
		++aggr_idx;
	}
}

void RowOperations::DestroyStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, idx_t count) {
	if (count == 0) {
		return;
	}
	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses, UnsafeNumericCast<int64_t>(layout.GetAggrOffset()), count);
	for (const auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
			aggr.function.destructor(addresses, aggr_input_data, count);
		}
		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses, UnsafeNumericCast<int64_t>(aggr.payload_size), count);
	}
}

void RowOperations::UpdateStates(RowOperationsState &state, AggregateObject &aggr, Vector &addresses,
                                 DataChunk &payload, idx_t arg_idx, idx_t count) {
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
	aggr.function.update(aggr.child_count == 0 ? nullptr : &payload.data[arg_idx], aggr_input_data, aggr.child_count,
	                     addresses, count);
}

void RowOperations::UpdateFilteredStates(RowOperationsState &state, AggregateFilterData &filter_data,
                                         AggregateObject &aggr, Vector &addresses, DataChunk &payload, idx_t arg_idx) {
	idx_t count = filter_data.ApplyFilter(payload);
	if (count == 0) {
		return;
	}

	Vector filtered_addresses(addresses, filter_data.true_sel, count);
	filtered_addresses.Flatten(count);

	UpdateStates(state, aggr, filtered_addresses, filter_data.filtered_payload, arg_idx, count);
}

void RowOperations::CombineStates(RowOperationsState &state, TupleDataLayout &layout, Vector &sources, Vector &targets,
                                  idx_t count) {
	if (count == 0) {
		return;
	}

	//	Move to the first aggregate states
	VectorOperations::AddInPlace(sources, UnsafeNumericCast<int64_t>(layout.GetAggrOffset()), count);
	VectorOperations::AddInPlace(targets, UnsafeNumericCast<int64_t>(layout.GetAggrOffset()), count);

	// Keep track of the offset
	idx_t offset = layout.GetAggrOffset();

	for (auto &aggr : layout.GetAggregates()) {
		D_ASSERT(aggr.function.combine);
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
		aggr.function.combine(sources, targets, aggr_input_data, count);

		// Move to the next aggregate states
		VectorOperations::AddInPlace(sources, UnsafeNumericCast<int64_t>(aggr.payload_size), count);
		VectorOperations::AddInPlace(targets, UnsafeNumericCast<int64_t>(aggr.payload_size), count);

		// Increment the offset
		offset += aggr.payload_size;
	}

	// Now subtract the offset to get back to the original position
	VectorOperations::AddInPlace(sources, -UnsafeNumericCast<int64_t>(offset), count);
	VectorOperations::AddInPlace(targets, -UnsafeNumericCast<int64_t>(offset), count);
}

void RowOperations::FinalizeStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses,
                                   DataChunk &result, idx_t aggr_idx) {
	// Copy the addresses
	if (!state.addresses) {
		state.addresses = make_uniq<Vector>(LogicalType::POINTER);
	}
	auto &addresses_copy = *state.addresses;
	VectorOperations::Copy(addresses, addresses_copy, result.size(), 0, 0);

	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses_copy, UnsafeNumericCast<int64_t>(layout.GetAggrOffset()), result.size());

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &target = result.data[aggr_idx + i];
		auto &aggr = aggregates[i];
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
		aggr.function.finalize(addresses_copy, aggr_input_data, target, result.size(), 0);

		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses_copy, UnsafeNumericCast<int64_t>(aggr.payload_size), result.size());
	}
}

} // namespace duckdb

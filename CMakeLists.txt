# Java and JNI

find_package(Java 1.8)

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_JVM_LIBRARY NotNeeded)

find_package(JNI)

cmake_minimum_required(VERSION 3.5...3.29)
set(CMAKE_CXX_STANDARD "11" CACHE STRING "C++ standard to enforce")
set(CMAKE_VERBOSE_MAKEFILE OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR "No compatible Java/JNI found")
endif()

if(NOT(Java_VERSION_MAJOR STREQUAL "1" AND Java_VERSION_MINOR STREQUAL "8"))
  message(WARNING "JDK ${Java_VERSION} is used to compile the Java part of the driver, resulting binaries will NOT be compatible with JDK 8.")
endif()

include(UseJava)


# project definition

project(DuckDBJava C CXX Java)

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)
endif()


# variables set by vendor.py

set(DUCKDB_INCLUDE_DIRS
  src/duckdb/src/include
  src/duckdb/third_party/concurrentqueue
  src/duckdb/third_party/fast_float
  src/duckdb/third_party/fastpforlib
  src/duckdb/third_party/fmt/include
  src/duckdb/third_party/fsst
  src/duckdb/third_party/httplib
  src/duckdb/third_party/hyperloglog
  src/duckdb/third_party/jaro_winkler
  src/duckdb/third_party/jaro_winkler/details
  src/duckdb/third_party/libpg_query
  src/duckdb/third_party/libpg_query/include
  src/duckdb/third_party/lz4
  src/duckdb/third_party/brotli/include
  src/duckdb/third_party/brotli/common
  src/duckdb/third_party/brotli/dec
  src/duckdb/third_party/brotli/enc
  src/duckdb/third_party/mbedtls/include
  src/duckdb/third_party/mbedtls/library
  src/duckdb/third_party/miniz
  src/duckdb/third_party/pcg
  src/duckdb/third_party/pdqsort
  src/duckdb/third_party/re2
  src/duckdb/third_party/ska_sort
  src/duckdb/third_party/skiplist
  src/duckdb/third_party/tdigest
  src/duckdb/third_party/utf8proc
  src/duckdb/third_party/utf8proc/include
  src/duckdb/third_party/vergesort
  src/duckdb/third_party/yyjson/include
  src/duckdb/third_party/zstd/include
  src/duckdb/extension/core_functions/include
  src/duckdb/extension/parquet/include
  src/duckdb/third_party/parquet
  src/duckdb/third_party/thrift
  src/duckdb/third_party/lz4
  src/duckdb/third_party/brotli/include
  src/duckdb/third_party/brotli/common
  src/duckdb/third_party/brotli/dec
  src/duckdb/third_party/brotli/enc
  src/duckdb/third_party/snappy
  src/duckdb/third_party/mbedtls
  src/duckdb/third_party/mbedtls/include
  src/duckdb/third_party/zstd/include
  src/duckdb/extension/icu/include
  src/duckdb/extension/icu/third_party/icu/common
  src/duckdb/extension/icu/third_party/icu/i18n
  src/duckdb/extension/json/include)

set(JEMALLOC_INCLUDE_DIRS
  src/duckdb/extension/jemalloc/include
  src/duckdb/extension/jemalloc/jemalloc/include)

set(DUCKDB_DEFINITIONS
  -DDUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED
  -DDUCKDB_EXTENSION_PARQUET_LINKED
  -DDUCKDB_EXTENSION_ICU_LINKED
  -DDUCKDB_EXTENSION_JSON_LINKED)

set(DUCKDB_SRC_FILES 
  src/duckdb/ub_src_catalog.cpp
  src/duckdb/ub_src_catalog_catalog_entry.cpp
  src/duckdb/ub_src_catalog_catalog_entry_dependency.cpp
  src/duckdb/ub_src_catalog_default.cpp
  src/duckdb/ub_src_common_adbc.cpp
  src/duckdb/ub_src_common_adbc_nanoarrow.cpp
  src/duckdb/ub_src_common.cpp
  src/duckdb/ub_src_common_arrow_appender.cpp
  src/duckdb/ub_src_common_arrow.cpp
  src/duckdb/ub_src_common_crypto.cpp
  src/duckdb/ub_src_common_enums.cpp
  src/duckdb/ub_src_common_exception.cpp
  src/duckdb/ub_src_common_multi_file.cpp
  src/duckdb/ub_src_common_operator.cpp
  src/duckdb/ub_src_common_progress_bar.cpp
  src/duckdb/ub_src_common_row_operations.cpp
  src/duckdb/ub_src_common_serializer.cpp
  src/duckdb/ub_src_common_sort.cpp
  src/duckdb/ub_src_common_sorting.cpp
  src/duckdb/ub_src_common_tree_renderer.cpp
  src/duckdb/ub_src_common_types.cpp
  src/duckdb/ub_src_common_types_column.cpp
  src/duckdb/ub_src_common_types_row.cpp
  src/duckdb/ub_src_common_value_operations.cpp
  src/duckdb/src/common/vector_operations/boolean_operators.cpp
  src/duckdb/src/common/vector_operations/comparison_operators.cpp
  src/duckdb/src/common/vector_operations/generators.cpp
  src/duckdb/src/common/vector_operations/is_distinct_from.cpp
  src/duckdb/src/common/vector_operations/null_operations.cpp
  src/duckdb/src/common/vector_operations/numeric_inplace_operators.cpp
  src/duckdb/src/common/vector_operations/vector_cast.cpp
  src/duckdb/src/common/vector_operations/vector_copy.cpp
  src/duckdb/src/common/vector_operations/vector_hash.cpp
  src/duckdb/src/common/vector_operations/vector_storage.cpp
  src/duckdb/ub_src_execution.cpp
  src/duckdb/ub_src_execution_expression_executor.cpp
  src/duckdb/ub_src_execution_index_art.cpp
  src/duckdb/ub_src_execution_index.cpp
  src/duckdb/ub_src_execution_nested_loop_join.cpp
  src/duckdb/ub_src_execution_operator_aggregate.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_buffer_manager.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_encode.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_scanner.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_sniffer.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_state_machine.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_table_function.cpp
  src/duckdb/ub_src_execution_operator_csv_scanner_util.cpp
  src/duckdb/ub_src_execution_operator_filter.cpp
  src/duckdb/ub_src_execution_operator_helper.cpp
  src/duckdb/ub_src_execution_operator_join.cpp
  src/duckdb/ub_src_execution_operator_order.cpp
  src/duckdb/ub_src_execution_operator_persistent.cpp
  src/duckdb/ub_src_execution_operator_projection.cpp
  src/duckdb/ub_src_execution_operator_scan.cpp
  src/duckdb/ub_src_execution_operator_schema.cpp
  src/duckdb/ub_src_execution_operator_set.cpp
  src/duckdb/ub_src_execution_physical_plan.cpp
  src/duckdb/ub_src_execution_sample.cpp
  src/duckdb/ub_src_function_aggregate_distributive.cpp
  src/duckdb/ub_src_function_aggregate.cpp
  src/duckdb/ub_src_function.cpp
  src/duckdb/ub_src_function_cast.cpp
  src/duckdb/ub_src_function_cast_union.cpp
  src/duckdb/ub_src_function_pragma.cpp
  src/duckdb/ub_src_function_scalar_compressed_materialization.cpp
  src/duckdb/ub_src_function_scalar.cpp
  src/duckdb/ub_src_function_scalar_date.cpp
  src/duckdb/ub_src_function_scalar_generic.cpp
  src/duckdb/ub_src_function_scalar_list.cpp
  src/duckdb/ub_src_function_scalar_map.cpp
  src/duckdb/ub_src_function_scalar_operator.cpp
  src/duckdb/ub_src_function_scalar_sequence.cpp
  src/duckdb/ub_src_function_scalar_string.cpp
  src/duckdb/ub_src_function_scalar_string_regexp.cpp
  src/duckdb/ub_src_function_scalar_struct.cpp
  src/duckdb/ub_src_function_scalar_system.cpp
  src/duckdb/ub_src_function_table_arrow.cpp
  src/duckdb/ub_src_function_table.cpp
  src/duckdb/ub_src_function_table_system.cpp
  src/duckdb/ub_src_function_table_version.cpp
  src/duckdb/ub_src_function_window.cpp
  src/duckdb/ub_src_logging.cpp
  src/duckdb/ub_src_main.cpp
  src/duckdb/ub_src_main_buffered_data.cpp
  src/duckdb/ub_src_main_capi.cpp
  src/duckdb/ub_src_main_capi_cast.cpp
  src/duckdb/ub_src_main_chunk_scan_state.cpp
  src/duckdb/src/main/extension/extension_alias.cpp
  src/duckdb/src/main/extension/extension_helper.cpp
  src/duckdb/src/main/extension/extension_install.cpp
  src/duckdb/src/main/extension/extension_load.cpp
  src/duckdb/src/main/extension/extension_loader.cpp
  src/duckdb/ub_src_main_http.cpp
  src/duckdb/ub_src_main_relation.cpp
  src/duckdb/ub_src_main_secret.cpp
  src/duckdb/ub_src_main_settings.cpp
  src/duckdb/ub_src_optimizer.cpp
  src/duckdb/ub_src_optimizer_compressed_materialization.cpp
  src/duckdb/ub_src_optimizer_join_order.cpp
  src/duckdb/ub_src_optimizer_matcher.cpp
  src/duckdb/ub_src_optimizer_pullup.cpp
  src/duckdb/ub_src_optimizer_pushdown.cpp
  src/duckdb/ub_src_optimizer_rule.cpp
  src/duckdb/ub_src_optimizer_statistics_expression.cpp
  src/duckdb/ub_src_optimizer_statistics_operator.cpp
  src/duckdb/ub_src_parallel.cpp
  src/duckdb/ub_src_parser.cpp
  src/duckdb/ub_src_parser_constraints.cpp
  src/duckdb/ub_src_parser_expression.cpp
  src/duckdb/ub_src_parser_parsed_data.cpp
  src/duckdb/ub_src_parser_query_node.cpp
  src/duckdb/ub_src_parser_statement.cpp
  src/duckdb/ub_src_parser_tableref.cpp
  src/duckdb/ub_src_parser_transform_constraint.cpp
  src/duckdb/ub_src_parser_transform_expression.cpp
  src/duckdb/ub_src_parser_transform_helpers.cpp
  src/duckdb/ub_src_parser_transform_statement.cpp
  src/duckdb/ub_src_parser_transform_tableref.cpp
  src/duckdb/ub_src_planner.cpp
  src/duckdb/ub_src_planner_binder_expression.cpp
  src/duckdb/ub_src_planner_binder_query_node.cpp
  src/duckdb/ub_src_planner_binder_statement.cpp
  src/duckdb/ub_src_planner_binder_tableref.cpp
  src/duckdb/ub_src_planner_expression.cpp
  src/duckdb/ub_src_planner_expression_binder.cpp
  src/duckdb/ub_src_planner_filter.cpp
  src/duckdb/ub_src_planner_operator.cpp
  src/duckdb/ub_src_planner_subquery.cpp
  src/duckdb/ub_src_storage.cpp
  src/duckdb/ub_src_storage_buffer.cpp
  src/duckdb/ub_src_storage_checkpoint.cpp
  src/duckdb/ub_src_storage_compression_alp.cpp
  src/duckdb/ub_src_storage_compression.cpp
  src/duckdb/ub_src_storage_compression_chimp.cpp
  src/duckdb/ub_src_storage_compression_dict_fsst.cpp
  src/duckdb/ub_src_storage_compression_dictionary.cpp
  src/duckdb/ub_src_storage_compression_roaring.cpp
  src/duckdb/ub_src_storage_metadata.cpp
  src/duckdb/ub_src_storage_serialization.cpp
  src/duckdb/ub_src_storage_statistics.cpp
  src/duckdb/ub_src_storage_table.cpp
  src/duckdb/ub_src_transaction.cpp
  src/duckdb/src/verification/copied_statement_verifier.cpp
  src/duckdb/src/verification/deserialized_statement_verifier.cpp
  src/duckdb/src/verification/external_statement_verifier.cpp
  src/duckdb/src/verification/fetch_row_verifier.cpp
  src/duckdb/src/verification/no_operator_caching_verifier.cpp
  src/duckdb/src/verification/parsed_statement_verifier.cpp
  src/duckdb/src/verification/prepared_statement_verifier.cpp
  src/duckdb/src/verification/statement_verifier.cpp
  src/duckdb/src/verification/unoptimized_statement_verifier.cpp
  src/duckdb/third_party/fmt/format.cc
  src/duckdb/third_party/fsst/libfsst.cpp
  src/duckdb/third_party/miniz/miniz.cpp
  src/duckdb/third_party/re2/re2/bitmap256.cc
  src/duckdb/third_party/re2/re2/bitstate.cc
  src/duckdb/third_party/re2/re2/compile.cc
  src/duckdb/third_party/re2/re2/dfa.cc
  src/duckdb/third_party/re2/re2/filtered_re2.cc
  src/duckdb/third_party/re2/re2/mimics_pcre.cc
  src/duckdb/third_party/re2/re2/nfa.cc
  src/duckdb/third_party/re2/re2/onepass.cc
  src/duckdb/third_party/re2/re2/parse.cc
  src/duckdb/third_party/re2/re2/perl_groups.cc
  src/duckdb/third_party/re2/re2/prefilter.cc
  src/duckdb/third_party/re2/re2/prefilter_tree.cc
  src/duckdb/third_party/re2/re2/prog.cc
  src/duckdb/third_party/re2/re2/re2.cc
  src/duckdb/third_party/re2/re2/regexp.cc
  src/duckdb/third_party/re2/re2/set.cc
  src/duckdb/third_party/re2/re2/simplify.cc
  src/duckdb/third_party/re2/re2/stringpiece.cc
  src/duckdb/third_party/re2/re2/tostring.cc
  src/duckdb/third_party/re2/re2/unicode_casefold.cc
  src/duckdb/third_party/re2/re2/unicode_groups.cc
  src/duckdb/third_party/re2/util/rune.cc
  src/duckdb/third_party/re2/util/strutil.cc
  src/duckdb/third_party/hyperloglog/hyperloglog.cpp
  src/duckdb/third_party/hyperloglog/sds.cpp
  src/duckdb/third_party/skiplist/SkipList.cpp
  src/duckdb/third_party/fastpforlib/bitpacking.cpp
  src/duckdb/third_party/utf8proc/utf8proc.cpp
  src/duckdb/third_party/utf8proc/utf8proc_wrapper.cpp
  src/duckdb/third_party/libpg_query/pg_functions.cpp
  src/duckdb/third_party/libpg_query/postgres_parser.cpp
  src/duckdb/third_party/libpg_query/src_backend_nodes_list.cpp
  src/duckdb/third_party/libpg_query/src_backend_nodes_makefuncs.cpp
  src/duckdb/third_party/libpg_query/src_backend_nodes_value.cpp
  src/duckdb/third_party/libpg_query/src_backend_parser_gram.cpp
  src/duckdb/third_party/libpg_query/src_backend_parser_parser.cpp
  src/duckdb/third_party/libpg_query/src_backend_parser_scan.cpp
  src/duckdb/third_party/libpg_query/src_backend_parser_scansup.cpp
  src/duckdb/third_party/libpg_query/src_common_keywords.cpp
  src/duckdb/third_party/mbedtls/library/aes.cpp
  src/duckdb/third_party/mbedtls/library/asn1parse.cpp
  src/duckdb/third_party/mbedtls/library/asn1write.cpp
  src/duckdb/third_party/mbedtls/library/base64.cpp
  src/duckdb/third_party/mbedtls/library/bignum.cpp
  src/duckdb/third_party/mbedtls/library/bignum_core.cpp
  src/duckdb/third_party/mbedtls/library/cipher.cpp
  src/duckdb/third_party/mbedtls/library/cipher_wrap.cpp
  src/duckdb/third_party/mbedtls/library/constant_time.cpp
  src/duckdb/third_party/mbedtls/library/gcm.cpp
  src/duckdb/third_party/mbedtls/library/md.cpp
  src/duckdb/third_party/mbedtls/library/oid.cpp
  src/duckdb/third_party/mbedtls/library/pem.cpp
  src/duckdb/third_party/mbedtls/library/pk.cpp
  src/duckdb/third_party/mbedtls/library/pk_wrap.cpp
  src/duckdb/third_party/mbedtls/library/pkparse.cpp
  src/duckdb/third_party/mbedtls/library/platform.cpp
  src/duckdb/third_party/mbedtls/library/platform_util.cpp
  src/duckdb/third_party/mbedtls/library/rsa.cpp
  src/duckdb/third_party/mbedtls/library/rsa_alt_helpers.cpp
  src/duckdb/third_party/mbedtls/library/sha1.cpp
  src/duckdb/third_party/mbedtls/library/sha256.cpp
  src/duckdb/third_party/mbedtls/mbedtls_wrapper.cpp
  src/duckdb/third_party/yyjson/yyjson.cpp
  src/duckdb/third_party/zstd/common/debug.cpp
  src/duckdb/third_party/zstd/common/entropy_common.cpp
  src/duckdb/third_party/zstd/common/error_private.cpp
  src/duckdb/third_party/zstd/common/fse_decompress.cpp
  src/duckdb/third_party/zstd/common/pool.cpp
  src/duckdb/third_party/zstd/common/threading.cpp
  src/duckdb/third_party/zstd/common/xxhash.cpp
  src/duckdb/third_party/zstd/common/zstd_common.cpp
  src/duckdb/third_party/zstd/compress/fse_compress.cpp
  src/duckdb/third_party/zstd/compress/hist.cpp
  src/duckdb/third_party/zstd/compress/huf_compress.cpp
  src/duckdb/third_party/zstd/compress/zstd_compress.cpp
  src/duckdb/third_party/zstd/compress/zstd_compress_literals.cpp
  src/duckdb/third_party/zstd/compress/zstd_compress_sequences.cpp
  src/duckdb/third_party/zstd/compress/zstd_compress_superblock.cpp
  src/duckdb/third_party/zstd/compress/zstd_double_fast.cpp
  src/duckdb/third_party/zstd/compress/zstd_fast.cpp
  src/duckdb/third_party/zstd/compress/zstd_lazy.cpp
  src/duckdb/third_party/zstd/compress/zstd_ldm.cpp
  src/duckdb/third_party/zstd/compress/zstd_opt.cpp
  src/duckdb/third_party/zstd/compress/zstdmt_compress.cpp
  src/duckdb/third_party/zstd/decompress/huf_decompress.cpp
  src/duckdb/third_party/zstd/decompress/zstd_ddict.cpp
  src/duckdb/third_party/zstd/decompress/zstd_decompress.cpp
  src/duckdb/third_party/zstd/decompress/zstd_decompress_block.cpp
  src/duckdb/third_party/zstd/deprecated/zbuff_common.cpp
  src/duckdb/third_party/zstd/deprecated/zbuff_compress.cpp
  src/duckdb/third_party/zstd/deprecated/zbuff_decompress.cpp
  src/duckdb/third_party/zstd/dict/cover.cpp
  src/duckdb/third_party/zstd/dict/divsufsort.cpp
  src/duckdb/third_party/zstd/dict/fastcover.cpp
  src/duckdb/third_party/zstd/dict/zdict.cpp
  src/duckdb/extension/core_functions/core_functions_extension.cpp
  src/duckdb/extension/core_functions/function_list.cpp
  src/duckdb/extension/core_functions/lambda_functions.cpp
  src/duckdb/ub_extension_core_functions_scalar_map.cpp
  src/duckdb/ub_extension_core_functions_scalar_operators.cpp
  src/duckdb/ub_extension_core_functions_scalar_struct.cpp
  src/duckdb/ub_extension_core_functions_scalar_random.cpp
  src/duckdb/ub_extension_core_functions_scalar_string.cpp
  src/duckdb/ub_extension_core_functions_scalar_blob.cpp
  src/duckdb/ub_extension_core_functions_scalar_union.cpp
  src/duckdb/ub_extension_core_functions_scalar_bit.cpp
  src/duckdb/ub_extension_core_functions_scalar_array.cpp
  src/duckdb/ub_extension_core_functions_scalar_math.cpp
  src/duckdb/ub_extension_core_functions_scalar_debug.cpp
  src/duckdb/ub_extension_core_functions_scalar_enum.cpp
  src/duckdb/ub_extension_core_functions_scalar_generic.cpp
  src/duckdb/ub_extension_core_functions_scalar_date.cpp
  src/duckdb/ub_extension_core_functions_scalar_list.cpp
  src/duckdb/ub_extension_core_functions_aggregate_nested.cpp
  src/duckdb/ub_extension_core_functions_aggregate_regression.cpp
  src/duckdb/ub_extension_core_functions_aggregate_algebraic.cpp
  src/duckdb/ub_extension_core_functions_aggregate_distributive.cpp
  src/duckdb/ub_extension_core_functions_aggregate_holistic.cpp
  src/duckdb/extension/parquet/parquet_writer.cpp
  src/duckdb/extension/parquet/geo_parquet.cpp
  src/duckdb/extension/parquet/column_reader.cpp
  src/duckdb/extension/parquet/parquet_float16.cpp
  src/duckdb/extension/parquet/parquet_metadata.cpp
  src/duckdb/extension/parquet/parquet_reader.cpp
  src/duckdb/extension/parquet/zstd_file_system.cpp
  src/duckdb/extension/parquet/parquet_timestamp.cpp
  src/duckdb/extension/parquet/parquet_multi_file_info.cpp
  src/duckdb/extension/parquet/column_writer.cpp
  src/duckdb/extension/parquet/parquet_extension.cpp
  src/duckdb/extension/parquet/parquet_crypto.cpp
  src/duckdb/extension/parquet/parquet_file_metadata_cache.cpp
  src/duckdb/extension/parquet/serialize_parquet.cpp
  src/duckdb/extension/parquet/parquet_statistics.cpp
  src/duckdb/ub_extension_parquet_decoder.cpp
  src/duckdb/ub_extension_parquet_reader.cpp
  src/duckdb/ub_extension_parquet_reader_variant.cpp
  src/duckdb/ub_extension_parquet_writer.cpp
  src/duckdb/third_party/parquet/parquet_types.cpp
  src/duckdb/third_party/thrift/thrift/protocol/TProtocol.cpp
  src/duckdb/third_party/thrift/thrift/transport/TTransportException.cpp
  src/duckdb/third_party/thrift/thrift/transport/TBufferTransports.cpp
  src/duckdb/third_party/snappy/snappy.cc
  src/duckdb/third_party/snappy/snappy-sinksource.cc
  src/duckdb/third_party/lz4/lz4.cpp
  src/duckdb/third_party/brotli/common/constants.cpp
  src/duckdb/third_party/brotli/common/context.cpp
  src/duckdb/third_party/brotli/common/dictionary.cpp
  src/duckdb/third_party/brotli/common/platform.cpp
  src/duckdb/third_party/brotli/common/shared_dictionary.cpp
  src/duckdb/third_party/brotli/common/transform.cpp
  src/duckdb/third_party/brotli/dec/bit_reader.cpp
  src/duckdb/third_party/brotli/dec/decode.cpp
  src/duckdb/third_party/brotli/dec/huffman.cpp
  src/duckdb/third_party/brotli/dec/state.cpp
  src/duckdb/third_party/brotli/enc/backward_references.cpp
  src/duckdb/third_party/brotli/enc/backward_references_hq.cpp
  src/duckdb/third_party/brotli/enc/bit_cost.cpp
  src/duckdb/third_party/brotli/enc/block_splitter.cpp
  src/duckdb/third_party/brotli/enc/brotli_bit_stream.cpp
  src/duckdb/third_party/brotli/enc/cluster.cpp
  src/duckdb/third_party/brotli/enc/command.cpp
  src/duckdb/third_party/brotli/enc/compound_dictionary.cpp
  src/duckdb/third_party/brotli/enc/compress_fragment.cpp
  src/duckdb/third_party/brotli/enc/compress_fragment_two_pass.cpp
  src/duckdb/third_party/brotli/enc/dictionary_hash.cpp
  src/duckdb/third_party/brotli/enc/encode.cpp
  src/duckdb/third_party/brotli/enc/encoder_dict.cpp
  src/duckdb/third_party/brotli/enc/entropy_encode.cpp
  src/duckdb/third_party/brotli/enc/fast_log.cpp
  src/duckdb/third_party/brotli/enc/histogram.cpp
  src/duckdb/third_party/brotli/enc/literal_cost.cpp
  src/duckdb/third_party/brotli/enc/memory.cpp
  src/duckdb/third_party/brotli/enc/metablock.cpp
  src/duckdb/third_party/brotli/enc/static_dict.cpp
  src/duckdb/third_party/brotli/enc/utf8_util.cpp
  src/duckdb/extension/icu/./icu-list-range.cpp
  src/duckdb/extension/icu/./icu_extension.cpp
  src/duckdb/extension/icu/./icu-strptime.cpp
  src/duckdb/extension/icu/./icu-current.cpp
  src/duckdb/extension/icu/./icu-makedate.cpp
  src/duckdb/extension/icu/./icu-datesub.cpp
  src/duckdb/extension/icu/./icu-timezone.cpp
  src/duckdb/extension/icu/./icu-timebucket.cpp
  src/duckdb/extension/icu/./icu-table-range.cpp
  src/duckdb/extension/icu/./icu-datefunc.cpp
  src/duckdb/extension/icu/./icu-datepart.cpp
  src/duckdb/extension/icu/./icu-datetrunc.cpp
  src/duckdb/extension/icu/./icu-dateadd.cpp
  src/duckdb/ub_extension_icu_third_party_icu_common.cpp
  src/duckdb/ub_extension_icu_third_party_icu_i18n.cpp
  src/duckdb/extension/icu/third_party/icu/stubdata/stubdata.cpp
  src/duckdb/extension/json/json_common.cpp
  src/duckdb/extension/json/json_extension.cpp
  src/duckdb/extension/json/json_multi_file_info.cpp
  src/duckdb/extension/json/json_scan.cpp
  src/duckdb/extension/json/json_enums.cpp
  src/duckdb/extension/json/json_functions.cpp
  src/duckdb/extension/json/json_reader.cpp
  src/duckdb/extension/json/json_deserializer.cpp
  src/duckdb/extension/json/serialize_json.cpp
  src/duckdb/extension/json/json_serializer.cpp
  src/duckdb/ub_extension_json_json_functions.cpp)

set(JEMALLOC_SRC_FILES 
  src/duckdb/extension/jemalloc/jemalloc_extension.cpp
  src/duckdb/extension/jemalloc/jemalloc/src/jemalloc.c
  src/duckdb/extension/jemalloc/jemalloc/src/arena.c
  src/duckdb/extension/jemalloc/jemalloc/src/background_thread.c
  src/duckdb/extension/jemalloc/jemalloc/src/base.c
  src/duckdb/extension/jemalloc/jemalloc/src/batcher.c
  src/duckdb/extension/jemalloc/jemalloc/src/bin.c
  src/duckdb/extension/jemalloc/jemalloc/src/bin_info.c
  src/duckdb/extension/jemalloc/jemalloc/src/bitmap.c
  src/duckdb/extension/jemalloc/jemalloc/src/buf_writer.c
  src/duckdb/extension/jemalloc/jemalloc/src/cache_bin.c
  src/duckdb/extension/jemalloc/jemalloc/src/ckh.c
  src/duckdb/extension/jemalloc/jemalloc/src/counter.c
  src/duckdb/extension/jemalloc/jemalloc/src/ctl.c
  src/duckdb/extension/jemalloc/jemalloc/src/decay.c
  src/duckdb/extension/jemalloc/jemalloc/src/div.c
  src/duckdb/extension/jemalloc/jemalloc/src/ecache.c
  src/duckdb/extension/jemalloc/jemalloc/src/edata.c
  src/duckdb/extension/jemalloc/jemalloc/src/edata_cache.c
  src/duckdb/extension/jemalloc/jemalloc/src/ehooks.c
  src/duckdb/extension/jemalloc/jemalloc/src/emap.c
  src/duckdb/extension/jemalloc/jemalloc/src/eset.c
  src/duckdb/extension/jemalloc/jemalloc/src/exp_grow.c
  src/duckdb/extension/jemalloc/jemalloc/src/extent.c
  src/duckdb/extension/jemalloc/jemalloc/src/extent_dss.c
  src/duckdb/extension/jemalloc/jemalloc/src/extent_mmap.c
  src/duckdb/extension/jemalloc/jemalloc/src/fxp.c
  src/duckdb/extension/jemalloc/jemalloc/src/san.c
  src/duckdb/extension/jemalloc/jemalloc/src/san_bump.c
  src/duckdb/extension/jemalloc/jemalloc/src/hook.c
  src/duckdb/extension/jemalloc/jemalloc/src/hpa.c
  src/duckdb/extension/jemalloc/jemalloc/src/hpa_hooks.c
  src/duckdb/extension/jemalloc/jemalloc/src/hpdata.c
  src/duckdb/extension/jemalloc/jemalloc/src/inspect.c
  src/duckdb/extension/jemalloc/jemalloc/src/large.c
  src/duckdb/extension/jemalloc/jemalloc/src/log.c
  src/duckdb/extension/jemalloc/jemalloc/src/malloc_io.c
  src/duckdb/extension/jemalloc/jemalloc/src/mutex.c
  src/duckdb/extension/jemalloc/jemalloc/src/nstime.c
  src/duckdb/extension/jemalloc/jemalloc/src/pa.c
  src/duckdb/extension/jemalloc/jemalloc/src/pa_extra.c
  src/duckdb/extension/jemalloc/jemalloc/src/pai.c
  src/duckdb/extension/jemalloc/jemalloc/src/pac.c
  src/duckdb/extension/jemalloc/jemalloc/src/pages.c
  src/duckdb/extension/jemalloc/jemalloc/src/peak_event.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof_data.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof_log.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof_recent.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof_stats.c
  src/duckdb/extension/jemalloc/jemalloc/src/prof_sys.c
  src/duckdb/extension/jemalloc/jemalloc/src/psset.c
  src/duckdb/extension/jemalloc/jemalloc/src/rtree.c
  src/duckdb/extension/jemalloc/jemalloc/src/safety_check.c
  src/duckdb/extension/jemalloc/jemalloc/src/sc.c
  src/duckdb/extension/jemalloc/jemalloc/src/sec.c
  src/duckdb/extension/jemalloc/jemalloc/src/stats.c
  src/duckdb/extension/jemalloc/jemalloc/src/sz.c
  src/duckdb/extension/jemalloc/jemalloc/src/tcache.c
  src/duckdb/extension/jemalloc/jemalloc/src/test_hooks.c
  src/duckdb/extension/jemalloc/jemalloc/src/thread_event.c
  src/duckdb/extension/jemalloc/jemalloc/src/ticker.c
  src/duckdb/extension/jemalloc/jemalloc/src/tsd.c
  src/duckdb/extension/jemalloc/jemalloc/src/util.c
  src/duckdb/extension/jemalloc/jemalloc/src/witness.c
  src/duckdb/extension/jemalloc/jemalloc/src/zone.c)


# a few OS-specific variables

set(OS_NAME "unknown")
set(OS_ARCH "amd64")

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "${CMAKE_SYSTEM_PROCESSOR}")
if(IS_ARM)
  set(OS_ARCH "arm64")
elseif(FORCE_32_BIT)
  set(OS_ARCH "i386")
endif()

if(APPLE)
  set(OS_NAME "osx")
  set(OS_ARCH "universal")
  SET(CMAKE_OSX_ARCHITECTURES "x86_64;arm64" CACHE STRING "Build architectures for Mac OS X" FORCE)
endif()
if(WIN32)
  set(OS_NAME "windows")
endif()
if(UNIX AND NOT APPLE)
  set(OS_NAME "linux") # sorry BSD
endif()

if(OVERRIDE_JDBC_OS_ARCH)
  set(OS_ARCH ${OVERRIDE_JDBC_OS_ARCH})
endif()


# Java compilation

file(GLOB_RECURSE JAVA_SRC_FILES src/main/java/org/duckdb/*.java)
file(GLOB_RECURSE JAVA_TEST_FILES src/test/java/org/duckdb/*.java)
set(CMAKE_JAVA_COMPILE_FLAGS -encoding utf-8 -g -Xlint:all)

add_jar(duckdb_jdbc ${JAVA_SRC_FILES} META-INF/services/java.sql.Driver
        MANIFEST META-INF/MANIFEST.MF
        GENERATE_NATIVE_HEADERS duckdb-native)
add_jar(duckdb_jdbc_tests ${JAVA_TEST_FILES} INCLUDE_JARS duckdb_jdbc)


# main shared lib compilation

if(MSVC)
  list(APPEND DUCKDB_SRC_FILES duckdb_java.def)
else()
  list(APPEND DUCKDB_SRC_FILES ${JEMALLOC_SRC_FILES})
endif()

add_library(duckdb_java SHARED
  src/jni/bindings_appender.cpp
  src/jni/bindings_common.cpp
  src/jni/bindings_data_chunk.cpp
  src/jni/bindings_logical_type.cpp
  src/jni/bindings_validity.cpp
  src/jni/bindings_vector.cpp
  src/jni/config.cpp
  src/jni/duckdb_java.cpp
  src/jni/functions.cpp
  src/jni/refs.cpp
  src/jni/types.cpp
  src/jni/util.cpp
  ${DUCKDB_SRC_FILES})

target_include_directories(duckdb_java PRIVATE
  ${JAVA_INCLUDE_PATH}
  ${JAVA_INCLUDE_PATH2}
  ${DUCKDB_INCLUDE_DIRS})

if (NOT MSVC)
  target_include_directories(duckdb_java PRIVATE
    ${JEMALLOC_INCLUDE_DIRS})
endif()

target_link_libraries(duckdb_java PRIVATE
  duckdb-native
  ${CMAKE_DL_LIBS})

if(MSVC)
  target_link_libraries(duckdb_java PRIVATE
    ws2_32
    rstrtmgr
    bcrypt)
endif()

if(MSVC)
  target_compile_options(duckdb_java PRIVATE
    /bigobj)
else()
  target_compile_options(duckdb_java PRIVATE
    -fexceptions)
endif()

target_compile_definitions(duckdb_java PRIVATE
  ${DUCKDB_DEFINITIONS}
  -DDUCKDB_BUILD_LIBRARY
  -DDUCKDB_STATIC_BUILD
  -DDUCKDB_EXTENSION_AUTOLOAD_DEFAULT
  -DDUCKDB_EXTENSION_AUTOINSTALL_DEFAULT)

if(MSVC)
  target_compile_definitions(duckdb_java PRIVATE
    -D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
else()
  target_compile_definitions(duckdb_java PRIVATE
    -DDUCKDB_EXTENSION_JEMALLOC_LINKED)
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_options(duckdb_java PRIVATE
    -Bsymbolic
    -Bsymbolic-functions
    -fvisibility=hidden 
    -Wl,--version-script=${CMAKE_CURRENT_LIST_DIR}/duckdb_java.map)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  target_link_options(duckdb_java PRIVATE
    -fvisibility=hidden
    -Wl,-exported_symbols_list,${CMAKE_CURRENT_LIST_DIR}/duckdb_java.exp)
endif()

string(JOIN "_" LIB_SUFFIX ".so" ${OS_NAME} ${OS_ARCH})
set_target_properties(duckdb_java PROPERTIES SUFFIX ${LIB_SUFFIX})
set_target_properties(duckdb_java PROPERTIES PREFIX "lib")


# JAR packing

add_custom_command(
  OUTPUT dummy_jdbc_target
  DEPENDS duckdb_java duckdb_jdbc duckdb_jdbc_tests
  COMMAND ${Java_JAR_EXECUTABLE} uf duckdb_jdbc.jar -C
          $<TARGET_FILE_DIR:duckdb_java> $<TARGET_FILE_NAME:duckdb_java>)

add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)

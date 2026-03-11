#pragma once

#include <jni.h>

void _duckdb_jdbc_register_scalar_udf(JNIEnv *env, jclass clazz, jobject conn_ref_buf, jbyteArray name_j,
                                      jobject callback, jobjectArray argument_logical_types_j,
                                      jobject return_logical_type_j, jboolean special_handling,
                                      jboolean return_null_on_exception, jboolean deterministic, jboolean var_args);

void _duckdb_jdbc_register_scalar_udf_on_function(JNIEnv *env, jclass clazz, jobject conn_ref_buf,
                                                  jobject scalar_function_buf, jobject callback,
                                                  jobjectArray argument_logical_types_j, jobject return_logical_type_j,
                                                  jboolean return_null_on_exception, jboolean var_args);

void _duckdb_jdbc_register_table_function(JNIEnv *env, jclass clazz, jobject conn_ref_buf, jbyteArray name_j,
                                          jobject callback, jobjectArray parameter_types_j,
                                          jboolean supports_projection_pushdown, jint max_threads,
                                          jboolean thread_safe);

void _duckdb_jdbc_register_table_function_on_function(JNIEnv *env, jclass clazz, jobject conn_ref_buf,
                                                      jobject table_function_buf, jobject callback,
                                                      jobjectArray parameter_types_j, jint max_threads,
                                                      jboolean thread_safe);

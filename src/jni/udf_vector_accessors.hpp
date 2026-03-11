#pragma once

#include <jni.h>

jbyteArray _duckdb_jdbc_udf_get_varchar_bytes(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row);

void _duckdb_jdbc_udf_set_varchar_bytes(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row, jbyteArray value);

jbyteArray _duckdb_jdbc_udf_get_blob_bytes(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row);

void _duckdb_jdbc_udf_set_blob_bytes(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row, jbyteArray value);

jobject _duckdb_jdbc_udf_get_decimal(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row);

void _duckdb_jdbc_udf_set_decimal(JNIEnv *env, jclass clazz, jobject vector_ref_buf, jint row, jobject value);

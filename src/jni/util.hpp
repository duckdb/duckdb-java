#pragma once

#include "duckdb.hpp"

#include <jni.h>
#include <string>

void check_java_exception_and_rethrow(JNIEnv *env);

std::string byte_array_to_string(JNIEnv *env, jbyteArray ba_j);

std::string jstring_to_string(JNIEnv *env, jstring string_j);

jobject decode_charbuffer_to_jstring(JNIEnv *env, const char *d_str, idx_t d_str_len);

duckdb::Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal);

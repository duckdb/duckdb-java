#pragma once

extern "C" {
#include "duckdb.h"
}

#include <functional>
#include <jni.h>
#include <memory>
#include <string>

using jbyteArray_ptr = std::unique_ptr<char, std::function<void(char *)>>;

using varchar_ptr = std::unique_ptr<char, void (*)(char *)>;

inline void varchar_deleter(char *val) {
	duckdb_free(val);
}

void check_java_exception_and_rethrow(JNIEnv *env);

std::string jbyteArray_to_string(JNIEnv *env, jbyteArray ba_j);

std::string jstring_to_string(JNIEnv *env, jstring string_j);

jobject decode_charbuffer_to_jstring(JNIEnv *env, const char *d_str, idx_t d_str_len);

jlong uint64_to_jlong(uint64_t value);

idx_t jlong_to_idx(JNIEnv *env, jlong value);

void check_out_param(JNIEnv *env, jobjectArray out_param);

void set_out_param(JNIEnv *env, jobjectArray out_param, jobject value);

jbyteArray_ptr make_jbyteArray_ptr(JNIEnv *env, jbyteArray jbytes);

jbyteArray make_jbyteArray(JNIEnv *env, const char *data, idx_t len);

jobject make_ptr_buf(JNIEnv *env, void *ptr);

jobject make_data_buf(JNIEnv *env, void *data, idx_t len);

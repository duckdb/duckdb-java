#pragma once

#include "capi_v2.hpp"

#include <cstdint>
#include <functional>
#include <jni.h>
#include <memory>
#include <string>

using jbyteArray_ptr = std::unique_ptr<char, std::function<void(char *)>>;

void check_java_exception_and_rethrow(JNIEnv *env);

std::string jbyteArray_to_string(JNIEnv *env, jbyteArray jbytes);

std::string jstring_to_string(JNIEnv *env, jstring string_j);

jstring decode_charbuffer_to_jstring(JNIEnv *env, const char *cstr, idx_t len);

jlong uint64_to_jlong(uint64_t value);

idx_t jlong_to_idx(JNIEnv *env, jlong value);

void check_out_param(JNIEnv *env, jobjectArray out_param);

void set_out_param(JNIEnv *env, jobjectArray out_param, jobject value);

jbyteArray_ptr make_jbyteArray_ptr(JNIEnv *env, jbyteArray jbytes);

jbyteArray make_jbyteArray(JNIEnv *env, const char *data, idx_t len);

jobject make_ptr_buf(JNIEnv *env, void *ptr);

jobject make_data_buf(JNIEnv *env, void *data, idx_t len);

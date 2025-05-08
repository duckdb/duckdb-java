#include "refs.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

jclass J_Charset;
jmethodID J_Charset_decode;
jclass J_StandardCharsets;
jobject J_Charset_UTF8;

jclass J_CharBuffer;
jmethodID J_CharBuffer_toString;

jmethodID J_String_getBytes;

jclass J_Throwable;
jmethodID J_Throwable_getMessage;
jclass J_SQLException;

jclass J_Bool;
jclass J_Byte;
jclass J_Short;
jclass J_Int;
jclass J_Long;
jclass J_Float;
jclass J_Double;
jclass J_String;
jclass J_Timestamp;
jmethodID J_Timestamp_valueOf;
jclass J_TimestampTZ;
jclass J_BigDecimal;
jclass J_HugeInt;
jclass J_ByteArray;

jmethodID J_Bool_booleanValue;
jmethodID J_Byte_byteValue;
jmethodID J_Short_shortValue;
jmethodID J_Int_intValue;
jmethodID J_Long_longValue;
jmethodID J_Float_floatValue;
jmethodID J_Double_doubleValue;
jmethodID J_Timestamp_getMicrosEpoch;
jmethodID J_TimestampTZ_getMicrosEpoch;
jmethodID J_BigDecimal_precision;
jmethodID J_BigDecimal_scale;
jmethodID J_BigDecimal_scaleByPowTen;
jmethodID J_BigDecimal_toPlainString;
jmethodID J_BigDecimal_longValue;
jfieldID J_HugeInt_lower;
jfieldID J_HugeInt_upper;

jclass J_DuckResultSetMeta;
jmethodID J_DuckResultSetMeta_init;

jclass J_DuckVector;
jmethodID J_DuckVector_init;
jfieldID J_DuckVector_constlen;
jfieldID J_DuckVector_varlen;

jclass J_DuckArray;
jmethodID J_DuckArray_init;

jclass J_Struct;
jmethodID J_Struct_getSQLTypeName;
jmethodID J_Struct_getAttributes;

jclass J_Array;
jmethodID J_Array_getBaseTypeName;
jmethodID J_Array_getArray;

jclass J_DuckStruct;
jmethodID J_DuckStruct_init;

jclass J_ByteBuffer;

jclass J_DuckMap;
jmethodID J_DuckMap_getSQLTypeName;

jclass J_List;
jmethodID J_List_iterator;
jclass J_Map;
jmethodID J_Map_entrySet;
jclass J_Set;
jmethodID J_Set_iterator;
jclass J_Iterator;
jmethodID J_Iterator_hasNext;
jmethodID J_Iterator_next;
jclass J_Entry;
jmethodID J_Entry_getKey;
jmethodID J_Entry_getValue;

jclass J_UUID;
jmethodID J_UUID_getMostSignificantBits;
jmethodID J_UUID_getLeastSignificantBits;

jclass J_DuckDBDate;
jmethodID J_DuckDBDate_getDaysSinceEpoch;

jclass J_Object;
jmethodID J_Object_toString;

jclass J_DuckDBTime;

jclass J_ProfilerPrintFormat;
jobject J_ProfilerPrintFormat_QUERY_TREE;
jobject J_ProfilerPrintFormat_JSON;
jobject J_ProfilerPrintFormat_QUERY_TREE_OPTIMIZER;
jobject J_ProfilerPrintFormat_NO_OUTPUT;
jobject J_ProfilerPrintFormat_HTML;
jobject J_ProfilerPrintFormat_GRAPHVIZ;

jclass J_QueryProgress;
jmethodID J_QueryProgress_init;

static std::vector<jobject> global_refs;

template <typename T>
static void check_not_null(T ptr, const std::string &message) {
	if (nullptr == ptr) {
		throw std::runtime_error(message);
	}
}

static jclass make_class_ref(JNIEnv *env, const std::string &name) {
	jclass local_ref = env->FindClass(name.c_str());
	check_not_null(local_ref, "Class not found, name: [" + name + "]");
	jclass global_ref = reinterpret_cast<jclass>(env->NewGlobalRef(local_ref));
	check_not_null(global_ref, "Cannot create global ref for class, name: [" + name + "]");
	env->DeleteLocalRef(local_ref);
	global_refs.emplace_back(global_ref);
	return global_ref;
}

static jmethodID get_method_id(JNIEnv *env, jclass clazz, const std::string &name, const std::string &sig) {
	jmethodID method_id = env->GetMethodID(clazz, name.c_str(), sig.c_str());
	check_not_null(method_id, "Method not found, name: [" + name + "], signature: [" + sig + "]");
	return method_id;
}

static jmethodID get_static_method_id(JNIEnv *env, jclass clazz, const std::string &name, const std::string &sig) {
	jmethodID method_id = env->GetStaticMethodID(clazz, name.c_str(), sig.c_str());
	check_not_null(method_id, "Static method not found, name: [" + name + "], signature: [" + sig + "]");
	return method_id;
}

static jfieldID get_field_id(JNIEnv *env, jclass clazz, const std::string &name, const std::string &sig) {
	jfieldID field_id = env->GetFieldID(clazz, name.c_str(), sig.c_str());
	check_not_null(field_id, "Field not found, name: [" + name + "], signature: [" + sig + "]");
	return field_id;
}

static jobject make_static_object_field_ref(JNIEnv *env, jclass clazz, const std::string &name,
                                            const std::string &sig) {
	jfieldID field_id = env->GetStaticFieldID(clazz, name.c_str(), sig.c_str());
	check_not_null(field_id, "Static field not found, name: [" + name + "], signature: [" + sig + "]");
	jobject local_ref = env->GetStaticObjectField(clazz, field_id);
	check_not_null(local_ref, "Specified static field is null, name: [" + name + "], signature: [" + sig + "]");
	jobject global_ref = env->NewGlobalRef(local_ref);
	check_not_null(global_ref,
	               "Cannot create global ref for static field, name: [" + name + "], signature: [" + sig + "]");
	env->DeleteLocalRef(local_ref);
	global_refs.emplace_back(global_ref);
	return global_ref;
}

void create_refs(JNIEnv *env) {
	jclass tmpLocalRef;

	J_Charset = make_class_ref(env, "java/nio/charset/Charset");
	J_Charset_decode = get_method_id(env, J_Charset, "decode", "(Ljava/nio/ByteBuffer;)Ljava/nio/CharBuffer;");
	J_StandardCharsets = make_class_ref(env, "java/nio/charset/StandardCharsets");
	J_Charset_UTF8 = make_static_object_field_ref(env, J_StandardCharsets, "UTF_8", "Ljava/nio/charset/Charset;");
	J_CharBuffer = make_class_ref(env, "java/nio/CharBuffer");
	J_CharBuffer_toString = get_method_id(env, J_CharBuffer, "toString", "()Ljava/lang/String;");

	J_Throwable = make_class_ref(env, "java/lang/Throwable");
	J_Throwable_getMessage = get_method_id(env, J_Throwable, "getMessage", "()Ljava/lang/String;");
	J_SQLException = make_class_ref(env, "java/sql/SQLException");

	J_Bool = make_class_ref(env, "java/lang/Boolean");
	J_Byte = make_class_ref(env, "java/lang/Byte");
	J_Short = make_class_ref(env, "java/lang/Short");
	J_Int = make_class_ref(env, "java/lang/Integer");
	J_Long = make_class_ref(env, "java/lang/Long");
	J_Float = make_class_ref(env, "java/lang/Float");
	J_Double = make_class_ref(env, "java/lang/Double");
	J_String = make_class_ref(env, "java/lang/String");
	J_BigDecimal = make_class_ref(env, "java/math/BigDecimal");
	J_HugeInt = make_class_ref(env, "org/duckdb/DuckDBHugeInt");
	J_ByteArray = make_class_ref(env, "[B");

	J_Timestamp = make_class_ref(env, "org/duckdb/DuckDBTimestamp");
	J_Timestamp_valueOf = get_static_method_id(env, J_Timestamp, "valueOf", "(Ljava/lang/Object;)Ljava/lang/Object;");
	J_TimestampTZ = make_class_ref(env, "org/duckdb/DuckDBTimestampTZ");

	J_DuckDBDate = make_class_ref(env, "org/duckdb/DuckDBDate");
	J_DuckDBDate_getDaysSinceEpoch = get_method_id(env, J_DuckDBDate, "getDaysSinceEpoch", "()J");
	J_DuckDBTime = make_class_ref(env, "org/duckdb/DuckDBTime");

	J_DuckMap = make_class_ref(env, "org/duckdb/user/DuckDBMap");
	J_DuckMap_getSQLTypeName = get_method_id(env, J_DuckMap, "getSQLTypeName", "()Ljava/lang/String;");

	J_List = make_class_ref(env, "java/util/List");
	J_List_iterator = get_method_id(env, J_List, "iterator", "()Ljava/util/Iterator;");
	J_Map = make_class_ref(env, "java/util/Map");
	J_Map_entrySet = get_method_id(env, J_Map, "entrySet", "()Ljava/util/Set;");
	J_Set = make_class_ref(env, "java/util/Set");
	J_Set_iterator = get_method_id(env, J_Set, "iterator", "()Ljava/util/Iterator;");
	J_Iterator = make_class_ref(env, "java/util/Iterator");
	J_Iterator_hasNext = get_method_id(env, J_Iterator, "hasNext", "()Z");
	J_Iterator_next = get_method_id(env, J_Iterator, "next", "()Ljava/lang/Object;");

	J_UUID = make_class_ref(env, "java/util/UUID");
	J_UUID_getMostSignificantBits = get_method_id(env, J_UUID, "getMostSignificantBits", "()J");
	J_UUID_getLeastSignificantBits = get_method_id(env, J_UUID, "getLeastSignificantBits", "()J");

	J_DuckArray = make_class_ref(env, "org/duckdb/DuckDBArray");
	J_DuckArray_init = get_method_id(env, J_DuckArray, "<init>", "(Lorg/duckdb/DuckDBVector;II)V");

	J_DuckStruct = make_class_ref(env, "org/duckdb/DuckDBStruct");
	J_DuckStruct_init = get_method_id(env, J_DuckStruct, "<init>",
	                                  "([Ljava/lang/String;[Lorg/duckdb/DuckDBVector;ILjava/lang/String;)V");

	J_Struct = make_class_ref(env, "java/sql/Struct");
	J_Struct_getSQLTypeName = get_method_id(env, J_Struct, "getSQLTypeName", "()Ljava/lang/String;");
	J_Struct_getAttributes = get_method_id(env, J_Struct, "getAttributes", "()[Ljava/lang/Object;");

	J_Array = make_class_ref(env, "java/sql/Array");
	J_Array_getArray = get_method_id(env, J_Array, "getArray", "()Ljava/lang/Object;");
	J_Array_getBaseTypeName = get_method_id(env, J_Array, "getBaseTypeName", "()Ljava/lang/String;");

	J_Object = make_class_ref(env, "java/lang/Object");
	J_Object_toString = get_method_id(env, J_Object, "toString", "()Ljava/lang/String;");

	J_Entry = make_class_ref(env, "java/util/Map$Entry");
	J_Entry_getKey = get_method_id(env, J_Entry, "getKey", "()Ljava/lang/Object;");
	J_Entry_getValue = get_method_id(env, J_Entry, "getValue", "()Ljava/lang/Object;");

	J_Bool_booleanValue = get_method_id(env, J_Bool, "booleanValue", "()Z");
	J_Byte_byteValue = get_method_id(env, J_Byte, "byteValue", "()B");
	J_Short_shortValue = get_method_id(env, J_Short, "shortValue", "()S");
	J_Int_intValue = get_method_id(env, J_Int, "intValue", "()I");
	J_Long_longValue = get_method_id(env, J_Long, "longValue", "()J");
	J_Float_floatValue = get_method_id(env, J_Float, "floatValue", "()F");
	J_Double_doubleValue = get_method_id(env, J_Double, "doubleValue", "()D");
	J_Timestamp_getMicrosEpoch = get_method_id(env, J_Timestamp, "getMicrosEpoch", "()J");
	J_TimestampTZ_getMicrosEpoch = get_method_id(env, J_TimestampTZ, "getMicrosEpoch", "()J");
	J_BigDecimal_precision = get_method_id(env, J_BigDecimal, "precision", "()I");
	J_BigDecimal_scale = get_method_id(env, J_BigDecimal, "scale", "()I");
	J_BigDecimal_scaleByPowTen = get_method_id(env, J_BigDecimal, "scaleByPowerOfTen", "(I)Ljava/math/BigDecimal;");
	J_BigDecimal_toPlainString = get_method_id(env, J_BigDecimal, "toPlainString", "()Ljava/lang/String;");
	J_BigDecimal_longValue = get_method_id(env, J_BigDecimal, "longValue", "()J");
	J_HugeInt_lower = get_field_id(env, J_HugeInt, "lower", "J");
	J_HugeInt_upper = get_field_id(env, J_HugeInt, "upper", "J");

	J_DuckResultSetMeta = make_class_ref(env, "org/duckdb/DuckDBResultSetMetaData");
	J_DuckResultSetMeta_init = env->GetMethodID(J_DuckResultSetMeta, "<init>",
	                                            "(II[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;Ljava/"
	                                            "lang/String;[Ljava/lang/String;[Ljava/lang/String;)V");

	J_DuckVector = make_class_ref(env, "org/duckdb/DuckDBVector");

	J_String_getBytes = get_method_id(env, J_String, "getBytes", "(Ljava/nio/charset/Charset;)[B");

	J_DuckVector_init = get_method_id(env, J_DuckVector, "<init>", "(Ljava/lang/String;I[Z)V");
	J_DuckVector_constlen = get_field_id(env, J_DuckVector, "constlen_data", "Ljava/nio/ByteBuffer;");
	J_DuckVector_varlen = get_field_id(env, J_DuckVector, "varlen_data", "[Ljava/lang/Object;");

	J_ByteBuffer = make_class_ref(env, "java/nio/ByteBuffer");

	J_ProfilerPrintFormat = make_class_ref(env, "org/duckdb/ProfilerPrintFormat");
	J_ProfilerPrintFormat_QUERY_TREE =
	    make_static_object_field_ref(env, J_ProfilerPrintFormat, "QUERY_TREE", "Lorg/duckdb/ProfilerPrintFormat;");
	J_ProfilerPrintFormat_JSON =
	    make_static_object_field_ref(env, J_ProfilerPrintFormat, "JSON", "Lorg/duckdb/ProfilerPrintFormat;");
	J_ProfilerPrintFormat_QUERY_TREE_OPTIMIZER = make_static_object_field_ref(
	    env, J_ProfilerPrintFormat, "QUERY_TREE_OPTIMIZER", "Lorg/duckdb/ProfilerPrintFormat;");
	J_ProfilerPrintFormat_NO_OUTPUT =
	    make_static_object_field_ref(env, J_ProfilerPrintFormat, "NO_OUTPUT", "Lorg/duckdb/ProfilerPrintFormat;");
	J_ProfilerPrintFormat_HTML =
	    make_static_object_field_ref(env, J_ProfilerPrintFormat, "HTML", "Lorg/duckdb/ProfilerPrintFormat;");
	J_ProfilerPrintFormat_GRAPHVIZ =
	    make_static_object_field_ref(env, J_ProfilerPrintFormat, "GRAPHVIZ", "Lorg/duckdb/ProfilerPrintFormat;");

	J_QueryProgress = make_class_ref(env, "org/duckdb/QueryProgress");
	J_QueryProgress_init = get_method_id(env, J_QueryProgress, "<init>", "(DJJ)V");
}

void delete_global_refs(JNIEnv *env) noexcept {
	try {
		for (auto &rf : global_refs) {
			env->DeleteGlobalRef(rf);
		}
	} catch (const std::exception e) {
		std::cout << "ERROR: delete_global_refs: " << e.what() << std::endl;
	}
}

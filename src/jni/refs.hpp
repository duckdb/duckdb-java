#pragma once

#include <jni.h>

extern jclass J_Charset;
extern jmethodID J_Charset_decode;
extern jclass J_StandardCharsets;
extern jobject J_Charset_UTF8;

extern jclass J_CharBuffer;
extern jmethodID J_CharBuffer_toString;

extern jmethodID J_String_getBytes;

extern jclass J_Throwable;
extern jmethodID J_Throwable_getMessage;
extern jclass J_SQLException;

extern jclass J_Bool;
extern jclass J_Byte;
extern jclass J_Short;
extern jclass J_Int;
extern jclass J_Long;
extern jclass J_Float;
extern jclass J_Double;
extern jclass J_String;
extern jclass J_Timestamp;
extern jmethodID J_Timestamp_valueOf;
extern jclass J_TimestampTZ;
extern jclass J_Decimal;
extern jclass J_ByteArray;

extern jmethodID J_Bool_booleanValue;
extern jmethodID J_Byte_byteValue;
extern jmethodID J_Short_shortValue;
extern jmethodID J_Int_intValue;
extern jmethodID J_Long_longValue;
extern jmethodID J_Float_floatValue;
extern jmethodID J_Double_doubleValue;
extern jmethodID J_Timestamp_getMicrosEpoch;
extern jmethodID J_TimestampTZ_getMicrosEpoch;
extern jmethodID J_Decimal_precision;
extern jmethodID J_Decimal_scale;
extern jmethodID J_Decimal_scaleByPowTen;
extern jmethodID J_Decimal_toPlainString;
extern jmethodID J_Decimal_longValue;

extern jclass J_DuckResultSetMeta;
extern jmethodID J_DuckResultSetMeta_init;

extern jclass J_DuckVector;
extern jmethodID J_DuckVector_init;
extern jfieldID J_DuckVector_constlen;
extern jfieldID J_DuckVector_varlen;

extern jclass J_DuckArray;
extern jmethodID J_DuckArray_init;

extern jclass J_Struct;
extern jmethodID J_Struct_getSQLTypeName;
extern jmethodID J_Struct_getAttributes;

extern jclass J_Array;
extern jmethodID J_Array_getBaseTypeName;
extern jmethodID J_Array_getArray;

extern jclass J_DuckStruct;
extern jmethodID J_DuckStruct_init;

extern jclass J_ByteBuffer;

extern jclass J_DuckMap;
extern jmethodID J_DuckMap_getSQLTypeName;

extern jclass J_List;
extern jmethodID J_List_iterator;
extern jclass J_Map;
extern jmethodID J_Map_entrySet;
extern jclass J_Set;
extern jmethodID J_Set_iterator;
extern jclass J_Iterator;
extern jmethodID J_Iterator_hasNext;
extern jmethodID J_Iterator_next;
extern jclass J_Entry;
extern jmethodID J_Entry_getKey;
extern jmethodID J_Entry_getValue;

extern jclass J_UUID;
extern jmethodID J_UUID_getMostSignificantBits;
extern jmethodID J_UUID_getLeastSignificantBits;

extern jclass J_DuckDBDate;
extern jmethodID J_DuckDBDate_getDaysSinceEpoch;

extern jclass J_Object;
extern jmethodID J_Object_toString;

extern jclass J_DuckDBTime;

void create_refs(JNIEnv *env);

void delete_global_refs(JNIEnv *env) noexcept;

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
extern jclass J_SQLTimeoutException;

extern jclass J_Bool;
extern jclass J_Byte;
extern jclass J_Short;
extern jclass J_Int;
extern jclass J_Long;
extern jmethodID J_Bool_init;
extern jmethodID J_Byte_init;
extern jmethodID J_Short_init;
extern jmethodID J_Int_init;
extern jmethodID J_Long_init;
extern jclass J_Float;
extern jclass J_Double;
extern jmethodID J_Float_init;
extern jmethodID J_Double_init;
extern jclass J_String;
extern jclass J_Timestamp;
extern jmethodID J_Timestamp_valueOf;
extern jclass J_TimestampTZ;
extern jclass J_BigDecimal;
extern jclass J_HugeInt;
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
extern jmethodID J_BigDecimal_precision;
extern jmethodID J_BigDecimal_scale;
extern jmethodID J_BigDecimal_scaleByPowTen;
extern jmethodID J_BigDecimal_toPlainString;
extern jmethodID J_BigDecimal_longValue;
extern jmethodID J_BigDecimal_initString;
extern jfieldID J_HugeInt_lower;
extern jfieldID J_HugeInt_upper;

extern jclass J_DuckResultSetMeta;
extern jmethodID J_DuckResultSetMeta_init;

extern jclass J_DuckVector;
extern jmethodID J_DuckVector_init;
extern jmethodID J_DuckVector_retainConstlenData;
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
extern jmethodID J_ByteBuffer_order;
extern jclass J_ByteOrder;
extern jobject J_ByteOrder_NATIVE;

extern jclass J_DuckMap;
extern jmethodID J_DuckMap_getSQLTypeName;

extern jclass J_List;
extern jmethodID J_List_iterator;
extern jclass J_ArrayList;
extern jmethodID J_ArrayList_init;
extern jmethodID J_ArrayList_add;
extern jclass J_Map;
extern jmethodID J_Map_entrySet;
extern jclass J_LinkedHashMap;
extern jmethodID J_LinkedHashMap_init;
extern jmethodID J_LinkedHashMap_put;
extern jclass J_Set;
extern jmethodID J_Set_iterator;
extern jclass J_Iterator;
extern jmethodID J_Iterator_hasNext;
extern jmethodID J_Iterator_next;
extern jclass J_Entry;
extern jmethodID J_Entry_getKey;
extern jmethodID J_Entry_getValue;

extern jclass J_UUID;
extern jmethodID J_UUID_init;
extern jmethodID J_UUID_getMostSignificantBits;
extern jmethodID J_UUID_getLeastSignificantBits;

extern jclass J_LocalDate;
extern jmethodID J_LocalDate_ofEpochDay;
extern jclass J_LocalTime;
extern jmethodID J_LocalTime_ofNanoOfDay;
extern jclass J_LocalDateTime;
extern jmethodID J_LocalDateTime_ofEpochSecond;
extern jmethodID J_LocalDateTime_atOffset;
extern jclass J_OffsetTime;
extern jmethodID J_OffsetTime_of;
extern jclass J_ZoneOffset;
extern jobject J_ZoneOffset_UTC;
extern jmethodID J_ZoneOffset_ofTotalSeconds;

extern jclass J_DuckDBDate;
extern jmethodID J_DuckDBDate_getDaysSinceEpoch;

extern jclass J_Object;
extern jmethodID J_Object_toString;
extern jclass J_StringArray;
extern jclass J_Enum;
extern jmethodID J_Enum_name;

extern jclass J_DuckDBTime;

extern jclass J_ProfilerPrintFormat;
extern jobject J_ProfilerPrintFormat_QUERY_TREE;
extern jobject J_ProfilerPrintFormat_JSON;
extern jobject J_ProfilerPrintFormat_QUERY_TREE_OPTIMIZER;
extern jobject J_ProfilerPrintFormat_NO_OUTPUT;
extern jobject J_ProfilerPrintFormat_HTML;
extern jobject J_ProfilerPrintFormat_GRAPHVIZ;

extern jclass J_QueryProgress;
extern jmethodID J_QueryProgress_init;

extern jclass J_ScalarUdf;
extern jmethodID J_ScalarUdf_apply;
extern jclass J_UdfReader;
extern jclass J_UdfNativeReader;
extern jmethodID J_UdfNativeReader_init;
extern jclass J_UdfScalarWriter;
extern jmethodID J_UdfScalarWriter_init;
extern jclass J_TableFunction;
extern jmethodID J_TableFunction_bind;
extern jmethodID J_TableFunction_init;
extern jmethodID J_TableFunction_produce;
extern jclass J_TableBindResult;
extern jmethodID J_TableBindResult_getColumnNames;
extern jmethodID J_TableBindResult_getColumnTypes;
extern jmethodID J_TableBindResult_getColumnLogicalTypes;
extern jclass J_TableState;
extern jclass J_TableInitContext;
extern jmethodID J_TableInitContext_init;
extern jclass J_UdfOutputAppender;
extern jmethodID J_UdfOutputAppender_init;
extern jmethodID J_UdfOutputAppender_close;
extern jclass J_DuckDBColumnType;
extern jclass J_UdfLogicalType;
extern jmethodID J_UdfLogicalType_getType;
extern jmethodID J_UdfLogicalType_getChildType;
extern jmethodID J_UdfLogicalType_getArraySize;
extern jmethodID J_UdfLogicalType_getKeyType;
extern jmethodID J_UdfLogicalType_getValueType;
extern jmethodID J_UdfLogicalType_getFieldNames;
extern jmethodID J_UdfLogicalType_getFieldTypes;
extern jmethodID J_UdfLogicalType_getEnumValues;
extern jmethodID J_UdfLogicalType_getDecimalWidth;
extern jmethodID J_UdfLogicalType_getDecimalScale;

void create_refs(JNIEnv *env);

void delete_global_refs(JNIEnv *env) noexcept;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.common.constant;

public class TsFileConstant {

  public static final String TSFILE_SUFFIX = ".tsfile";
  public static final String TSFILE_HOME = "TSFILE_HOME";
  public static final String TSFILE_CONF = "TSFILE_CONF";
  public static final String PATH_ROOT = "root";
  public static final String TMP_SUFFIX = "tmp";
  public static final String PATH_SEPARATOR = ".";
  public static final char PATH_SEPARATOR_CHAR = '.';
  public static final String PATH_SEPARATER_NO_REGEX = "\\.";
  public static final char DOUBLE_QUOTE = '"';

  public static final byte TIME_COLUMN_MASK = (byte) 0x80;
  public static final byte VALUE_COLUMN_MASK = (byte) 0x40;

  // list all the key operations that need time measuring

  public static boolean decomposeMeasureTime = true; // false to measure the total time only
  public static boolean DataSetWithoutTimeGenerator_total = false; // true to measure init, hasNext, next, instead of lower-level api such as readMemChunk

  // [index part]
  public static String index_read_deserialize_MagicString_FileMetadataSize =
      "1_index_read_deserialize_MagicString_FileMetadataSize";
  // these three components constitute the so-called TsFileMetadata
  public static String index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter =
      "2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter";
  // from the IndexRootNode to the TimeseriesMetadata
  public static String index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp =
      "3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp";
  public static String index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet =
      "3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet";

  // [data part]
  public static String data_read_deserialize_ChunkHeader = "4_data_read_deserialize_ChunkHeader";
  public static String data_read_ChunkData = "5_data_read_ChunkData";

  public static String data_deserialize_PageHeader = "6_data_deserialize_PageHeader";

  // public static String data_decompress_PageData_split_timeBuffer_valueBuffer = "7_data_decompress_PageData_split_timeBuffer_valueBuffer";
  public static String data_ByteBuffer_to_ByteArray = "7_1_data_ByteBuffer_to_ByteArray";
  public static String data_decompress_PageData = "7_2_data_decompress_PageData";
  public static String data_ByteArray_to_ByteBuffer = "7_3_data_ByteArray_to_ByteBuffer";
  public static String data_split_time_value_Buffer = "7_4_data_split_time_value_Buffer";

  public static String data_decode_time_value_Buffer = "8_data_decode_time_value_Buffer";

  public static String other_cpu_time = "other_cpu_time";
  public static String total_time = "total_time";

  private TsFileConstant() {
  }
}

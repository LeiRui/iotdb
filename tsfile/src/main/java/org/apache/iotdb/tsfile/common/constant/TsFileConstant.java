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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  public static boolean D_2_decompose_each_step =
      true; // false to measure 8_data_decode_time_value_Buffer as a whole, true to measure each
  // step in 8_data_decode_time_value_Buffer

  // [index part]
  public static String index_read_deserialize_MagicString_FileMetadataSize =
      "(A)1_index_read_deserialize_MagicString_FileMetadataSize";
  // these three components constitute the so-called TsFileMetadata
  public static String index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter =
      "(A)2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter";
  // from the IndexRootNode to the TimeseriesMetadata
  public static String
      index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp =
          "(A)3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp";
  public static String
      index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet =
          "(A)3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet";

  // [data part]
  public static String data_read_deserialize_ChunkHeader = "(B)4_data_read_deserialize_ChunkHeader";
  public static String data_read_ChunkData = "(B)5_data_read_ChunkData";

  public static String data_deserialize_PageHeader = "(C)6_data_deserialize_PageHeader";

  public static String data_ByteBuffer_to_ByteArray = "(D-1)7_1_data_ByteBuffer_to_ByteArray";
  public static String data_decompress_PageData = "(D-1)7_2_data_decompress_PageData";
  public static String data_ByteArray_to_ByteBuffer = "(D-1)7_3_data_ByteArray_to_ByteBuffer";
  public static String data_split_time_value_Buffer = "(D-1)7_4_data_split_time_value_Buffer";

  public static String data_decode_time_value_Buffer = "(D-2)8_data_decode_time_value_Buffer";

  public static String D_2_createBatchData = "(D-2)8_1_createBatchData";
  public static String D_2_timeDecoder_hasNext = "(D-2)8_2_timeDecoder_hasNext";
  public static String D_2_timeDecoder_readLong = "(D-2)8_3_timeDecoder_readLong";
  public static String D_2_valueDecoder_read = "(D-2)8_4_valueDecoder_read";
  public static String D_2_checkValueSatisfyOrNot = "(D-2)8_5_checkValueSatisfyOrNot";
  public static String D_2_putIntoBatchData = "(D-2)8_6_putIntoBatchData";

  public static String total_time = "total_time";

  private TsFileConstant() {}

  /**
   * @param elapsedTime in nanoseconds
   * @param append true to append in list, false to accumulate the first element in list
   * @param print true to print information
   */
  public static void record(
      Map<String, List<Long>> elapsedTimeInNanoSec,
      String key,
      long elapsedTime,
      boolean append,
      boolean print) {
    if (append) { // true to append in list
      if (!elapsedTimeInNanoSec.containsKey(key)) {
        elapsedTimeInNanoSec.put(key, new ArrayList<>());
      }
      elapsedTimeInNanoSec.get(key).add(elapsedTime);
    } else { // false to accumulate the first element in list
      if (!elapsedTimeInNanoSec.containsKey(key)) {
        elapsedTimeInNanoSec.put(key, new ArrayList<>());
        elapsedTimeInNanoSec.get(key).add(0L);
      }
      elapsedTimeInNanoSec.get(key).set(0, elapsedTimeInNanoSec.get(key).get(0) + elapsedTime);
    }
    if (print) {
      System.out.println("done:" + key + "," + elapsedTime / 1000.0 + "us");
    }
  }
}

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
package org.apache.iotdb.db;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Yuyuan Kang
 */
public class RLMonitor {

  public static String index_read_deserialize_MagicString_FileMetadataSize =
      "(A)1_index_read_deserialize_MagicString_FileMetadataSize";
  public static String index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter =
      "(A)2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter";
  public static String
      index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp =
      "(A)3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp";
  public static String
      index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet =
      "(A)3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet";

  public static String data_read_deserialize_ChunkHeader = "(B)4_data_read_deserialize_ChunkHeader";
  public static String data_read_ChunkData = "(B)5_data_read_ChunkData";

  public static String data_deserialize_PageHeader = "(C)6_data_deserialize_PageHeader";

  public static String data_decompress_PageData = "(D-1)7_data_decompress_PageData";
  public static String D_1_data_ByteBuffer_to_ByteArray = "(D-1)7_1_data_ByteBuffer_to_ByteArray";
  public static String D_1_data_decompress_PageDataByteArray =
      "(D-1)7_2_data_decompress_PageDataByteArray";
  public static String D_1_data_ByteArray_to_ByteBuffer = "(D-1)7_3_data_ByteArray_to_ByteBuffer";
  public static String D_1_data_split_time_value_Buffer = "(D-1)7_4_data_split_time_value_Buffer";

  public static String data_decode_time_value_Buffer = "(D-2)8_data_decode_PageData";
  public static String D_2_createBatchData = "(D-2)8_1_createBatchData";
  public static String D_2_timeDecoder_hasNext = "(D-2)8_2_timeDecoder_hasNext";
  public static String D_2_timeDecoder_readLong = "(D-2)8_3_timeDecoder_readLong";
  public static String D_2_valueDecoder_read = "(D-2)8_4_valueDecoder_read";
  public static String D_2_checkValueSatisfyOrNot = "(D-2)8_5_checkValueSatisfyOrNot";
  public static String D_2_putIntoBatchData = "(D-2)8_6_putIntoBatchData";

  public static String E_timeDecoder_loadIntBatch = "(E)LongDeltaDecoder_loadIntBatch";

  public static String total_time = "total_time";

  private static String sql; // 假设每次只执行一次查询，只有一个query sql

  public static Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();

  public static DecimalFormat df = new DecimalFormat("#.00");

  public static void setSQL(String v) {
    sql = v;
    elapsedTimeInNanoSec.clear();
  }

  /**
   * @param elapsedTime in nanoseconds
   * @param append      true to append in list, false to accumulate the first element in list
   * @param print       true to print information
   */
  public static void record(
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

  public static void printEach(Map<String, List<Long>> elapsedTimeInNanoSec, PrintWriter pw) {
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      System.out.print(key + ":");
      if (pw != null) {
        pw.print(key);
        pw.print("(us),");
      }
      double sum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
        stringBuilder.append(t / 1000.0).append(", ");
      }
      System.out.print("[" + df.format(sum) + "us(SUM)," + elapsedTimes.size() + "(CNT)]:");
      System.out.println();
      System.out.println(stringBuilder.toString());
      if (pw != null) {
        pw.print(sum);
        pw.println();
      }
    }
  }

  public static String finish() {
    StringBuilder total = new StringBuilder();
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      StringBuilder res = new StringBuilder();
      res.append("[RLMonitor]");
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      res.append(key);
      res.append(":");
      double sum = 0;
      StringBuilder tmp = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
        tmp.append(df.format(t / 1000.0)).append(", ");
      }
      res.append("[").append(df.format(sum)).append("us(SUM),").append(elapsedTimes.size())
          .append("(CNT)]:");
      res.append(tmp.toString());
      total.append(res.toString());
      total.append(System.getProperty("line.separator"));
    }

    // clear
    sql = null;
    elapsedTimeInNanoSec.clear();

    return total.toString();
  }
}

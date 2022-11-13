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
package org.apache.iotdb.commons.service.metric.enums;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;

public enum Operation {
  EXECUTE_JDBC_BATCH("EXECUTE_JDBC_BATCH"),
  EXECUTE_ONE_SQL_IN_BATCH("EXECUTE_ONE_SQL_IN_BATCH"),
  EXECUTE_ROWS_PLAN_IN_BATCH("EXECUTE_ROWS_PLAN_IN_BATCH"),
  EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH("EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH"),
  EXECUTE_RPC_BATCH_INSERT("EXECUTE_RPC_BATCH_INSERT"),
  EXECUTE_QUERY("EXECUTE_QUERY"),
  EXECUTE_SELECT_INTO("EXECUTE_SELECT_INTO"),

  // DCP short for decompose
  DCP_A_GET_CHUNK_METADATAS("DCP_A_GET_CHUNK_METADATAS"),
  DCP_B_READ_MEM_CHUNK("DCP_B_READ_MEM_CHUNK"),
  DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA(
      "DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA"),
  DCP_D_DECODE_PAGEDATA("DCP_D_DECODE_PAGEDATA"),
  DCP_ITSELF("DCP_ITSELF"), // when there is no further decomposition
  DCP_SeriesScanOperator_hasNext("DCP_SeriesScanOperator_hasNext"),
  DCP_Server_Query_Execute("DCP_Server_Query_Execute"),
  DCP_Server_Query_Fetch("DCP_Server_Query_Fetch"),
  DCP_LongDeltaDecoder_loadIntBatch("DCP_LongDeltaDecoder_loadIntBatch");

  public String getName() {
    return name;
  }

  String name;

  Operation(String name) {
    this.name = name;
  }

  public static void addOperationLatency_ns(
      Operation metricName, Operation tagName, long startTime) {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnablePerformanceStat()) {
      //      System.out.println("addOperationLatency_ns~~~" + metricName + " " + tagName); // TODO
      // tmp
      MetricService.getInstance()
          .timer(
              System.nanoTime() - startTime,
              TimeUnit.NANOSECONDS,
              metricName.getName(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              tagName.getName());
    }
  }

  public static void addOperationLatency_loadIntBatch(long elapsedTime_ns, long cnt) {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnablePerformanceStat()) {
      MetricService.getInstance()
          .timer(
              elapsedTime_ns,
              TimeUnit.NANOSECONDS,
              DCP_LongDeltaDecoder_loadIntBatch.getName() + "_timer",
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              DCP_ITSELF.getName());
      MetricService.getInstance()
          .count(
              cnt,
              DCP_LongDeltaDecoder_loadIntBatch.getName() + "_count",
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              DCP_ITSELF.getName());
    }
  }
}

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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class MySessionExample {

  private static Session sessionEnableRedirect;
  private static final String deviceName = "root.sg1.d1";

  private static final String sensorName = "s1";

  /**
   * Before starting IoTDB, set the following config parameters:
   *
   * <p>// IoTDBConfig // 分区问题 enablePartition = false; // 只要一个time partition
   * timePartitionIntervalForRouting = 315360000000L; //
   * 10年。设大避免用户输入的一个tablet被切碎从而无法精确控制flush的chunk点数
   *
   * <p>// 控制chunk点数 enableSeqSpaceCompaction = false; enableUnseqSpaceCompaction = false;
   * enableCrossSpaceCompaction = false;
   *
   * <p>enableTimedFlushSeqMemtable = false; enableTimedFlushUnseqMemtable = false; enableMemControl
   * = false; avgSeriesPointNumberThreshold = Integer.MAX_VALUE;
   *
   * <p>// TsFileConfig // 控制page点数 maxNumberOfPointsInPage = desired_pagePointNum; pageSizeInByte =
   * maxNumberOfPointsInPage * 2 * 8 * 2; // timeEncoding = "TS_2DIFF"; // 这个不必动了
   *
   * <p>// MetricConfig // metric参数 enableMetric = true; enablePerformanceStat = true;
   * metricReporterList = Arrays.asList(ReporterType.JMX, ReporterType.PROMETHEUS,
   * ReporterType.IOTDB); pushPeriodInSecond = 15;
   */
  public static void main(String[] args) throws Exception {
    sessionEnableRedirect = new Session("127.0.0.1", 6667, "root", "root");
    sessionEnableRedirect.setEnableQueryRedirection(true);
    sessionEnableRedirect.open(false);

    if (args.length == 0) {
      throw new IOException("The first argument should be 'w' or 'r'!");
    }

    String mode = args[0].toLowerCase();
    if (mode.equals("w")) {
      try {
        int desiredChunkPointNum = Integer.parseInt(args[1]);
        String csvData = args[2];
        TSDataType valueDataType = TSDataType.valueOf(args[3]);
        TSEncoding valueEncoding = TSEncoding.valueOf(args[4]);
        CompressionType compressionType = CompressionType.valueOf(args[5]);
        writeRealData(desiredChunkPointNum, csvData, valueDataType, valueEncoding, compressionType);
      } catch (Exception e) {
        System.out.println(
            "Correct usage: w desiredChunkPointNum csvData valueDataType valueEncoding compressionType");
        System.out.println("Example: w 100000000 ZT11529.csv DOUBLE GORILLA SNAPPY");
        // w 100000000 "D:\LabSync\iotdb\我的Gitbook基地\RUI Lei gitbook\ZC data\ZT11529.csv" DOUBLE
        // GORILLA SNAPPY
        throw new IOException(e);
      }
    } else if (mode.equals("r")) {
      try {
        int fetchSize = Integer.parseInt(args[1]); // large enough to make all in one chunk
        String queryMetricResultCsvPath = args[2];
        String ip = args[3];
        sessionEnableRedirect.setFetchSize(fetchSize);
        query4Redirect(queryMetricResultCsvPath, ip);
      } catch (Exception e) {
        System.out.println("Correct usage: r fetchSize queryMetricResultCsvPath IP");
        System.out.println("Example: r 100000000 dcp.csv 0.0.0.0");
        throw new IOException(e);
      }
    } else {
      throw new IOException("The first argument should be 'w' or 'r'!");
    }

    sessionEnableRedirect.close();
  }

  private static void writeRealData(
      int desiredChunkPointNum,
      String csvData,
      TSDataType valueDataType,
      TSEncoding valueEncoding,
      CompressionType compressionType)
      throws Exception {
    MeasurementSchema measurementSchema =
        new MeasurementSchema(sensorName, valueDataType, valueEncoding, compressionType);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(measurementSchema);
    Tablet tablet = new Tablet(deviceName, schemaList);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    int currentChunkPointNum = 0;
    int chunksWritten = 0;
    long totalPointNum = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(csvData))) {
      br.readLine(); // skip header
      for (String line; (line = br.readLine()) != null; ) {
        totalPointNum++;
        currentChunkPointNum++;
        String[] tv = line.split(",");
        long time = Long.parseLong(tv[0]); // get timestamp from real data
        int row = tablet.rowSize++;
        timestamps[row] = time;
        switch (valueDataType) {
          case INT32:
            int int_value = Integer.parseInt(tv[1]); // get value from real data
            int[] int_sensor = (int[]) values[0];
            int_sensor[row] = int_value;
            break;
          case INT64:
            long long_value = Long.parseLong(tv[1]); // get value from real data
            long[] long_sensor = (long[]) values[0];
            long_sensor[row] = long_value;
            break;
          case FLOAT:
            float float_value = Float.parseFloat(tv[1]); // get value from real data
            float[] float_sensor = (float[]) values[0];
            float_sensor[row] = float_value;
            break;
          case DOUBLE:
            double double_value = Double.parseDouble(tv[1]); // get value from real data
            double[] double_sensor = (double[]) values[0];
            double_sensor[row] = double_value;
            break;
          default:
            throw new IOException("not supported data type!");
        }

        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          sessionEnableRedirect.insertTablet(tablet, true);
          tablet.reset();
        }

        if (currentChunkPointNum == desiredChunkPointNum) {
          // only here and the last tablet trigger the flush
          sessionEnableRedirect.insertTablet(tablet, true);
          tablet.reset();
          sessionEnableRedirect.executeNonQueryStatement("flush");
          currentChunkPointNum = 0;
          chunksWritten++;
        }
      }
      // flush the last Tablet
      if (tablet.rowSize != 0) {
        sessionEnableRedirect.insertTablet(tablet, true);
        tablet.reset();
        sessionEnableRedirect.executeNonQueryStatement("flush");
        chunksWritten++;
      }
    } finally {
      System.out.println("chunksWritten=" + chunksWritten);
      System.out.println("pointsWritten=" + totalPointNum);
    }
  }

  private static void query4Redirect(String queryMetricResultCsvPath, String ip)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException,
          FileNotFoundException {
    PrintWriter pw = new PrintWriter(queryMetricResultCsvPath);

    String query_data = String.format("select %s from %s", sensorName, deviceName);
    System.out.println("begin query: " + query_data);
    long cnt = 0;
    long startTime = System.nanoTime();
    //    try (SessionDataSet dataSet = sessionEnableRedirect.executeQueryStatement(query_data)) {
    //      while (dataSet.next() != null) {
    //        cnt++;
    //      }
    //    }
    try (SessionDataSet dataSet = sessionEnableRedirect.executeQueryStatement(query_data)) {
      DataIterator dataIterator = dataSet.iterator();
      while (dataIterator.next()) { // avoid constructRowRecordFromValueArray
        cnt++;
      }
    }
    long elapsedTimeNanoSec = System.nanoTime() - startTime;
    System.out.println("elapsedTime in nanosecond: " + elapsedTimeNanoSec);
    System.out.println("query finish! total point: " + cnt);
    pw.println("ClientElapsedTime_ns," + elapsedTimeNanoSec);

    System.out.println("Waiting some time for the metrics to be pushed into IoTDB...");
    Thread.sleep(30000);
    System.out.println("waiting finish.");
    String query_metric =
        "select sum(DCP_SeriesScanOperator_hasNext_count.`name=DCP_A_GET_CHUNK_METADATAS`.value) as A_cnt, sum(DCP_SeriesScanOperator_hasNext_count.`name=DCP_B_READ_MEM_CHUNK`.value) as B_cnt, sum(DCP_SeriesScanOperator_hasNext_count.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value) as C_cnt, sum(DCP_SeriesScanOperator_hasNext_count.`name=DCP_D_DECODE_PAGEDATA`.value) as D_cnt, sum(DCP_LongDeltaDecoder_loadIntBatch_count.`name=DCP_ITSELF`.value) as LongDeltaDecoder_loadIntBatch_cnt, sum(DCP_SeriesScanOperator_hasNext_count.`name=DCP_ITSELF`.value) as SeriesScanOperator_hasNext_cnt, sum(DCP_Server_Query_Execute_count.`name=DCP_ITSELF`.value) as Server_Query_Execute_cnt, sum(DCP_Server_Query_Fetch_count.`name=DCP_ITSELF`.value) as Server_Query_Fetch_cnt, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_A_GET_CHUNK_METADATAS`.value) as A_ns, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_B_READ_MEM_CHUNK`.value) as B_ns, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value) as C_ns, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value) as D_ns, sum(DCP_LongDeltaDecoder_loadIntBatch_timer_total.`name=DCP_ITSELF`.value) as LongDeltaDecoder_loadIntBatch_ns, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_A_GET_CHUNK_METADATAS`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_B_READ_MEM_CHUNK`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value) as sum_ABCD_ns, sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_ITSELF`.value) as SeriesScanOperator_hasNext_ns, sum(DCP_Server_Query_Execute_total.`name=DCP_ITSELF`.value) as Server_Query_Execute_ns, sum(DCP_Server_Query_Fetch_total.`name=DCP_ITSELF`.value) as Server_Query_Fetch_ns, 100*(sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_A_GET_CHUNK_METADATAS`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_B_READ_MEM_CHUNK`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value))/sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_ITSELF`.value) as `ABCD/SeriesScanOperator_hasNext(%)`, 100*(sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_A_GET_CHUNK_METADATAS`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_B_READ_MEM_CHUNK`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value))/sum(DCP_Server_Query_Execute_total.`name=DCP_ITSELF`.value) as `ABCD/Server_execute(%)`, 100*(sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_A_GET_CHUNK_METADATAS`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_B_READ_MEM_CHUNK`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA`.value)+sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value))/(sum(DCP_Server_Query_Execute_total.`name=DCP_ITSELF`.value)+sum(DCP_Server_Query_Fetch_total.`name=DCP_ITSELF`.value)) as `ABCD/Server_execute_fetch(%)`, 100*sum(DCP_LongDeltaDecoder_loadIntBatch_timer_total.`name=DCP_ITSELF`.value)/sum(DCP_SeriesScanOperator_hasNext_total.`name=DCP_D_DECODE_PAGEDATA`.value) as `loadIntBatch/D(%)`, 100*sum(DCP_LongDeltaDecoder_loadIntBatch_timer_total.`name=DCP_ITSELF`.value)/sum(DCP_Server_Query_Execute_total.`name=DCP_ITSELF`.value) as `loadIntBatch/Server_execute(%)`, 100*sum(DCP_LongDeltaDecoder_loadIntBatch_timer_total.`name=DCP_ITSELF`.value)/(sum(DCP_Server_Query_Execute_total.`name=DCP_ITSELF`.value)+sum(DCP_Server_Query_Fetch_total.`name=DCP_ITSELF`.value)) as `loadIntBatch/Server_execute_fetch(%)` from root.__system.metric.`"
            + ip
            + ":6667`";
    System.out.println("begin DCP metric query: " + query_metric);
    try (SessionDataSet dataSet = sessionEnableRedirect.executeQueryStatement(query_metric)) {
      outputResult(dataSet, pw);
    }
    pw.close();
  }

  private static void outputResult(SessionDataSet resultSet, PrintWriter pw)
      throws StatementExecutionException, IoTDBConnectionException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      List<String> columnNames = resultSet.getColumnNames();
      int columnCount = columnNames.size();
      for (int i = 0; i < columnCount; i++) { // exclude TIME
        System.out.print(String.format("%-35s", columnNames.get(i)));
      }
      System.out.println();
      while (resultSet.hasNext()) {
        List<Field> fields = resultSet.next().getFields();
        for (int i = 0; i < columnCount; i++) { // exclude TIME
          System.out.print(String.format("%-35s", fields.get(i)));
          pw.println(columnNames.get(i) + "," + fields.get(i));
        }
      }
      System.out.println();
      System.out.println("--------------------------\n");
    }
  }
}

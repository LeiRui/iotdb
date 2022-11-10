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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MySessionExample {

  private static Session sessionEnableRedirect;
  private static final String deviceName = "root.sg1.d1";
  private static final String sensorName = "s1";

  public static void main(String[] args) throws Exception {
    sessionEnableRedirect = new Session("127.0.0.1", 6667, "root", "root");
    sessionEnableRedirect.setEnableQueryRedirection(true);
    sessionEnableRedirect.open(false);
    sessionEnableRedirect.setFetchSize(10000);

    writeRealData();
    query4Redirect();

    sessionEnableRedirect.close();
  }

  private static void writeRealData() throws Exception {
    String csvData = "D:\\LabSync\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT11529.csv";
    TSDataType valueDataType = TSDataType.DOUBLE;
    TSEncoding valueEncoding = TSEncoding.RLE;
    CompressionType compressionType = CompressionType.SNAPPY;
    MeasurementSchema measurementSchema =
        new MeasurementSchema(sensorName, valueDataType, valueEncoding, compressionType);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(measurementSchema);
    Tablet tablet = new Tablet(deviceName, schemaList);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    int desiredChunkPointNum = 100000;
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
          //          sessionEnableRedirect.executeNonQueryStatement("flush");
          tablet.reset();
          //          chunksWritten++;
        }

        if (currentChunkPointNum == desiredChunkPointNum) {
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

  private static void query4Redirect()
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    String query_data = String.format("select %s from %s", sensorName, deviceName);
    try (SessionDataSet dataSet = sessionEnableRedirect.executeQueryStatement(query_data)) {
      System.out.println(dataSet.getColumnNames());
      long cnt = 0;
      while (dataSet.next() != null) {
        cnt++;
      }
      System.out.println(cnt);
    }

    // wait some time because the metric writes back to server by time interval pushPeriodInSecond
    System.out.println("Waiting some time for the metrics to be pushed into IoTDB...");
    Thread.sleep(30000);
    System.out.println("waiting finish.");

    String query_metric =
        "select sum(DCP_A_GET_CHUNK_METADATAS_histogram_count.`name=ITSELF`.value) as A_cnt, sum(DCP_B_READ_MEM_CHUNK_histogram_count.`name=ITSELF`.value) as B_cnt, sum(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_histogram_count.`name=ITSELF`.value) as C_cnt, sum(DCP_D_DECODE_PAGEDATA_histogram_count.`name=ITSELF`.value) as D_cnt, sum(DCP_A_GET_CHUNK_METADATAS_histogram_total.`name=ITSELF`.value) as A_ns, sum(DCP_B_READ_MEM_CHUNK_histogram_total.`name=ITSELF`.value) as B_ns, sum(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_histogram_total.`name=ITSELF`.value) as C_ns, sum(DCP_D_DECODE_PAGEDATA_histogram_total.`name=ITSELF`.value) as D_ns from root.__system.metric.`0.0.0.0:6667`";
    try (SessionDataSet dataSet = sessionEnableRedirect.executeQueryStatement(query_metric)) {
      outputResult(dataSet);
    }
  }

  private static void outputResult(SessionDataSet resultSet)
      throws StatementExecutionException, IoTDBConnectionException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      List<String> columnNames = resultSet.getColumnNames();
      int columnCount = columnNames.size();
      for (int i = 0; i < columnCount; i++) { // exclude TIME
        System.out.print(String.format("%-15s", columnNames.get(i)));
      }
      System.out.println();
      while (resultSet.hasNext()) {
        List<Field> fields = resultSet.next().getFields();
        for (int i = 0; i < columnCount; i++) { // exclude TIME
          System.out.print(String.format("%-15s", fields.get(i)));
        }
      }
      System.out.println();
      System.out.println("--------------------------\n");
    }
  }
}

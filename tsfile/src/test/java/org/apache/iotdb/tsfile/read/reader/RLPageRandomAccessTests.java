package org.apache.iotdb.tsfile.read.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.LongDeltaEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.junit.Assert;

public class RLPageRandomAccessTests {

  public static void main(String[] args) {
    try {
      PageWriter pageWriter = new PageWriter();
      pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
      pageWriter.setValueEncoder(new LongDeltaEncoder());
      pageWriter.initStatistics(TSDataType.INT64);
      String csv = "D:\\LabSync\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
      writeFromCsvData(csv, pageWriter, TSDataType.INT64); // TODO 从csv读写数据到page

      ByteBuffer page = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());

      PageReader pageReader = new PageReader(page, TSDataType.INT64,
          new DeltaBinaryDecoder.LongDeltaDecoder(), new DeltaBinaryDecoder.LongDeltaDecoder(),
          null);

      long start = System.nanoTime();
      long timestamp = pageReader.getFirstPointAfterTimestamp(1592308319601L);
      long elapsedTime = System.nanoTime() - start;

      Assert.assertEquals(1592308320113L, timestamp);
      System.out.println("elapsed time: " + elapsedTime/1000.0 + "us");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeFromCsvData(String csvData, PageWriter pageWriter, TSDataType dataType)
      throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(csvData))) {
      br.readLine(); // skip header
      for (String line; (line = br.readLine()) != null; ) {
        String[] tv = line.split(",");
        long time = Long.parseLong(tv[0]); // get timestamp from real data
        double value = Double.parseDouble(tv[1]);
        switch (dataType) {
          case INT32:
            pageWriter.write(time, (int) value);
            break;
          case INT64:
            pageWriter.write(time, (long) value);
            break;
          case FLOAT:
            pageWriter.write(time, (float) value);
            break;
          case DOUBLE:
            pageWriter.write(time, value);
            break;
          default:
            throw new IOException("not supported data type!");
        }
      }
    }
  }
}

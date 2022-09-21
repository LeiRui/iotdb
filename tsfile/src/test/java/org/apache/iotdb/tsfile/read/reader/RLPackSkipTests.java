package org.apache.iotdb.tsfile.read.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.LongDeltaEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.page.PageWriter;

public class RLPackSkipTests {

  public static void main(String[] args) {
    try {
      PageWriter pageWriter = new PageWriter();
      pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
      pageWriter.setValueEncoder(new LongDeltaEncoder());
      pageWriter.initStatistics(TSDataType.INT64);
      String csv = "G:\\实验室电脑同步\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
      long pointNum = writeFromCsvData(csv, pageWriter, TSDataType.INT64);

      ByteBuffer page = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());

      PageReader pageReader =
          new PageReader(
              page,
              TSDataType.INT64,
              new DeltaBinaryDecoder.LongDeltaDecoder(),
              new DeltaBinaryDecoder.LongDeltaDecoder(),
              null);
      LongDeltaDecoder timeDecoder = (LongDeltaDecoder) pageReader.timeDecoder;
      ByteBuffer timeBuffer = pageReader.timeBuffer;
      System.out.println("pack point size: " + DeltaBinaryEncoder.BLOCK_DEFAULT_SIZE);

      long query = 1593217962681L; // TODO modify
//      int cnt = 0;
//      while (timeDecoder.hasNextPackInterval(timeBuffer)) {
//        cnt++;
//        if (timeDecoder.intervalStart < query && timeDecoder.intervalStop >= query) {
//          System.out.println(timeDecoder.intervalStart + "," + timeDecoder.intervalStop);
//          break;
//        }
//
//      }
//      System.out.println(cnt);

      System.out.println(timeDecoder.checkContainsTimestamp(query, timeBuffer));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static long writeFromCsvData(String csvData, PageWriter pageWriter, TSDataType dataType)
      throws IOException {
    long cnt = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(csvData))) {
      br.readLine(); // skip header
      for (String line; (line = br.readLine()) != null; ) {
        cnt++;
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
    return cnt;
  }

}

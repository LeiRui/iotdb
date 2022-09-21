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
//      String csv = "G:\\实验室电脑同步\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
      String csv = "D:\\LabSync\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
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

      long query = 1605706904193L; // TODO modify

      int repeat = 10;

      long sumTime2 = 0;
      for (int i = 0; i < repeat; i++) { // repeat tests
        sumTime2 += testWithoutPackSkip(timeDecoder, timeBuffer, query);
      }

      long sumTime1 = 0;
      for (int i = 0; i < repeat; i++) { // repeat tests
        sumTime1 += testWithPackSkip(timeDecoder, timeBuffer, query);
      }

      System.out.println(
          "testWithPackSkip average elapsed time: " + sumTime1 * 1.0 / repeat / 1000.0 + "us");
      System.out.println(
          "testWithoutPackSkip average elapsed time: " + sumTime2 * 1.0 / repeat / 1000.0 + "us");
      System.out.println(timeBuffer.limit());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static long testWithPackSkip(LongDeltaDecoder timeDecoder, ByteBuffer timeBuffer,
      long query) {
    timeDecoder.reset();
    timeBuffer.position(0); // DO NOT USE buffer.clear as LIMIT CAN NOT BE CHANGED
    long start = System.nanoTime();
    long position = timeDecoder.checkContainsTimestamp(query, timeBuffer);
    long elapsedTime = System.nanoTime() - start;
    System.out.println("testWithPackSkip elapsed time: " + elapsedTime / 1000.0 + "us");
    if (position > 0) {
      System.out.println("found at position " + position);
    } else {
      System.out.println("not exists");
    }
    return elapsedTime;
  }

  public static long testWithoutPackSkip(LongDeltaDecoder timeDecoder, ByteBuffer timeBuffer,
      long query) throws IOException {
    timeDecoder.reset();
    timeBuffer.position(0); // DO NOT USE buffer.clear as LIMIT CAN NOT BE CHANGED
    long start = System.nanoTime();
    int cnt = 0;
    boolean found = false;
    while (timeDecoder.hasNext(timeBuffer)) {
      long time = timeDecoder.readLong(timeBuffer);
      cnt++;
      if (time == query) {
        found = true;
        break;
      }
      if (time > query) {
        break; // no need to continue as monotonically increasing timestamps
      }
    }
    long elapsedTime = System.nanoTime() - start;
    System.out.println("testWithoutPackSkip elapsed time: " + elapsedTime / 1000.0 + "us");
    if (found) {
      System.out.println("found at position " + cnt);
    } else {
      System.out.println("not exists");
    }
    return elapsedTime;
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

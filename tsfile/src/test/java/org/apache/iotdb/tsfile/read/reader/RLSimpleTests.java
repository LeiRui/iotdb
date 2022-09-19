package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.LongDeltaEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.page.PageWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RLSimpleTests {

  public static void main(String[] args) {
    try {
      PageWriter pageWriter = new PageWriter();
      pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
      pageWriter.setValueEncoder(new LongDeltaEncoder());
      pageWriter.initStatistics(TSDataType.INT64);
      String csv = "G:\\实验室电脑同步\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
      writeFromCsvData(csv, pageWriter, TSDataType.INT64);

      ByteBuffer page = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());

      PageReader pageReader =
          new PageReader(
              page,
              TSDataType.INT64,
              new DeltaBinaryDecoder.LongDeltaDecoder(),
              new DeltaBinaryDecoder.LongDeltaDecoder(),
              null);
      LongDeltaDecoder deltaDecoder = (LongDeltaDecoder) pageReader.timeDecoder;
      long query = 1605706904193L; // TODO modify

      System.out.println("pack point size: " + DeltaBinaryEncoder.BLOCK_DEFAULT_SIZE);
      long elapsedTime3 = test3(pageReader, deltaDecoder, query);

      long elapsedTime1 = test1(pageReader, deltaDecoder, query);

      long elapsedTime2 = test2(pageReader, deltaDecoder, query);

      // TODO：目前结论：即便block
      // size设置成对写和存储都不友好的两千万个点这么多，二分查询方法的耗时也没有很多提升，并且单纯地调换3个测试的执行顺序造成的耗时波动都比加速的时间多

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static long test1(PageReader pageReader, LongDeltaDecoder deltaDecoder, long query) {
    System.out.println("----------------------------------------------");
    System.out.println("test1: random access a pack");
    pageReader.timeBuffer.clear();
    long start = System.nanoTime();
    // load first pack in the buffer. this test only uses the first pack for simplicity
    deltaDecoder.loadIntBatch_RL(pageReader.timeBuffer);
    int first = 0; // 0 indicate firstValue here
    int last = deltaDecoder.packNum; // indicate the last point in this pack, starting from 0
    int mid = (first + last) / 2;
    long startValue = deltaDecoder.firstValue;
    int startPos = 0;
    while (first <= last) {
      startValue = deltaDecoder.getValue(startValue, startPos, mid);
      startPos = mid;
      if (startValue < query) {
        first = mid + 1;
      } else if (startValue > query) {
        last = mid - 1;
      } else { // startValue == query
        break;
      }
      mid = (first + last) / 2;
    }
    long elapsedTime = System.nanoTime() - start;
    if (first > last) {
      System.out.println("query key is not found!");
    } else {
      System.out.println("find query key: " + startValue + " at pos " + mid);
    }
    System.out.println("elapsed time: " + elapsedTime / 1000.0 + "us");
    return elapsedTime;
  }

  public static long test2(PageReader pageReader, LongDeltaDecoder deltaDecoder, long query) {
    System.out.println("----------------------------------------------");
    System.out.println("test2: sequential scan a pack and break early when possible");
    // load first pack in the buffer. this test only uses the first pack for simplicity
    pageReader.timeBuffer.clear();
    long start = System.nanoTime();
    long firstValue = deltaDecoder.loadIntBatch(pageReader.timeBuffer);
    boolean found = false;
    if (firstValue < query) {
      int i;
      for (i = deltaDecoder.nextReadIndex; i < deltaDecoder.readIntTotalCount; i++) {
        if (deltaDecoder.data[i] >= query) {
          break;
        }
      }
      if (i < deltaDecoder.readIntTotalCount && deltaDecoder.data[i] == query) {
        found = true;
      }
    } else if (firstValue > query) {
      found = false;
    } else { // firstValue == query
      found = true;
    }
    long elapsedTime = System.nanoTime() - start;
    System.out.println("elapsed time: " + elapsedTime / 1000.0 + "us");
    if (found) {
      System.out.println("find key!");
    } else {
      System.out.println("query key is not found!");
    }
    return elapsedTime;
  }

  public static long test3(PageReader pageReader, LongDeltaDecoder deltaDecoder, long query) {
    System.out.println("----------------------------------------------");
    System.out.println("test3: sequential scan a pack and NOT break early when possible");
    // load first pack in the buffer. this test only uses the first pack for simplicity
    pageReader.timeBuffer.clear();
    long start = System.nanoTime();
    long firstValue = deltaDecoder.loadIntBatch(pageReader.timeBuffer);
    boolean found = false;
    if (firstValue < query) {
      for (int i = deltaDecoder.nextReadIndex; i < deltaDecoder.readIntTotalCount; i++) {
        //        if (deltaDecoder.data[i] == query) {
        if (satisfy(deltaDecoder.data[i], query)) {
          found = true;
        }
      }
    } else if (firstValue > query) {
      found = false;
    } else { // firstValue == query
      found = true;
    }
    long elapsedTime = System.nanoTime() - start;
    System.out.println("elapsed time: " + elapsedTime / 1000.0 + "us");
    if (found) {
      System.out.println("find key!");
    } else {
      System.out.println("query key is not found!");
    }
    return elapsedTime;
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

  public static boolean satisfy(long time, long query) {
    return time == query;
  }
}

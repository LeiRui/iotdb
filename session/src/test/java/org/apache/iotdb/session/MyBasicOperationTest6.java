package org.apache.iotdb.session;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.junit.Assert;

public class MyBasicOperationTest6 {

  public static void main(String[] args) throws IOException {
    // op1: long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
    // op2: put bytes as a whole into long, i.e., BytesUtils.bytesToLong2(deltaBuf, packWidth * i,
    // packWidth);
    // op3: newest implementation on master branch by Haoyu Wang.
    // op4: compare bytes

    int repeat = 100000;
    int packNum = 128;
    int[] packWidthList = new int[]{
        1, 2, 5, 7, 8,
        9, 10, 11, 12, 15, 16,
        18, 20, 24,
        28, 30, 32,
        35, 40,
        44, 48,
        52, 56,
        58, 64};
    PrintWriter printWriter = new PrintWriter("MyBasicOperationTest6.csv");
    DecimalFormat df = new DecimalFormat("#.00");
    printWriter.println(
        "packWidth,op1-bytesToLong(us),op2-bytesToLong2-rl(us),op3-bytesToLong3-why(us),op4-compare-bytes(us),"
            + "op2/op1,op3/op1,op3/op2,"
            + "op4/op1,op4/op2,op4/op3");

    for (int packWidth : packWidthList) {
      byte[][] regularBytes = TsFileConstant.generateRegularByteArray(packWidth, 1, 0);

      int[] fallWithinMasks;
      if (packWidth < 8) {
        fallWithinMasks = TsFileConstant.generateFallWithinMasks(packWidth);
      } else {
        fallWithinMasks = null;
      }

      DescriptiveStatistics op1 = new DescriptiveStatistics();
      DescriptiveStatistics op2 = new DescriptiveStatistics();
      DescriptiveStatistics op3 = new DescriptiveStatistics();
      DescriptiveStatistics op4 = new DescriptiveStatistics();
      long sum = 0;

      for (int c = 0; c < repeat; c++) {
        // prepare test data
        long low = 0;
        long high = (long) Math.pow(2, packWidth) - 1;  // (endpoints included)
        byte[] buf = new byte[packNum * 8];
        for (int i = 0; i < packNum; i++) {
          long v = new RandomDataGenerator().nextLong(low, high); // (endpoints included)
          BytesUtils.longToBytes(v, buf, i * packWidth, packWidth);
        }

        // test op1
        long[] value1 = new long[packNum];
        long start = System.nanoTime();
        for (int i = 0; i < packNum; i++) {
          value1[i] = BytesUtils.bytesToLong(buf, packWidth * i, packWidth);
        }
        long elapsedTime = System.nanoTime() - start;
        op1.addValue(elapsedTime / 1000.0);

        // test op2
        long[] value2 = new long[packNum];
        start = System.nanoTime();
        for (int i = 0; i < packNum; i++) {
          value2[i] = BytesUtils.bytesToLong2(buf, packWidth * i, packWidth, fallWithinMasks);
        }
        elapsedTime = System.nanoTime() - start;
        op2.addValue(elapsedTime / 1000.0);

        // test op3
        long[] value3 = new long[packNum];
        start = System.nanoTime();
        for (int i = 0; i < packNum; i++) {
          value3[i] = bytesToLong3(buf, packWidth * i, packWidth);
        }
        elapsedTime = System.nanoTime() - start;
        op3.addValue(elapsedTime / 1000.0);

        // test op4
        start = System.nanoTime();
        for (int i = 0; i < packNum; i++) {
          // the starting relative position in the byte from high to low bits
          int pos = i * packWidth % 8;
          // the regular padded bytes to be compared
          byte[] byteArray = regularBytes[pos];
          // the start byte of the encoded new delta
          int posByteIdx = i * packWidth / 8;
          boolean equal = true;
          for (int k = 0; k < byteArray.length; k++, posByteIdx++) {
            byte regular = byteArray[k];
            byte data = buf[posByteIdx];
            if (regular != data) {
              equal = false;
              break;
            }
          }
          if (equal) {
            sum++;
          }
        }
        elapsedTime = System.nanoTime() - start;
        op4.addValue(elapsedTime / 1000.0);

        for (int i = 0; i < packNum; i++) {
          Assert.assertEquals(value1[i], value2[i]);
          Assert.assertEquals(value1[i], value3[i]);
        }
      }
      System.out.println(sum);

      printStat(op1, "op1-bytesToLong-original");
      printStat(op2, "op2-bytesToLong2-RL");
      printStat(op3, "op3-bytesToLong3-WHY");
      printStat(op4, "op4-compare-bytes");
      System.out.println("op2/op1=" + op2.getMean() / op1.getMean());
      System.out.println("op3/op1=" + op3.getMean() / op1.getMean());
      System.out.println("op3/op2=" + op3.getMean() / op2.getMean());
      System.out.println("op4/op1=" + op4.getMean() / op1.getMean());
      System.out.println("op4/op2=" + op4.getMean() / op2.getMean());
      System.out.println("op4/op3=" + op4.getMean() / op3.getMean());
      System.out.println("repeat=" + repeat);
      System.out.println("packNum=" + packNum);
      System.out.println("packWidth=" + packWidth);

      printWriter.print(packWidth);
      printWriter.print(",");
      printWriter.print(df.format(op1.getSum()));
      printWriter.print(",");
      printWriter.print(df.format(op2.getSum()));
      printWriter.print(",");
      printWriter.print(df.format(op3.getSum()));
      printWriter.print(",");
      printWriter.print(df.format(op4.getSum()));
      printWriter.print(",");
      printWriter.print(df.format(op2.getMean() / op1.getMean()));
      printWriter.print(",");
      printWriter.print(df.format(op3.getMean() / op1.getMean()));
      printWriter.print(",");
      printWriter.print(df.format(op3.getMean() / op2.getMean()));
      printWriter.print(",");
      printWriter.print(df.format(op4.getMean() / op1.getMean()));
      printWriter.print(",");
      printWriter.print(df.format(op4.getMean() / op2.getMean()));
      printWriter.print(",");
      printWriter.print(df.format(op4.getMean() / op3.getMean()));
      printWriter.println();
    }
    printWriter.close();
  }

  public static long bytesToLong3(byte[] result, int pos, int width) {
    long ret = 0;
    int cnt = pos & 0x07;
    int index = pos >> 3;
    while (width > 0) {
      int m = width + cnt >= 8 ? 8 - cnt : width;
      width -= m;
      ret = ret << m;
      byte y = (byte) (result[index] & (0xff >> cnt));
      y = (byte) ((y & 0xff) >>> (8 - cnt - m));
      ret = ret | (y & 0xff);
      cnt += m;
      if (cnt == 8) {
        cnt = 0;
        index++;
      }
    }
    return ret;
  }

  private static String printStat(DescriptiveStatistics statistics, String name) {
    DecimalFormat df = new DecimalFormat("#,###.00");
    double max = statistics.getMax();
    double min = statistics.getMin();
    double mean = statistics.getMean();
    double std = statistics.getStandardDeviation();
    double p25 = statistics.getPercentile(25);
    double p50 = statistics.getPercentile(50);
    double p75 = statistics.getPercentile(75);
    double p90 = statistics.getPercentile(90);
    double p95 = statistics.getPercentile(95);
    String res =
        name
            + "_stats"
            + ": "
            + "num="
            + statistics.getN()
            + ", "
            + "sum="
            + df.format(statistics.getSum())
            + "us,"
            + "mean="
            + df.format(mean)
            + ", "
            + "min="
            + df.format(min)
            + ", "
            + "max="
            + df.format(max)
            + ", "
            + "std="
            + df.format(std)
            + ", "
            + "p25="
            + df.format(p25)
            + ", "
            + "p50="
            + df.format(p50)
            + ", "
            + "p75="
            + df.format(p75)
            + ", "
            + "p90="
            + df.format(p90)
            + ", "
            + "p95="
            + df.format(p95);
    System.out.println(res);
    return res;
  }
}

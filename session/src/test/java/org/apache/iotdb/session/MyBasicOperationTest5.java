package org.apache.iotdb.session;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

public class MyBasicOperationTest5 {

  public static void main(String[] args) throws IOException {
    // op1: direct construct regularBytes when decoding
    // op2: readRegularBytes

    int repeat = 10000000;
    DescriptiveStatistics op1 = new DescriptiveStatistics();
    DescriptiveStatistics op2 = new DescriptiveStatistics();
    int writeWidth = 3;
    int minDeltaBase = 3;

    for (int k = 0; k < repeat; k++) {
      int regularTimeInterval =
          (int) ((Math.random() * (Math.pow(2, writeWidth) - 0)) + 0) + minDeltaBase;

      // test op1
      long start = System.nanoTime();
      byte[][] regularBytes =
          TsFileConstant.generateRegularByteArray(writeWidth, regularTimeInterval, minDeltaBase);
      long elapsedTime = System.nanoTime() - start;
      op1.addValue(elapsedTime / 1000.0);

      long sum1 = 0;
      for (int i = 0; i < 8; i++) {
        byte[] byteArray = regularBytes[i];
        int num = byteArray.length;
        for (int j = 0; j < num; j++) {
          sum1 += byteArray[j];
        }
      }

      // prepare data for op2
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (int i = 0; i < 8; i++) {
        byte[] byteArray = regularBytes[i];
        int num = byteArray.length;
        ReadWriteIOUtils.write(num, out);
        for (int j = 0; j < num; j++) {
          ReadWriteIOUtils.write(byteArray[j], out);
        }
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(out.toByteArray());

      // test op2
      start = System.nanoTime();
      byte[][] res1 = TsFileConstant.readRegularBytes(byteBuffer);
      elapsedTime = System.nanoTime() - start;
      op2.addValue(elapsedTime / 1000.0);

      long sum2 = 0;
      for (int i = 0; i < 8; i++) {
        byte[] byteArray = res1[i];
        int num = byteArray.length;
        for (int j = 0; j < num; j++) {
          sum2 += byteArray[j];
        }
      }
      Assert.assertEquals(sum1, sum2);
    }

    printStat(op1, "op1-direct construct regularBytes");
    printStat(op2, "op2-readRegularBytes");
    System.out.println("op1/op2=" + op1.getMean() / op2.getMean());
    System.out.println("op2/op1=" + op2.getMean() / op1.getMean());
    System.out.println("but when op1 uses cache to hit like 50%:");
    System.out.println("op2/(op1*0.5)=" + op2.getMean() / (op1.getMean() * 0.5));
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

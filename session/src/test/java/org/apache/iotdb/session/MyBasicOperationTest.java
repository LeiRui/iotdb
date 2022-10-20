package org.apache.iotdb.session;

import java.text.DecimalFormat;
import java.util.Random;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class MyBasicOperationTest {

  public static void main(String[] args) {
    // op1: long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
    // op2: compare bytes

    int repeat = 10000000;
    DescriptiveStatistics op1 = new DescriptiveStatistics();
    DescriptiveStatistics op2 = new DescriptiveStatistics();
    for (int k = 0; k < repeat; k++) {
      // prepare test data
      Random r = new Random();
      int low = 0; // inclusive
      int high = 256; // exclusive
      int packNum = 128;
      int packWidth = 8; // equal to one byte length
      byte[] buf = new byte[packNum];
      for (int i = 0; i < packNum; i++) {
        int v = r.nextInt(high - low) + low;
        BytesUtils.longToBytes(v, buf, i * packWidth, packWidth);
      }

      // test op1
      long sum = 0;
      long start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        sum += BytesUtils.bytesToLong(buf, packWidth * i, packWidth);
      }
      long elapsedTime = System.nanoTime() - start;
//      System.out.println(elapsedTime / 1000.0 + "us");
      System.out.println(sum);
      op1.addValue(elapsedTime / 1000.0);

      // test op2
      sum = 0;
      byte regular = 1;
      start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        if (regular != buf[i]) {
          sum++;
        }
      }
      elapsedTime = System.nanoTime() - start;
//      System.out.println(elapsedTime / 1000.0 + "us");
      System.out.println(sum);
      op2.addValue(elapsedTime / 1000.0);
    }
    printStat(op1, "op1-bytesToLong");
    printStat(op2, "op2-compareByte");
    System.out.println("op1/op2=" + op1.getMean() / op2.getMean());
    System.out.println("op2/op1=" + op2.getMean() / op1.getMean());
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

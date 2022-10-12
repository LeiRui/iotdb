package org.apache.iotdb.session;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;

import java.text.DecimalFormat;
import java.util.Random;

public class MyBasicOperationTest4 {

  public static void main(String[] args) {
    // op1: iterate a byte bit-by-bit to convert into two numbers
    // op2: use masks twice on a byte to convert into two number

    // prepare data
    int repeat = 100000000;
    Random r = new Random();
    byte[] buf = new byte[repeat];
    r.nextBytes(buf);
    int splitPos = 3; // the start pos of the second number, from high to low bits starting from 0
    int mask1 = 0b11100000;
    int mask2 = 0b00011111;
    DescriptiveStatistics op1 = new DescriptiveStatistics();
    DescriptiveStatistics op2 = new DescriptiveStatistics();
    int packWidth = 10;

    // test op1
    //    System.out.println("op1");
    //    long[] op1_v1 = new long[repeat];
    //    long[] op1_v2 = new long[repeat];
    //    long start = System.nanoTime();
    //    for (int i = 0; i < repeat; i++) {
    //      int mask = 0b10000000;
    //      long v1 = 0;
    //      long v2 = 0;
    //      for (int k = 0; k < 8; k++) {
    //        if (k < splitPos) {
    //          v1 = v1 | (buf[i] & mask);
    //          mask = mask >> 1;
    //        } else {
    //          v2 = v2 | (buf[i] & mask);
    //          mask = mask >> 1;
    //        }
    //      }
    //      op1_v1[i] = v1 >> 5;
    //      op1_v2[i] = v2 << (packWidth - (8 - splitPos));
    //    }
    //    long elapsedTime = System.nanoTime() - start;
    //    op1.addValue(elapsedTime / 1000.0);

    //    System.out.println("op1");
    //    long[] op1_v1 = new long[repeat];
    //    long[] op1_v2 = new long[repeat];
    //    long start = System.nanoTime();
    //    for (int i = 0; i < repeat; i++) {
    //      long v1 = 0;
    //      long v2 = 0;
    //      char[] v = Long.toBinaryString(buf[i] & 0xff).toCharArray();
    ////      System.out.println(v);
    //      for (int k = 8 - v.length; k < 8; k++) {
    //        if (k < splitPos) {
    //          v1 = v1 << 1;
    //          v1 = v1 + Character.getNumericValue(v[k - 8 + v.length]);
    //        } else {
    //          v2 = v2 << 1;
    //          v2 = v2 + Character.getNumericValue(v[k - 8 + v.length]);
    //        }
    //      }
    //      op1_v1[i] = v1;
    //      op1_v2[i] = v2 << (packWidth - (8 - splitPos));
    ////      System.out.println(op1_v1[i]);
    ////      System.out.println(op1_v2[i]);
    //    }
    //    long elapsedTime = System.nanoTime() - start;
    //    op1.addValue(elapsedTime / 1000.0);

    System.out.println("op1");
    //    long[] op1_v1 = new long[repeat];
    //    long[] op1_v2 = new long[repeat];
    long sum1 = 0;
    long start = System.nanoTime();
    for (int i = 0; i < repeat; i++) {
      long v1 = 0;
      long v2 = 0;
      int data = buf[i] & 0xff;
      for (int k = 7; k >= 0; k--) {
        int b = data % 2;
        data /= 2;
        if (k < splitPos) {
          v1 = v1 + (b << (splitPos - k - 1));
        } else {
          v2 = v2 + (b << (7 - k));
        }
      }
      //      op1_v1[i] = v1;
      //      op1_v2[i] = v2 << (packWidth - (8 - splitPos));
      //      System.out.println(op1_v1[i]);
      //      System.out.println(op1_v2[i]);
      sum1 += v1;
      sum1 += v2 << (packWidth - (8 - splitPos));
    }
    long elapsedTime = System.nanoTime() - start;
    op1.addValue(elapsedTime / 1000.0);

    // test op2
    System.out.println("op2");
    //    long[] op2_v1 = new long[repeat];
    //    long[] op2_v2 = new long[repeat];
    long sum2 = 0;
    start = System.nanoTime();
    for (int i = 0; i < repeat; i++) {
      byte data = buf[i];
      long v1 = 0;
      long v2 = 0;
      //      op2_v1[i] = v1 | ((data & mask1) >> 5);
      //      op2_v2[i] = v2 | ((data & mask2) << (packWidth - (8 - splitPos)));
      //      System.out.println(op2_v1[i]);
      //      System.out.println(op2_v2[i]);
      sum2 += v1 | ((data & mask1) >> 5);
      sum2 += v2 | ((data & mask2) << (packWidth - (8 - splitPos)));
    }
    elapsedTime = System.nanoTime() - start;
    op2.addValue(elapsedTime / 1000.0);

    //    for (int i = 0; i < repeat; i++) {
    //      Assert.assertEquals(op1_v1[i], op2_v1[i]);
    //      Assert.assertEquals(op1_v2[i], op2_v2[i]);
    //    }
    Assert.assertEquals(sum1, sum2);

    printStat(op1, "op1-iterateBits");
    printStat(op2, "op2-useMasks");
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

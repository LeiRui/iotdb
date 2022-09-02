package org.apache.iotdb.tsfile.read;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RLTestReadDataSpeed {

  // TsFileSequenceReader.readData(pos, totalSize)的totalSize增加，读取平均速度是否变慢
  public static void main(String[] args) throws Exception {
    int totalSize = 1 * 1024 * 1024;

    // 打开一个大文件
    String filePath = "E:\\github\\cost2\\iotdb\\testTsFile\\10000_1000_50_1662120019473.tsfile";
    TsFileSequenceReader fileReader = new TsFileSequenceReader(filePath, true);
    long fileSize = fileReader.tsFileInput.size();

    long pos = fileSize; // 倒着读
    long n = fileSize / totalSize;
    List<Double> times = new ArrayList<>();
    // 执行若干次读取totalSize大小的buffer的操作，记录每次操作的耗时，最后计算平均耗时和平均速度
    for (int i = 0; i < n; i++) {
      pos -= totalSize;
      long start = System.nanoTime();
      ByteBuffer buffer = fileReader.readData(pos, totalSize);
      double elapsedTime = (System.nanoTime() - start) / 1000.0;
      times.add(elapsedTime);
      System.out.println(buffer.capacity());
    }
    fileReader.close();

    double sum = 0;
    for (Double t : times) {
      System.out.println(t + "us");
      sum += t;
    }
    double averageTime = sum / n;
    System.out.println(n + " ops");
    System.out.println("each op readData of size " + totalSize / 1024.0 / 1024.0 + "MB");
    System.out.println("each op average time: " + averageTime + "us");
    System.out.println("speed: " + totalSize / averageTime + "Bytes/us");
  }

}

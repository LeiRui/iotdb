package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Assert;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RLTestReadDataSpeed {

  // TsFileSequenceReader.readData(pos, totalSize)的totalSize增加，读取平均速度是否变慢
  public static void main(String[] args) throws Exception {
    int totalSize = 20 * 1024 * 1024;
    int headerSize = 1024 * 4;

    // 写一个大文件
    int pagePointNum = 10000;
    int numOfPagesInChunk = 1000;
    int numOfChunksWritten = 10;
    String filePath =
        "testTsFile"
            + File.separator
            + pagePointNum
            + "_"
            + numOfPagesInChunk
            + "_"
            + numOfChunksWritten
            + "_"
            + System.currentTimeMillis()
            + ".tsfile";
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    // only consider one time series here
    String deviceName = "d1";
    String sensorName = "s1";
    Path mypath = new Path(deviceName, sensorName);

    int chunkPointNum = pagePointNum * numOfPagesInChunk;
    int rowNum = chunkPointNum * numOfChunksWritten; // 写数据点数

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setMaxNumberOfPointsInPage(pagePointNum);
    tsFileConfig.setPageSizeInByte(Integer.MAX_VALUE);
    tsFileConfig.setGroupSizeInByte(
        Integer
            .MAX_VALUE); // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。

    // 设置时间戳列的编码方式 TS_2DIFF, PLAIN and RLE(run-length encoding). Default value is TS_2DIFF.
    String timeEncoding = "PLAIN";
    tsFileConfig.setTimeEncoder(timeEncoding);

    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
    TSDataType valueDataType = TSDataType.INT32;
    TSEncoding valueEncoding = TSEncoding.PLAIN;
    CompressionType compressionType = CompressionType.LZ4;
    MeasurementSchema measurementSchema =
        new MeasurementSchema(sensorName, valueDataType, valueEncoding, compressionType);
    tsFileWriter.registerTimeseries(new Path(mypath.getDevice()), measurementSchema);

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(measurementSchema);
    Tablet tablet =
        new Tablet(
            deviceName,
            schemaList,
            chunkPointNum); // 设置Tablet的maxRowNumber等于一个chunk里想要的数据量大小，因为tablet是flush的最小单位
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    long timestamp = 1;
    Random ran = new Random();
    for (int r = 0; r < rowNum; r++) {
      int row = tablet.rowSize++;
      timestamps[row] = timestamp++;
      int[] sensor = (int[]) values[0];
      sensor[row] = ran.nextInt(100);
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        tsFileWriter.write(tablet);
        tablet.reset();
        tsFileWriter
            .flushAllChunkGroups(); // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。
      }
    }
    // write Tablet to TsFile
    if (tablet.rowSize != 0) {
      tsFileWriter.write(tablet);
      tablet.reset();
    }
    tsFileWriter.flushAllChunkGroups();
    tsFileWriter.close();
    System.out.println("TsFile written!");

    // 读这个大文件
    TsFileSequenceReader fileReader = new TsFileSequenceReader(filePath, true);
    long fileSize = fileReader.tsFileInput.size();

    //    long pos = fileSize; // 倒着读
    long pos = 0; // 顺着读
    long n = fileSize / (totalSize + headerSize);
    List<Double> times = new ArrayList<>();
    // 执行若干次读取totalSize大小的buffer的操作，记录每次操作的耗时，最后计算平均耗时和平均速度
    for (int i = 0; i < n; i++) {
      long start = System.nanoTime();
      ByteBuffer buffer = fileReader.readData(pos, totalSize);
      double elapsedTime = (System.nanoTime() - start) / 1000.0;
      times.add(elapsedTime);
      System.out.println(buffer.capacity());
      pos += totalSize;
      ByteBuffer buffer2 = fileReader.readData(pos, headerSize);
      pos += headerSize;
      System.out.println(buffer2.capacity());
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

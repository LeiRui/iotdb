package org.apache.iotdb.tsfile.read;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.Assert;

public class RLTestChunkReadCost {

  public static void main(String[] args) throws Exception {
    // ==============write tsfile==============
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    // only consider one time series here
    String deviceName = "d1";
    String sensorName = "s1";
    Path mypath = new Path(deviceName, sensorName);

    int numOfPagesInChunk = 100;
    int numOfChunksWritten = 10;
    int pagePointNum = 1000000;

    int chunkPointNum = pagePointNum * numOfPagesInChunk;
    int rowNum = chunkPointNum * numOfChunksWritten; // 写数据点数

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setMaxNumberOfPointsInPage(pagePointNum);
    tsFileConfig.setGroupSizeInByte(
        Integer
            .MAX_VALUE); // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。

    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
    MeasurementSchema measurementSchema =
        new MeasurementSchema(sensorName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4);
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

    // ==============warm up JIT==============
    // warmUpJIT();
    /*
    Why does the first method call always take the longest?
    应对尝试：先把大tsfile写了，然后写一个小的tsfile并且读一遍，然后紧跟着读大tsfile。
    不过到了读大TsFile好像还是第一次操作耗时略大些。只是说没有那么大了。
    思路2：去掉重复调用函数的第一次耗时。但是有个问题是有的操作就是只执行1次的，
    比如1_index_read_deserialize_MagicString_FileMetadataSize，它的耗时包括了JIT加载函数代码的代价，
    所以思路3：就让第一次耗时大的留着，这本来就是会有的代价。
     */

    // ==============read tsfile==============

    Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
    long totalStart = System.nanoTime();

    // read 【tailMagic】 and 【metadataSize】
    TsFileSequenceReader fileReader =
        new TsFileSequenceReader(filePath, true, elapsedTimeInNanoSec);

    // read TsFileMetadata and instantiate an empty chunkMetaDataCache
    MetadataQuerierByFileImpl metadataQuerier =
        new MetadataQuerierByFileImpl(fileReader, elapsedTimeInNanoSec);

    //    long start = System.nanoTime();

    // instantiate an empty chunkCache
    CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileReader);

    // do nothing special
    TsFileExecutor tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);

    // do nothing special
    TsFileReader tsFileReader =
        new TsFileReader(fileReader, metadataQuerier, chunkLoader, tsFileExecutor);

    List<Path> selectedSeries = Arrays.asList(mypath);
    List<Path> filteredSeriesPath = new ArrayList<>();
    // use BloomFilter in TsFileMetadata to filter
    BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
    if (bloomFilter != null) {
      for (Path p : selectedSeries) {
        if (bloomFilter.contains(p.getFullPath())) {
          filteredSeriesPath.add(p);
        }
      }
      selectedSeries = filteredSeriesPath;
    }

    //    long elapsedTime = System.nanoTime() - start;
    //    if (!elapsedTimeInNanoSec.containsKey(TsFileConstant.other_cpu_time)) {
    //      elapsedTimeInNanoSec.put(TsFileConstant.other_cpu_time, new ArrayList<>());
    //    }
    //    elapsedTimeInNanoSec.get(TsFileConstant.other_cpu_time).add(elapsedTime);

    // fill up chunkMetaDataCache by reading 【TimeseriesIndex】
    metadataQuerier.loadChunkMetaDatas(selectedSeries, elapsedTimeInNanoSec);

    List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : selectedSeries) {
      // try to get 【ChunkIndex】 from chunkMetaDataCache. Probably cache hit.
      List<IChunkMetadata> chunkMetadataList =
          metadataQuerier.getChunkMetaDataList(path, elapsedTimeInNanoSec);
      AbstractFileSeriesReader seriesReader;
      if (chunkMetadataList.isEmpty()) {
        seriesReader = new EmptyFileSeriesReader();
        dataTypes.add(metadataQuerier.getDataType(path));
      } else {
        // assume timeExpression == null
        seriesReader =
            new FileSeriesReader(
                chunkLoader, chunkMetadataList, null, elapsedTimeInNanoSec); // do nothing special
        dataTypes.add(chunkMetadataList.get(0).getDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }

    // loading and deserializing 【ChunkHeader】,
    // loading 【ChunkData】 buffer,
    // get pageReaderList by deserializing 【PageHeader】,
    // uncompressing 【PageData】 and split into timeBuffer and valueBuffer for pageReader
    QueryDataSet queryDataSet =
        new DataSetWithoutTimeGenerator(selectedSeries, dataTypes, readersOfSelectedSeries);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      // pageReader getAllSatisfiedPageData by timeDecoding and valueDecoding the timeBuffer and
      // valueBuffer respectively
      RowRecord next = queryDataSet.next();
      // System.out.println(next);
      cnt++;
    }

    long totalTime = System.nanoTime() - totalStart;
    if (!TsFileConstant.decomposeMeasureTime) {
      if (!elapsedTimeInNanoSec.containsKey(TsFileConstant.total_time)) {
        elapsedTimeInNanoSec.put(TsFileConstant.total_time, new ArrayList<>());
      }
      elapsedTimeInNanoSec.get(TsFileConstant.total_time).add(totalTime);
    }

    System.out.println(cnt);

    // ==============close and delete tsfile==============
    tsFileReader.close();
    //    file.delete();

    // ==============print elapsed time rechsults==============
    double totalSum = 0;
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      System.out.print(key + ":");
      double sum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
        stringBuilder.append(t / 1000.0 + ", ");
      }
      System.out.println(
          "[" + sum + "us(SUM)," + elapsedTimes.size() + "(CNT)]:" + stringBuilder.toString());
      System.out.println();

      if (!key.equals(TsFileConstant.total_time)) {
        totalSum += sum;
      }
    }
    System.out.println("sum 1-8: " + totalSum + "us");
  }

  /**
   * write and read a small fake TsFile to warm up JIT
   */
  public static void warmUpJIT() throws Exception {
    System.out.println("==============warmUpJIT begins==============");
    // ==============write tsfile==============
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    // only consider one time series here
    String deviceName = "d1";
    String sensorName = "s1";
    Path mypath = new Path(deviceName, sensorName);

    int numOfPagesInChunk = 20;
    int numOfChunksWritten = 10;
    int pagePointNum = 10;

    int chunkPointNum = pagePointNum * numOfPagesInChunk;
    int rowNum = chunkPointNum * numOfChunksWritten; // 写数据点数

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setMaxNumberOfPointsInPage(pagePointNum);
    tsFileConfig.setGroupSizeInByte(
        Integer
            .MAX_VALUE); // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。

    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
    MeasurementSchema measurementSchema =
        new MeasurementSchema(sensorName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4);
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

    // ==============read tsfile==============
    Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
    long totalStart = System.nanoTime();

    // read 【tailMagic】 and 【metadataSize】
    TsFileSequenceReader fileReader =
        new TsFileSequenceReader(filePath, true, elapsedTimeInNanoSec);

    // read TsFileMetadata and instantiate an empty chunkMetaDataCache
    MetadataQuerierByFileImpl metadataQuerier =
        new MetadataQuerierByFileImpl(fileReader, elapsedTimeInNanoSec);

    //    long start = System.nanoTime();

    // instantiate an empty chunkCache
    CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileReader);

    // do nothing special
    TsFileExecutor tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);

    // do nothing special
    TsFileReader tsFileReader =
        new TsFileReader(fileReader, metadataQuerier, chunkLoader, tsFileExecutor);

    List<Path> selectedSeries = Arrays.asList(mypath);
    List<Path> filteredSeriesPath = new ArrayList<>();
    // use BloomFilter in TsFileMetadata to filter
    BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
    if (bloomFilter != null) {
      for (Path p : selectedSeries) {
        if (bloomFilter.contains(p.getFullPath())) {
          filteredSeriesPath.add(p);
        }
      }
      selectedSeries = filteredSeriesPath;
    }

    // fill up chunkMetaDataCache by reading 【TimeseriesIndex】
    metadataQuerier.loadChunkMetaDatas(selectedSeries, elapsedTimeInNanoSec);

    List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : selectedSeries) {
      // try to get 【ChunkIndex】 from chunkMetaDataCache. Probably cache hit.
      List<IChunkMetadata> chunkMetadataList =
          metadataQuerier.getChunkMetaDataList(path, elapsedTimeInNanoSec);
      AbstractFileSeriesReader seriesReader;
      if (chunkMetadataList.isEmpty()) {
        seriesReader = new EmptyFileSeriesReader();
        dataTypes.add(metadataQuerier.getDataType(path));
      } else {
        // assume timeExpression == null
        seriesReader =
            new FileSeriesReader(
                chunkLoader, chunkMetadataList, null, elapsedTimeInNanoSec); // do nothing special
        dataTypes.add(chunkMetadataList.get(0).getDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }

    // loading and deserializing 【ChunkHeader】,
    // loading 【ChunkData】 buffer,
    // get pageReaderList by deserializing 【PageHeader】,
    // uncompressing 【PageData】 and split into timeBuffer and valueBuffer for pageReader
    QueryDataSet queryDataSet =
        new DataSetWithoutTimeGenerator(selectedSeries, dataTypes, readersOfSelectedSeries);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      // pageReader getAllSatisfiedPageData by timeDecoding and valueDecoding the timeBuffer and
      // valueBuffer respectively
      RowRecord next = queryDataSet.next();
      // System.out.println(next);
      cnt++;
    }

    long totalTime = System.nanoTime() - totalStart;
    if (!TsFileConstant.decomposeMeasureTime) {
      if (!elapsedTimeInNanoSec.containsKey(TsFileConstant.total_time)) {
        elapsedTimeInNanoSec.put(TsFileConstant.total_time, new ArrayList<>());
      }
      elapsedTimeInNanoSec.get(TsFileConstant.total_time).add(totalTime);
    }

    System.out.println(cnt);

    // ==============close and delete tsfile==============
    tsFileReader.close();
    file.delete();

    // ==============print elapsed time rechsults==============
    double totalSum = 0;
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      System.out.print(key + ":");
      double sum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
        stringBuilder.append(t / 1000.0 + ", ");
      }
      System.out.println(
          "[" + sum + "us(SUM)," + elapsedTimes.size() + "(CNT)]:" + stringBuilder.toString());
      System.out.println();

      if (!key.equals(TsFileConstant.total_time)) {
        totalSum += sum;
      }
    }
    System.out.println("sum 1-8: " + totalSum + "us");

    System.out.println("==============warmUpJIT finishes==============");
    System.out.println();
  }


}

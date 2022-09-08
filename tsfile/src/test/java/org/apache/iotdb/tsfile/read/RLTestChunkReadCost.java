package org.apache.iotdb.tsfile.read;

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

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class RLTestChunkReadCost {

  public static void main(String[] args) throws Exception {
    int pagePointNum = 10000;
    int numOfPagesInChunk = 1000;
    int numOfChunksWritten = 10;
    // 设置时间戳列的编码方式 TS_2DIFF, PLAIN and RLE(run-length encoding). Default value is TS_2DIFF.
    String timeEncoding = "TS_2DIFF";
    TSDataType valueDataType = TSDataType.INT64;
    TSEncoding valueEncoding = TSEncoding.PLAIN; // PLAIN / RLE / TS_2DIFF / GORILLA
    CompressionType compressionType = CompressionType.LZ4; // UNCOMPRESSED / SNAPPY / GZIP / LZ4

    // ==============write tsfile==============
    // final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
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

    tsFileConfig.setTimeEncoder(timeEncoding);

    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
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
      long[] sensor = (long[]) values[0];
      // int[] sensor = (int[]) values[0];
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

    int cnt = 0;
    // loading and deserializing 【ChunkHeader】,
    // loading 【ChunkData】 buffer,
    // get pageReaderList by deserializing 【PageHeader】,
    // uncompressing 【PageData】 and split into timeBuffer and valueBuffer for pageReader
    QueryDataSet queryDataSet =
        new DataSetWithoutTimeGenerator(selectedSeries, dataTypes, readersOfSelectedSeries);

    while (queryDataSet.hasNext()) {
      // pageReader getAllSatisfiedPageData by timeDecoding and valueDecoding the timeBuffer and
      // valueBuffer respectively
      RowRecord next = queryDataSet.next();
      //        System.out.println(next);
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

    // ==============print elapsed time results==============
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
      System.out.print("[" + sum + "us(SUM)," + elapsedTimes.size() + "(CNT)]:");
      System.out.println();
      //      System.out.println(stringBuilder.toString());

      if (!key.equals(TsFileConstant.total_time)) {
        totalSum += sum;
      }
    }
    System.out.println("sum 1-8: " + totalSum + "us");
    System.out.println("ALL FINISHED!");

    //    System.out.println(
    //        "====================================focus
    // results====================================");
    //    System.out.println("- pagePointNum=" + pagePointNum);
    //    System.out.println("- numOfPagesInChunk=" + numOfPagesInChunk);
    //    System.out.println("- numOfChunksWritten=" + numOfChunksWritten);
    //    // calculate statistics for step 4-8
    //    List<String> focus = new ArrayList<>();
    //    // focus.add(TsFileConstant.data_read_deserialize_ChunkHeader);
    //    focus.add(TsFileConstant.data_read_ChunkData);
    //    // focus.add(TsFileConstant.data_deserialize_PageHeader);
    //    focus.add(TsFileConstant.data_ByteBuffer_to_ByteArray);
    //    focus.add(TsFileConstant.data_decompress_PageData);
    //    // focus.add(TsFileConstant.data_ByteArray_to_ByteBuffer);
    //    // focus.add(TsFileConstant.data_split_time_value_Buffer);
    //    focus.add(TsFileConstant.data_decode_time_value_Buffer);
    //    StringBuilder stringBuilder = new StringBuilder();
    //    for (String key : focus) {
    //      DescriptiveStatistics stats = new DescriptiveStatistics();
    //      for (long t : elapsedTimeInNanoSec.get(key)) {
    //        stats.addValue(t);
    //      }
    //      long num = stats.getN();
    //      double max = stats.getMax() / 1000.0;
    //      double min = stats.getMin() / 1000.0;
    //      double mean = stats.getMean() / 1000.0;
    //      double std = stats.getStandardDeviation() / 1000.0;
    //      double p25 = stats.getPercentile(25) / 1000.0;
    //      double p50 = stats.getPercentile(50) / 1000.0;
    //      double p75 = stats.getPercentile(75) / 1000.0;
    //      double p90 = stats.getPercentile(90) / 1000.0;
    //      double p95 = stats.getPercentile(95) / 1000.0;
    //      System.out.println(
    //          "- " + key + ": " + "mean=" + mean + "us, " + "num=" + num + ", " + "min=" + min +
    // "us, "
    //              + "max=" + max + "us, " + "std=" + std + "us, " + "p25=" + p25 + "us, " + "p50="
    // + p50
    //              + "us, " + "p75=" + p75 + "us, " + "p90=" + p90 + "us, " + "p95=" + p95 + "us,
    // ");
    //      stringBuilder.append(mean);
    //      stringBuilder.append(", ");
    //    }
    //    System.out.println(stringBuilder.toString());

    System.out.println(
        "====================================sum results====================================");
    double A_get_chunkMetadatas = 0;
    double B_load_on_disk_chunk = 0;
    double C_get_pageHeader = 0;
    double D_1_decompress_pageData_in_batch = 0;
    double D_2_decode_pageData_point_by_point = 0;
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      double sum = 0;
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
      }
      if (key.equals(TsFileConstant.index_read_deserialize_MagicString_FileMetadataSize)
          || key.equals(TsFileConstant.index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter)
          || key.equals(
              TsFileConstant
                  .index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp)
          || key.equals(
              TsFileConstant
                  .index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet)) {
        A_get_chunkMetadatas += sum;
      }
      if (key.equals(TsFileConstant.data_read_deserialize_ChunkHeader)
          || key.equals(TsFileConstant.data_read_ChunkData)) {
        B_load_on_disk_chunk += sum;
      }
      if (key.equals(TsFileConstant.data_deserialize_PageHeader)) {
        C_get_pageHeader += sum;
      }
      if (key.equals(TsFileConstant.data_ByteBuffer_to_ByteArray)
          || key.equals(TsFileConstant.data_decompress_PageData)
          || key.equals(TsFileConstant.data_ByteArray_to_ByteBuffer)
          || key.equals(TsFileConstant.data_split_time_value_Buffer)) {
        D_1_decompress_pageData_in_batch += sum;
      }
      if (key.equals(TsFileConstant.data_decode_time_value_Buffer)
          || key.equals(TsFileConstant.D_2_createBatchData)
          || key.equals(TsFileConstant.D_2_timeDecoder_hasNext)
          || key.equals(TsFileConstant.D_2_timeDecoder_readLong)
          || key.equals(TsFileConstant.D_2_valueDecoder_read)
          || key.equals(TsFileConstant.D_2_checkValueSatisfyOrNot)
          || key.equals(TsFileConstant.D_2_putIntoBatchData)) {
        D_2_decode_pageData_point_by_point += sum;
      }
    }
    double total = 0;
    total += A_get_chunkMetadatas;
    total += B_load_on_disk_chunk;
    total += C_get_pageHeader;
    total += D_1_decompress_pageData_in_batch;
    total += D_2_decode_pageData_point_by_point;
    DecimalFormat df = new DecimalFormat("0.00");
    System.out.println(
        "(A)get_chunkMetadatas = "
            + df.format(A_get_chunkMetadatas)
            + "us, "
            + df.format(A_get_chunkMetadatas / total * 100)
            + "%");
    System.out.println(
        "(B)load_on_disk_chunk = "
            + df.format(B_load_on_disk_chunk)
            + "us, "
            + df.format(B_load_on_disk_chunk / total * 100)
            + "%");
    System.out.println(
        "(C)get_pageHeader = "
            + df.format(C_get_pageHeader)
            + "us, "
            + df.format(C_get_pageHeader / total * 100)
            + "%");
    System.out.println(
        "(D_1)decompress_pageData_in_batch = "
            + df.format(D_1_decompress_pageData_in_batch)
            + "us, "
            + df.format(D_1_decompress_pageData_in_batch / total * 100)
            + "%");
    System.out.println(
        "(D_2)decode_pageData_point_by_point = "
            + df.format(D_2_decode_pageData_point_by_point)
            + "us, "
            + df.format(D_2_decode_pageData_point_by_point / total * 100)
            + "%");
    System.out.println("total time = " + total + "us");

    if (TsFileConstant.D_2_decompose_each_step) {
      System.out.println(
          "====================================D_2_decompose_each_step====================================");
      long total_D2 =
          elapsedTimeInNanoSec.get(TsFileConstant.D_2_createBatchData).get(0)
              + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_hasNext).get(0)
              + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_readLong).get(0)
              + elapsedTimeInNanoSec.get(TsFileConstant.D_2_valueDecoder_read).get(0)
              + elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0)
              + elapsedTimeInNanoSec.get(TsFileConstant.D_2_putIntoBatchData).get(0);
      System.out.println(
          TsFileConstant.D_2_createBatchData
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_createBatchData).get(0) / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_createBatchData).get(0)
                      * 100.0
                      / total_D2)
              + "%");
      System.out.println(
          TsFileConstant.D_2_timeDecoder_hasNext
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_hasNext).get(0) / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_hasNext).get(0)
                      * 100.0
                      / total_D2)
              + "%");
      System.out.println(
          TsFileConstant.D_2_timeDecoder_readLong
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_readLong).get(0) / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_readLong).get(0)
                      * 100.0
                      / total_D2)
              + "%");
      System.out.println(
          TsFileConstant.D_2_valueDecoder_read
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_valueDecoder_read).get(0) / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_valueDecoder_read).get(0)
                      * 100.0
                      / total_D2)
              + "%");
      System.out.println(
          TsFileConstant.D_2_checkValueSatisfyOrNot
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0)
                      / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0)
                      * 100.0
                      / total_D2)
              + "%");
      System.out.println(
          TsFileConstant.D_2_putIntoBatchData
              + ": "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_putIntoBatchData).get(0) / 1000.0)
              + "us, "
              + df.format(
                  elapsedTimeInNanoSec.get(TsFileConstant.D_2_putIntoBatchData).get(0)
                      * 100.0
                      / total_D2)
              + "%");
    }
    System.out.println(
        "====================================parameters====================================");
    System.out.println("file: " + filePath);
    System.out.println("pagePointNum = " + pagePointNum);
    System.out.println("numOfPagesInChunk = " + numOfPagesInChunk);
    System.out.println("numOfChunksWritten = " + numOfChunksWritten);
    System.out.println("time encoding: " + timeEncoding);
    System.out.println("value data type: " + valueDataType);
    System.out.println("value encoding: " + valueEncoding);
    System.out.println("compression: " + compressionType);
  }

  /** write and read a small fake TsFile to warm up JIT */
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

    int pagePointNum = 10;
    int numOfPagesInChunk = 20;
    int numOfChunksWritten = 10;

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

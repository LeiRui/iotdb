package org.apache.iotdb.tsfile.read;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.Assert;

public class RLTestChunkReadCost {

  public static void main(String[] args) throws Exception {
    //==============write tsfile==============
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setMaxNumberOfPointsInPage(1000); // set small pages
    tsFileConfig.setGroupSizeInByte(100 * 1024 * 1024);
    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
    Path mypath = new Path("t", "id");
    tsFileWriter.registerTimeseries(
        new Path(mypath.getDevice()),
        new MeasurementSchema("id", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));

    for (int i = 0; i < 100000; i++) {
      TSRecord t = new TSRecord(i, "t");
      t.addTuple(new IntDataPoint("id", i));
      tsFileWriter.write(t);
    }
    tsFileWriter.flushAllChunkGroups();
    tsFileWriter.close();

    //==============read tsfile==============

    Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();

    // read 【tailMagic】 and 【metadataSize】
    TsFileSequenceReader fileReader = new TsFileSequenceReader(filePath, true,
        elapsedTimeInNanoSec);

    // read TsFileMetadata and instantiate an empty chunkMetaDataCache
    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(fileReader,
        elapsedTimeInNanoSec);

    long start = System.nanoTime();

    // instantiate an empty chunkCache
    CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileReader);

    // do nothing special
    TsFileExecutor tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);

    // do nothing special
    TsFileReader tsFileReader = new TsFileReader(fileReader, metadataQuerier, chunkLoader,
        tsFileExecutor);

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

    long elapsedTime = System.nanoTime() - start;
    if (!elapsedTimeInNanoSec.containsKey(TsFileConstant.other_cpu_time)) {
      elapsedTimeInNanoSec.put(TsFileConstant.other_cpu_time, new ArrayList<>());
    }
    elapsedTimeInNanoSec.get(TsFileConstant.other_cpu_time).add(elapsedTime);

    // fill up chunkMetaDataCache by reading 【TimeseriesIndex】
    metadataQuerier.loadChunkMetaDatas(selectedSeries, elapsedTimeInNanoSec);

    List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : selectedSeries) {
      // try to get 【ChunkIndex】 from chunkMetaDataCache. Probably cache hit.
      List<IChunkMetadata> chunkMetadataList = metadataQuerier
          .getChunkMetaDataList(path, elapsedTimeInNanoSec);
      AbstractFileSeriesReader seriesReader;
      if (chunkMetadataList.isEmpty()) {
        seriesReader = new EmptyFileSeriesReader();
        dataTypes.add(metadataQuerier.getDataType(path));
      } else {
        // assume timeExpression == null
        seriesReader = new FileSeriesReader(chunkLoader, chunkMetadataList,
            null, elapsedTimeInNanoSec); // do nothing special
        dataTypes.add(chunkMetadataList.get(0).getDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }
    // loading and deserializing 【ChunkHeader】,
    // loading 【ChunkData】 buffer,
    // get pageReaderList by deserializing 【PageHeader】,
    // uncompressing 【PageData】 and split into timeBuffer and valueBuffer for pageReader
    QueryDataSet queryDataSet = new DataSetWithoutTimeGenerator(selectedSeries, dataTypes,
        readersOfSelectedSeries);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      // pageReader getAllSatisfiedPageData by timeDecoding and valueDecoding the timeBuffer and valueBuffer respectively
      RowRecord next = queryDataSet.next();
//      System.out.println(next);
      cnt++;
    }
    System.out.println(cnt);

    //==============close and delete tsfile==============
    tsFileReader.close();
    file.delete();

    //==============print elapsed time results==============
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      System.out.print(key + ":");
      double sum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000000.0;
        stringBuilder.append(t / 1000000.0 + ",");
      }
      System.out.println("[" + sum + "ms(SUM)," + elapsedTimes.size() + "(CNT)]:" + stringBuilder.toString());
    }
  }
}
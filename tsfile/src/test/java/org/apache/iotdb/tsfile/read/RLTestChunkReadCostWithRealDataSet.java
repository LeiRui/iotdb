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
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RLTestChunkReadCostWithRealDataSet {

  // only consider one time series here
  public static String deviceName = "d1";
  public static String sensorName = "s1";
  public static Path mypath = new Path(deviceName, sensorName);

  // DecimalFormat df = new DecimalFormat("0.00");
  public static DecimalFormat df = new DecimalFormat("#,###.00");

  public static void main(String[] args) throws Exception {
    String csvData = "D:\\LabSync\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\ZC data\\ZT17.csv";
    int pagePointNum = 10000;
    int numOfPagesInChunk = 10;
    String timeEncoding = "TS_2DIFF"; // 时间戳列的编码方式，默认即为TS_2DIFF
    TSDataType valueDataType = TSDataType.DOUBLE;
    TSEncoding valueEncoding = TSEncoding.PLAIN; // PLAIN / RLE / TS_2DIFF / GORILLA
    CompressionType compressionType =
        CompressionType.UNCOMPRESSED; // UNCOMPRESSED / SNAPPY / GZIP / LZ4

    // ==============write a TsFile from csv data==============
    String filePath =
        "testTsFile"
            + File.separator
            + FilenameUtils.removeExtension(new File(csvData).getName())
            + "_ppn_"
            + pagePointNum
            + "_pic_"
            + numOfPagesInChunk
            + "_vt_"
            + valueDataType
            + "_ve_"
            + valueEncoding
            + "_co_"
            + compressionType
            + "_"
            + System.currentTimeMillis()
            + ".tsfile";

    int pointNum =
        writeTsFile(
            csvData,
            pagePointNum,
            numOfPagesInChunk,
            timeEncoding,
            valueDataType,
            valueEncoding,
            compressionType,
            filePath);

    // ==============begin read tsfile test==============
    int repeat = 5;
    PrintWriter pw =
        new PrintWriter(
            new File(
                    "testTsFile"
                        + File.separator
                        + FilenameUtils.removeExtension(new File(filePath).getName()))
                + "_result.csv");
    if (!TsFileConstant.decomposeMeasureTime) {
      pw.println(
          "data,totalPointNum,pagePointNum,numOfPagesInChunk,valueType,valueEncoding,compressionType,totalTime(us)");
    } else if (!TsFileConstant.D_2_decompose_each_step) {
      pw.println(
          "data,totalPointNum,pagePointNum,numOfPagesInChunk,valueType,valueEncoding,compressionType,"
              + "(A)1_index_read_deserialize_MagicString_FileMetadataSize(us),"
              + "(A)2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter(us),"
              + "(A)3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp(us),"
              + "(A)3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet(us),"
              + "(B)4_data_read_deserialize_ChunkHeader(us),"
              + "(B)5_data_read_ChunkData(us),"
              + "(C)6_data_deserialize_PageHeader(us),"
              + "(D-1)7_1_data_ByteBuffer_to_ByteArray(us),"
              + "(D-1)7_2_data_decompress_PageData(us),"
              + "(D-1)7_3_data_ByteArray_to_ByteBuffer(us),"
              + "(D-1)7_4_data_split_time_value_Buffer(us),"
              + "(D-2)8_data_decode_time_value_Buffer(us)");
    } else {
      pw.println(
          "data,totalPointNum,pagePointNum,numOfPagesInChunk,valueType,valueEncoding,compressionType,"
              + "(A)1_index_read_deserialize_MagicString_FileMetadataSize(us),"
              + "(A)2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter(us),"
              + "(A)3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp(us),"
              + "(A)3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet(us),"
              + "(B)4_data_read_deserialize_ChunkHeader(us),"
              + "(B)5_data_read_ChunkData(us),"
              + "(C)6_data_deserialize_PageHeader(us),"
              + "(D-1)7_1_data_ByteBuffer_to_ByteArray(us),"
              + "(D-1)7_2_data_decompress_PageData(us),"
              + "(D-1)7_3_data_ByteArray_to_ByteBuffer(us),"
              + "(D-1)7_4_data_split_time_value_Buffer(us),"
              + "(D-2)8_1_createBatchData(us),"
              + "(D-2)8_2_timeDecoder_hasNext(us),"
              + "(D-2)8_3_timeDecoder_readLong(us),"
              + "(D-2)8_4_valueDecoder_read(us),"
              + "(D-2)8_5_checkValueSatisfyOrNot(us),"
              + "(D-2)8_6_putIntoBatchData(us),");
    }

    for (int n = 0; n < repeat; n++) {
      Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
      long totalStart = System.nanoTime();

      // 【1_index_read_deserialize_MagicString_FileMetadataSize】
      TsFileSequenceReader fileReader =
          new TsFileSequenceReader(filePath, true, elapsedTimeInNanoSec);

      // 【2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter】
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

      // 【3_1_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp】
      metadataQuerier.loadChunkMetaDatas(selectedSeries, elapsedTimeInNanoSec);

      List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (Path path : selectedSeries) {
        // 【3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet】
        List<IChunkMetadata> chunkMetadataList =
            metadataQuerier.getChunkMetaDataList(path, elapsedTimeInNanoSec);

        AbstractFileSeriesReader seriesReader;
        if (chunkMetadataList.isEmpty()) {
          seriesReader = new EmptyFileSeriesReader();
          dataTypes.add(metadataQuerier.getDataType(path));
        } else {
          // assume timeExpression == null
          // do nothing special
          seriesReader =
              new FileSeriesReader(chunkLoader, chunkMetadataList, null, elapsedTimeInNanoSec);
          dataTypes.add(chunkMetadataList.get(0).getDataType());
        }
        readersOfSelectedSeries.add(seriesReader);
      }

      int cnt = 0;
      // 【4_data_read_deserialize_ChunkHeader】
      // 【5_data_read_ChunkData】
      // 【6_data_deserialize_PageHeader】
      // 【7_1_data_ByteBuffer_to_ByteArray】
      // 【7_2_data_decompress_PageData】
      // 【7_3_data_ByteArray_to_ByteBuffer】
      // 【7_4_data_split_time_value_Buffer】
      // 【8_data_decode_time_value_Buffer】
      QueryDataSet queryDataSet =
          new DataSetWithoutTimeGenerator(selectedSeries, dataTypes, readersOfSelectedSeries);
      while (queryDataSet.hasNext()) {
        RowRecord next = queryDataSet.next();
        //        System.out.println(next);
        cnt++;
      }

      long runTime = System.nanoTime() - totalStart;
      if (!TsFileConstant.decomposeMeasureTime) {
        TsFileConstant.record(elapsedTimeInNanoSec, TsFileConstant.total_time, runTime, true, true);
      }

      System.out.println("read points: " + cnt);
      tsFileReader.close();
      //    file.delete();

      printEach(elapsedTimeInNanoSec, pw);
      printCategory(elapsedTimeInNanoSec, pw);
      printD2Detail(elapsedTimeInNanoSec, pw);

      System.out.println(
          "====================================data files====================================");
      System.out.println("csvFile: " + csvData);
      System.out.println(
          "csvFile size: " + df.format(new File(csvData).length() / 1024.0 / 1024.0) + "MB");
      System.out.println("TsFile: " + filePath);
      System.out.println(
          "TsFile size: " + df.format(new File(filePath).length() / 1024.0 / 1024.0) + "MB");
      DecimalFormat formatter = new DecimalFormat("#,###");
      System.out.println("pointNum: " + formatter.format(pointNum));
      System.out.println(
          "====================================parameters====================================");
      System.out.println("pagePointNum = " + pagePointNum);
      System.out.println("numOfPagesInChunk = " + numOfPagesInChunk);
      System.out.println("time encoding = " + timeEncoding);
      System.out.println("value data type = " + valueDataType);
      System.out.println("value encoding = " + valueEncoding);
      System.out.println("compression = " + compressionType);
    }
    pw.close();
  }

  // write tsfile from a real dataset csv
  public static int writeTsFile(
      String csvData,
      int pagePointNum,
      int numOfPagesInChunk,
      String timeEncoding,
      TSDataType valueDataType,
      TSEncoding valueEncoding,
      CompressionType compressionType,
      String filePath)
      throws Exception {
    int chunkPointNum = pagePointNum * numOfPagesInChunk;
    int pointNum = 0;
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

    tsFileConfig.setMaxNumberOfPointsInPage(pagePointNum);
    tsFileConfig.setPageSizeInByte(Integer.MAX_VALUE);
    // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。
    tsFileConfig.setGroupSizeInByte(Integer.MAX_VALUE);

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

    // read points from csv and write to TsFile
    try (BufferedReader br = new BufferedReader(new FileReader(csvData))) {
      br.readLine(); // skip header
      for (String line; (line = br.readLine()) != null; ) {
        pointNum++;
        String[] tv = line.split(",");
        long time = Long.parseLong(tv[0]);
        double value = Double.parseDouble(tv[1]);

        int row = tablet.rowSize++;
        timestamps[row] = time;
        double[] sensor = (double[]) values[0];
        sensor[row] = value;
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
          tsFileWriter.flushAllChunkGroups();
        }
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
    return pointNum;
  }

  public static void printEach(Map<String, List<Long>> elapsedTimeInNanoSec, PrintWriter pw) {
    System.out.println(
        "====================================each step====================================");
    double cumulativeSteps = 0;
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
      System.out.print("[" + df.format(sum) + "us(SUM)," + elapsedTimes.size() + "(CNT)]:");
      System.out.println();
      //      System.out.println(stringBuilder.toString());

      cumulativeSteps += sum;
    }
    System.out.println("sum 1-8: " + df.format(cumulativeSteps) + "us");
  }

  public static void printCategory(Map<String, List<Long>> elapsedTimeInNanoSec, PrintWriter pw) {
    if (!TsFileConstant.decomposeMeasureTime) {
      return;
    }
    System.out.println(
        "====================================category results====================================");
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
          // D_2_decompose_each_step = false
          // D_2_decompose_each_step = true
          || key.equals(TsFileConstant.D_2_createBatchData)
          || key.equals(TsFileConstant.D_2_timeDecoder_hasNext)
          || key.equals(TsFileConstant.D_2_timeDecoder_readLong)
          || key.equals(TsFileConstant.D_2_valueDecoder_read)
          || key.equals(TsFileConstant.D_2_checkValueSatisfyOrNot)
          || key.equals(TsFileConstant.D_2_putIntoBatchData)) {
        D_2_decode_pageData_point_by_point += sum;
      }
    }
    double cumulativeSteps = 0;
    cumulativeSteps += A_get_chunkMetadatas;
    cumulativeSteps += B_load_on_disk_chunk;
    cumulativeSteps += C_get_pageHeader;
    cumulativeSteps += D_1_decompress_pageData_in_batch;
    cumulativeSteps += D_2_decode_pageData_point_by_point;

    System.out.println(
        "(A)get_chunkMetadatas = "
            + df.format(A_get_chunkMetadatas)
            + "us, "
            + df.format(A_get_chunkMetadatas / cumulativeSteps * 100)
            + "%");
    System.out.println(
        "(B)load_on_disk_chunkData = "
            + df.format(B_load_on_disk_chunk)
            + "us, "
            + df.format(B_load_on_disk_chunk / cumulativeSteps * 100)
            + "%");
    System.out.println(
        "(C)get_pageHeader = "
            + df.format(C_get_pageHeader)
            + "us, "
            + df.format(C_get_pageHeader / cumulativeSteps * 100)
            + "%");
    System.out.println(
        "(D_1)decompress_pageData_in_batch = "
            + df.format(D_1_decompress_pageData_in_batch)
            + "us, "
            + df.format(D_1_decompress_pageData_in_batch / cumulativeSteps * 100)
            + "%");
    System.out.println(
        "(D_2)decode_pageData_point_by_point = "
            + df.format(D_2_decode_pageData_point_by_point)
            + "us, "
            + df.format(D_2_decode_pageData_point_by_point / cumulativeSteps * 100)
            + "%");
    System.out.println("sum 1-8: " + df.format(cumulativeSteps) + "us");
  }

  public static void printD2Detail(Map<String, List<Long>> elapsedTimeInNanoSec, PrintWriter pw) {
    if (!TsFileConstant.decomposeMeasureTime || !TsFileConstant.D_2_decompose_each_step) {
      return;
    }
    long total_D2 =
        elapsedTimeInNanoSec.get(TsFileConstant.D_2_createBatchData).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_hasNext).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_readLong).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_valueDecoder_read).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_putIntoBatchData).get(0);
    System.out.println(
        "====================================D_2_decompose_each_step====================================");
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
                elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0) / 1000.0)
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
}

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
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class RLTestChunkReadCostWithRealDataSet2 {

  // only consider one time series here
  public static String deviceName = "d1";
  public static String sensorName = "s1";
  public static Path mypath = new Path(deviceName, sensorName);

  // DecimalFormat df = new DecimalFormat("0.00");
  public static DecimalFormat df = new DecimalFormat("#,###.00");

  public static enum ExpType {
    WRITE_SYNC, // write synthetic data
    WRITE_REAL, // write real data
    READ // read data
  }

  /**
   * Three kinds of arguments for writing synthetic data, writing real data, and reading data
   * respectively.
   *
   * <p>WRITE_SYNC [pagePointNum] [numOfPagesInChunk] [chunksWritten] [timeEncoding] [valueDataType]
   * [valueEncoding] [compressionType]
   *
   * <p>WRITE_SYNC 10000 1000 10 TS_2DIFF int32 PLAIN LZ4
   *
   * <p>WRITE_REAL path_of_real_data_csv_to_write [pagePointNum] [numOfPagesInChunk] [timeEncoding]
   * [valueDataType] [valueEncoding] [compressionType]
   *
   * <p>WRITE_REAL "G:\实验室电脑同步\iotdb\我的Gitbook基地\RUI Lei gitbook\ZC data\ZT17.csv" 10000 10 TS_2DIFF
   * FLOAT RLE LZ4
   *
   * <p>READ [path_of_tsfile_to_read] [decomposeMeasureTime] [D_2_decompose_each_step]
   *
   * <p>READ
   * D:\iotdb\testTsFile\ZT17_ppn_10000_pic_10_vt_DOUBLE_ve_PLAIN_co_UNCOMPRESSED_1662604242717.tsfile
   * true false
   */
  public static void main(String[] args) throws Exception {
    ExpType expType;
    int pagePointNum = 0;
    int numOfPagesInChunk = 0;
    int chunksWritten = 0;
    String timeEncoding = ""; // TS_2DIFF, PLAIN, RLE
    TSDataType valueDataType = null; // INT32, INT64, FLOAT, DOUBLE
    TSEncoding valueEncoding = null; // PLAIN, RLE, TS_2DIFF, GORILLA
    CompressionType compressionType = null; // UNCOMPRESSED, SNAPPY, GZIP, LZ4
    String csvData = "";
    String tsfilePath = "";

    String exp = args[0].toUpperCase(Locale.ROOT);
    switch (exp) {
      case "WRITE_SYNC": // WRITE_SYNC [pagePointNum] [numOfPagesInChunk] [chunksWritten]
        // [valueDataType] [valueEncoding] [compressionType]
        expType = ExpType.WRITE_SYNC;
        pagePointNum = Integer.parseInt(args[1]);
        numOfPagesInChunk = Integer.parseInt(args[2]);
        chunksWritten = Integer.parseInt(args[3]);
        timeEncoding = args[4].toUpperCase(Locale.ROOT);
        valueDataType =
            TSDataType.valueOf(args[5].toUpperCase(Locale.ROOT)); // INT32, INT64, FLOAT, DOUBLE
        valueEncoding =
            TSEncoding.valueOf(
                args[6].toUpperCase(Locale.ROOT)); // PLAIN / RLE / TS_2DIFF / GORILLA
        compressionType =
            CompressionType.valueOf(
                args[7].toUpperCase(Locale.ROOT)); // UNCOMPRESSED / SNAPPY / GZIP / LZ4
        long pointNum =
            writeTsFile(
                ExpType.WRITE_SYNC,
                "",
                pagePointNum,
                numOfPagesInChunk,
                chunksWritten,
                timeEncoding,
                valueDataType,
                valueEncoding,
                compressionType);
        break;
      case "WRITE_REAL": // WRITE_REAL path_of_real_data_csv_to_write [pagePointNum]
        // [numOfPagesInChunk] [valueDataType] [valueEncoding] [compressionType]
        expType = ExpType.WRITE_REAL;
        csvData = args[1]; // 要写的真实csv数据
        pagePointNum = Integer.parseInt(args[2]);
        numOfPagesInChunk = Integer.parseInt(args[3]);
        timeEncoding = args[4].toUpperCase(Locale.ROOT);
        valueDataType =
            TSDataType.valueOf(args[5].toUpperCase(Locale.ROOT)); // INT32, INT64, FLOAT, DOUBLE
        valueEncoding =
            TSEncoding.valueOf(
                args[6].toUpperCase(Locale.ROOT)); // PLAIN / RLE / TS_2DIFF / GORILLA
        compressionType =
            CompressionType.valueOf(
                args[7].toUpperCase(Locale.ROOT)); // UNCOMPRESSED / SNAPPY / GZIP / LZ4
        pointNum =
            writeTsFile(
                ExpType.WRITE_REAL,
                csvData,
                pagePointNum,
                numOfPagesInChunk,
                -1,
                timeEncoding,
                valueDataType,
                valueEncoding,
                compressionType);
        break;
      case "READ": // READ path_of_tsfile_to_read
        expType = ExpType.READ;
        tsfilePath = args[1]; // 要读的tsfile路径
        TsFileConstant.decomposeMeasureTime = Boolean.parseBoolean(args[2]);
        TsFileConstant.D_2_decompose_each_step = Boolean.parseBoolean(args[3]);
        break;
      default:
        throw new IOException("Wrong ExpType. Only accept WRITE_SYNC/WRITE_REAL/READ");
    }

    // ==============read tsfile test==============
    if (expType.equals(ExpType.READ)) {
      long cnt = 0;
      TsFileSequenceReader fileReader = null;
      PrintWriter pw = null;
      try {
        pw =
            new PrintWriter(
                new File(
                        "testTsFile"
                            + File.separator
                            + FilenameUtils.removeExtension(new File(tsfilePath).getName()))
                    + "_result.csv");
        if (!TsFileConstant.decomposeMeasureTime) {
          pw.println(
              "data,totalPointNum,pagePointNum,numOfPagesInChunk,timeEncoding,valueType,valueEncoding,compressionType,totalTime(us)");
        } else if (!TsFileConstant.D_2_decompose_each_step) {
          pw.println(
              "data,totalPointNum,pagePointNum,numOfPagesInChunk,timeEncoding,valueType,valueEncoding,compressionType,"
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
              "data,totalPointNum,pagePointNum,numOfPagesInChunk,timeEncoding,valueType,valueEncoding,compressionType,"
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

        Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
        long totalStart = System.nanoTime();

        // 【1_index_read_deserialize_MagicString_FileMetadataSize】
        fileReader = new TsFileSequenceReader(tsfilePath, true, elapsedTimeInNanoSec);

        // 【2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter】
        MetadataQuerierByFileImpl metadataQuerier =
            new MetadataQuerierByFileImpl(fileReader, elapsedTimeInNanoSec);

        // instantiate an empty chunkCache
        CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileReader);
        // do nothing special
        //        TsFileExecutor tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
        // do nothing special
        //        tsFileReader = new TsFileReader(fileReader, metadataQuerier, chunkLoader,
        // tsFileExecutor);

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
          TsFileConstant.record(
              elapsedTimeInNanoSec, TsFileConstant.total_time, runTime, true, true);
        }

        System.out.println("read points: " + cnt);

        printEach(elapsedTimeInNanoSec, pw);
        printCategory(elapsedTimeInNanoSec, pw);
        printD2Detail(elapsedTimeInNanoSec, pw);

        System.out.println(
            "====================================data files====================================");
        if (expType.equals(ExpType.WRITE_REAL)) {
          System.out.println("csvFile: " + csvData);
          System.out.println(
              "csvFile size: " + df.format(new File(csvData).length() / 1024.0 / 1024.0) + "MB");
        }
        System.out.println("TsFile: " + tsfilePath);
        System.out.println(
            "TsFile size: " + df.format(new File(tsfilePath).length() / 1024.0 / 1024.0) + "MB");
        DecimalFormat formatter = new DecimalFormat("#,###");
        System.out.println("pointNum: " + formatter.format(cnt));

      } finally {
        if (fileReader != null) {
          fileReader.close();
        }
        if (pw != null) {
          pw.close();
        }
        System.out.println("current read point num: " + cnt);
      }
    }
  }

  public static long writeTsFile(
      ExpType expType,
      String csvData, // for WRITE_REAL only
      int pagePointNum,
      int numOfPagesInChunk,
      int chunksWritten, // for WRITE_SYNC only
      String timeEncoding,
      TSDataType valueDataType,
      TSEncoding valueEncoding,
      CompressionType compressionType)
      throws Exception {
    if (!expType.equals(ExpType.WRITE_REAL) && !expType.equals(ExpType.WRITE_SYNC)) {
      throw new IOException("Wrong write type!");
    }
    String tsfilePath;
    if (expType.equals(ExpType.WRITE_REAL)) {
      tsfilePath =
          "testTsFile"
              + File.separator
              + FilenameUtils.removeExtension(new File(csvData).getName()) // for WRITE_REAL only
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
    } else { // WRITE_SYNC
      tsfilePath =
          "testTsFile"
              + File.separator
              + "sync"
              + "_ppn_"
              + pagePointNum
              + "_pic_"
              + numOfPagesInChunk
              + "_cw_"
              + chunksWritten // for WRITE_SYNC only
              + "_vt_"
              + valueDataType
              + "_ve_"
              + valueEncoding
              + "_co_"
              + compressionType
              + "_"
              + System.currentTimeMillis()
              + ".tsfile";
    }

    File file = new File(tsfilePath);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }

    int chunkPointNum = pagePointNum * numOfPagesInChunk;
    long pointNum = 0;

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

    if (expType.equals(ExpType.WRITE_REAL)) { // read points from csv and write to TsFile
      try (BufferedReader br = new BufferedReader(new FileReader(csvData))) {
        br.readLine(); // skip header
        for (String line; (line = br.readLine()) != null; ) {
          pointNum++;
          String[] tv = line.split(",");
          long time = Long.parseLong(tv[0]); // get timestamp from real data
          int row = tablet.rowSize++;
          timestamps[row] = time;

          switch (valueDataType) {
            case INT32:
              int int_value = Integer.parseInt(tv[1]); // get value from real data
              int[] int_sensor = (int[]) values[0];
              int_sensor[row] = int_value;
              break;
            case INT64:
              long long_value = Long.parseLong(tv[1]); // get value from real data
              long[] long_sensor = (long[]) values[0];
              long_sensor[row] = long_value;
              break;
            case FLOAT:
              float float_value = Float.parseFloat(tv[1]); // get value from real data
              float[] float_sensor = (float[]) values[0];
              float_sensor[row] = float_value;
              break;
            case DOUBLE:
              double double_value = Double.parseDouble(tv[1]); // get value from real data
              double[] double_sensor = (double[]) values[0];
              double_sensor[row] = double_value;
              break;
            default:
              throw new IOException("not supported data type!");
          }

          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tablet.reset();
            tsFileWriter.flushAllChunkGroups();
          }
        }
      }
    } else { // WRITE_SYNC
      long rowNum = (long) chunkPointNum * chunksWritten; // 写数据点数
      long timestamp = 1;
      Random ran = new Random();
      for (long r = 0; r < rowNum; r++) {
        pointNum++;
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;

        switch (valueDataType) {
          case INT32:
            int[] int_sensor = (int[]) values[0];
            int_sensor[row] = ran.nextInt(100);
            break;
          case INT64:
            long[] long_sensor = (long[]) values[0];
            long_sensor[row] = ran.nextLong();
            break;
          case FLOAT:
            float[] float_sensor = (float[]) values[0];
            float_sensor[row] = ran.nextFloat();
            break;
          case DOUBLE:
            double[] double_sensor = (double[]) values[0];
            double_sensor[row] = ran.nextDouble();
            break;
          default:
            throw new IOException("not supported data type!");
        }

        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
          tsFileWriter
              .flushAllChunkGroups(); // 把chunkGroupSizeThreshold设够大，使得不会因为这个限制而flush，但是使用手动地提前flushAllChunkGroups来控制一个chunk里的数据量。
        }
      }
      System.out.println("current pointNum: " + pointNum);
    }
    // flush the last Tablet
    if (tablet.rowSize != 0) {
      tsFileWriter.write(tablet);
      tablet.reset();
    }
    tsFileWriter.flushAllChunkGroups();
    tsFileWriter.close();
    System.out.println(
        "====================================write parameters====================================");
    System.out.println("ExpType = " + expType);
    if (expType.equals(ExpType.WRITE_REAL)) {
      System.out.println("csvData = " + csvData);
    }
    System.out.println("pagePointNum = " + pagePointNum);
    System.out.println("numOfPagesInChunk = " + numOfPagesInChunk);
    if (expType.equals(ExpType.WRITE_SYNC)) {
      System.out.println("chunksWritten = " + chunksWritten);
    }
    System.out.println("time encoding = " + timeEncoding);
    System.out.println("value data type = " + valueDataType);
    System.out.println("value encoding = " + valueEncoding);
    System.out.println("compression = " + compressionType);
    System.out.println(
        "====================================write result====================================");
    System.out.println("output tsfilePath: " + tsfilePath);
    System.out.println("write points: " + pointNum);
    System.out.println("tsfile size: " + new File(tsfilePath).length() / 1024.0 / 1024.0 + " MB");
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
        "(B)load_on_disk_chunk = "
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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

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

public class RLTestChunkReadCostWithRealDataSet {

  // only consider one time series here
  public static String deviceName = "d1";
  public static String sensorName = "s1";
  public static Path mypath = new Path(deviceName, sensorName);

  // DecimalFormat df = new DecimalFormat("0.00");
  public static DecimalFormat df = new DecimalFormat("#,###.00");

  public static enum ExpType {
    WRITE_SYN, // write synthetic data
    WRITE_REAL, // write real data
    READ // read data
  }

  /**
   * Three kinds of arguments for writing synthetic data, writing real data, and reading data
   * respectively.
   *
   * <p>WRITE_SYN [pagePointNum] [numOfPagesInChunk] [chunksWritten] [timeEncoding] [valueDataType]
   * [valueEncoding] [compressionType]
   *
   * <p>WRITE_SYN 10000 1000 10 TS_2DIFF int32 PLAIN LZ4
   *
   * <p>WRITE_REAL [path_of_real_data_csv_to_write] [pagePointNum] [numOfPagesInChunk]
   * [timeEncoding] [valueDataType] [valueEncoding] [compressionType]
   *
   * <p>WRITE_REAL "G:\实验室电脑同步\iotdb\我的Gitbook基地\RUI Lei gitbook\ZC data\ZT17.csv" 10000 10 TS_2DIFF
   * FLOAT RLE LZ4
   *
   * <p>READ [path_of_tsfile_to_read] [decomposeMeasureTime] [D_decompose_each_step]
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
      case "WRITE_SYN": // WRITE_SYN [pagePointNum] [numOfPagesInChunk] [chunksWritten]
        // [valueDataType] [valueEncoding] [compressionType]
        expType = ExpType.WRITE_SYN;
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
                ExpType.WRITE_SYN,
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
        TsFileConstant.D_decompose_each_step_further = Boolean.parseBoolean(args[3]);
        break;
      default:
        throw new IOException("Wrong ExpType. Only accept WRITE_SYN/WRITE_REAL/READ");
    }

    // ==============read tsfile test==============
    if (expType.equals(ExpType.READ)) {
      long cnt = 0;
      TsFileSequenceReader fileReader = null;
      PrintWriter pw = null;
      String resultCsvName = "";
      try {
        if (!TsFileConstant.decomposeMeasureTime) {
          resultCsvName = "total";
        } else if (!TsFileConstant.D_decompose_each_step_further) {
          resultCsvName = "category";
        } else {
          resultCsvName = "furtherD";
        }
        resultCsvName =
            "testTsFile"
                + File.separator
                + FilenameUtils.removeExtension(new File(tsfilePath).getName())
                + "-readResult-"
                + resultCsvName
                + "-"
                + System.nanoTime()
                + ".csv";
        pw = new PrintWriter(resultCsvName);

        Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
        long totalStart = System.nanoTime();

        // 【1_index_read_deserialize_MagicString_FileMetadataSize】
        fileReader = new TsFileSequenceReader(tsfilePath, true, elapsedTimeInNanoSec);

        // 【2_index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter】
        MetadataQuerierByFileImpl metadataQuerier =
            new MetadataQuerierByFileImpl(fileReader, elapsedTimeInNanoSec);

        // 【3_2_index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet】
        List<IChunkMetadata> chunkMetadataList =
            metadataQuerier.getChunkMetaDataList(mypath, elapsedTimeInNanoSec);

        for (IChunkMetadata chunkMetadata : chunkMetadataList) {
          // 【4_data_read_deserialize_ChunkHeader】
          // 【5_data_read_ChunkData】
          Chunk chunk = fileReader.readMemChunk((ChunkMetadata) chunkMetadata);

          // 【6_data_deserialize_PageHeader】
          // 【7_data_decompress_PageData】
          ChunkReader chunkReader = new ChunkReader(chunk, null, elapsedTimeInNanoSec);

          // 【8_data_decode_time_value_Buffer】
          while (chunkReader.hasNextSatisfiedPage()) {
            BatchData batchData = chunkReader.nextPageData();
            cnt += batchData.length();
          }
        }

        long runTime = System.nanoTime() - totalStart;
        if (!TsFileConstant.decomposeMeasureTime) {
          TsFileConstant.record(
              elapsedTimeInNanoSec, TsFileConstant.total_time, runTime, true, true);
        }

        System.out.println("read points: " + cnt);

        printEach(elapsedTimeInNanoSec, pw);
        printCategory(elapsedTimeInNanoSec);
        printDDetail(elapsedTimeInNanoSec);

        System.out.println(
            "====================================data files====================================");
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
          System.out.println("read elapsed time result is stored in: " + resultCsvName);
        }
        System.out.println("current read point num: " + cnt);
      }
    }
  }

  public static void getTsfileSpaceStatistics(String tsfilePath, PrintWriter pw) throws Exception {
    System.out.println(
        "====================================tsfile space statistics====================================");
    TsFileConstant.decomposeMeasureTime = false;
    DescriptiveStatistics chunkDataSize_stats = new DescriptiveStatistics();
    DescriptiveStatistics compressedPageSize_stats = new DescriptiveStatistics();
    DescriptiveStatistics uncompressedPageSize_stats = new DescriptiveStatistics();
    DescriptiveStatistics timeBufferSize_stats = new DescriptiveStatistics();
    DescriptiveStatistics valueBufferSize_stats = new DescriptiveStatistics();
    Map<String, List<Long>> elapsedTimeInNanoSec = new TreeMap<>();
    try (TsFileSequenceReader fileReader =
        new TsFileSequenceReader(tsfilePath, true, elapsedTimeInNanoSec)) {
      MetadataQuerierByFileImpl metadataQuerier =
          new MetadataQuerierByFileImpl(fileReader, elapsedTimeInNanoSec);
      List<IChunkMetadata> chunkMetadataList =
          metadataQuerier.getChunkMetaDataList(mypath, elapsedTimeInNanoSec);
      int m = chunkMetadataList.size();
      for (int j = 0;
          j < m - 1;
          j++) { // let alone the last chunk which might not be full, assuming more than one chunk
        IChunkMetadata chunkMetadata = chunkMetadataList.get(j);
        Chunk chunk = fileReader.readMemChunk((ChunkMetadata) chunkMetadata);
        chunkDataSize_stats.addValue(chunk.getHeader().getDataSize());
        ChunkReader chunkReader = new ChunkReader(chunk, null, elapsedTimeInNanoSec);
        List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
        for (IPageReader pageReader : pageReaderList) {
          compressedPageSize_stats.addValue(
              ((PageReader) pageReader).getPageHeader().getCompressedSize());
          uncompressedPageSize_stats.addValue(
              ((PageReader) pageReader).getPageHeader().getUncompressedSize());
          timeBufferSize_stats.addValue(((PageReader) pageReader).getTimeBufferSize());
          valueBufferSize_stats.addValue(((PageReader) pageReader).getValueBufferSize());
        }
      }
    } finally {
      // System.out.println("tsfile size: " + df.format(new File(tsfilePath).length() / 1024.0 /
      // 1024.0) + "MB");
      printStats("chunkDataSize_stats", chunkDataSize_stats, "MB", pw);
      printStats("compressedPageSize_stats", compressedPageSize_stats, "B", pw);
      printStats("uncompressedPageSize_stats", uncompressedPageSize_stats, "B", pw);
      printStats("timeBufferSize_stats", timeBufferSize_stats, "B", pw);
      printStats("valueBufferSize_stats", valueBufferSize_stats, "B", pw);
    }
  }

  public static void printStats(
      String name, DescriptiveStatistics stats, String unit, PrintWriter pw) throws Exception {
    unit = unit.toUpperCase();
    double unitConvert = 1;
    switch (unit) {
      case "GB":
        unitConvert = 1024 * 1024 * 1024.0;
        break;
      case "MB":
        unitConvert = 1024 * 1024.0;
        break;
      case "KB":
        unitConvert = 1024.0;
        break;
      case "B":
        // unitConvert = 1;
        break;
      default:
        throw new IOException("Wrong unit!");
    }

    // long num = stats.getN();
    double max = stats.getMax() / unitConvert;
    double min = stats.getMin() / unitConvert;
    double mean = stats.getMean() / unitConvert;
    double std = stats.getStandardDeviation() / unitConvert;
    double p25 = stats.getPercentile(25) / unitConvert;
    double p50 = stats.getPercentile(50) / unitConvert;
    double p75 = stats.getPercentile(75) / unitConvert;
    double p90 = stats.getPercentile(90) / unitConvert;
    double p95 = stats.getPercentile(95) / unitConvert;
    // double sum = stats.getSum() / 1024.0;

    pw.print(name + "_mean(" + unit + "),");
    pw.println(mean); // note that print to csv cannot use the df format with comma inside

    System.out.println(
        name
            + ": "
            // + "num=" + num + ", " + "sum=" + sum + "KB, "  // num and sum are not accurate
            // because I let alone the last chunk
            + "mean="
            + df.format(mean)
            + unit
            + ", "
            + "min="
            + df.format(min)
            + unit
            + ", "
            + "max="
            + df.format(max)
            + unit
            + ", "
            + "std="
            + df.format(std)
            + unit
            + ", "
            + "p25="
            + df.format(p25)
            + unit
            + ", "
            + "p50="
            + df.format(p50)
            + unit
            + ", "
            + "p75="
            + df.format(p75)
            + unit
            + ", "
            + "p90="
            + df.format(p90)
            + unit
            + ", "
            + "p95="
            + df.format(p95)
            + unit);
  }

  public static long writeTsFile(
      ExpType expType,
      String csvData, // for WRITE_REAL only
      int pagePointNum,
      int numOfPagesInChunk,
      int chunksWritten, // for WRITE_SYN only
      String timeEncoding,
      TSDataType valueDataType,
      TSEncoding valueEncoding,
      CompressionType compressionType)
      throws Exception {
    if (!expType.equals(ExpType.WRITE_REAL) && !expType.equals(ExpType.WRITE_SYN)) {
      throw new IOException("Wrong write type!");
    }
    String tsfilePath;
    String resultPath;
    if (expType.equals(ExpType.WRITE_REAL)) {
      String name =
          "testTsFile"
              + File.separator
              + FilenameUtils.removeExtension(new File(csvData).getName()) // for WRITE_REAL only
              + "_ppn_"
              + pagePointNum
              + "_pic_"
              + numOfPagesInChunk
              + "_te_"
              + timeEncoding
              + "_vt_"
              + valueDataType
              + "_ve_"
              + valueEncoding
              + "_co_"
              + compressionType
          // + "_"
          // + System.currentTimeMillis()
          ;
      tsfilePath = name + ".tsfile";
      resultPath = name + "-writeResult.csv";
    } else { // WRITE_SYN
      String name =
          "testTsFile"
              + File.separator
              + "syn"
              + "_ppn_"
              + pagePointNum
              + "_pic_"
              + numOfPagesInChunk
              + "_cw_"
              + chunksWritten // for WRITE_SYN only
              + "_te_"
              + timeEncoding
              + "_vt_"
              + valueDataType
              + "_ve_"
              + valueEncoding
              + "_co_"
              + compressionType
          // + "_"
          // + System.currentTimeMillis()
          ;
      tsfilePath = name + ".tsfile";
      resultPath = name + "-writeResult.csv";
    }

    File file = new File(tsfilePath);
    file.delete();
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
    try (TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig)) {
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
        chunksWritten = 0;
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
              chunksWritten++;
            }
          }
        }
      } else { // WRITE_SYN
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
      }
      // flush the last Tablet for WRITE_REAL. WRITE_SYN will not have data points left here.
      if (tablet.rowSize != 0) {
        tsFileWriter.write(tablet);
        tablet.reset();
        tsFileWriter.flushAllChunkGroups();
        chunksWritten++;
      }
      // closing tsFileWriter is handled by try-with-resources
    } finally {
      try (PrintWriter pw = new PrintWriter(resultPath)) {
        /*
        pw.println(
            "dataset,pagePointNum(ppn),numOfPagesInChunk(pic),chunksWritten(cw),"
                + "timeEncoding(te),valueDataType(vt),valueEncoding(ve),compression(co),"
                + "TotalPointNum, TsfileSize(MB),AvgChunkDataSize(MB),AvgCompressedPageSize(Bytes),"
                + "AvgUncompressedPageSize(Bytes),AvgTimeBufferSize(Bytes),AvgValueBufferSize(Bytes)");
         */

        System.out.println(
            "====================================write parameters====================================");
        System.out.println("ExpType = " + expType);
        if (expType.equals(ExpType.WRITE_REAL)) {
          System.out.println("csvData = " + csvData);
          System.out.println("pagePointNum(ppn) = " + pagePointNum);
          System.out.println("numOfPagesInChunk(pic) = " + numOfPagesInChunk);
          System.out.println("(result)chunksWritten(cw) = " + chunksWritten);
          System.out.println("timeEncoding(te) = " + timeEncoding);
          System.out.println("valueDataType(vt) = " + valueDataType);
          System.out.println("valueEncoding(ve) = " + valueEncoding);
          System.out.println("compression(co) = " + compressionType);

          pw.print("dataset,");
          pw.println(csvData);
          pw.print("pagePointNum(ppn),");
          pw.println(pagePointNum);
          pw.print("numOfPagesInChunk(pic),");
          pw.println(numOfPagesInChunk);
          pw.print("(result)chunksWritten(cw),");
          pw.println(chunksWritten);
          pw.print("timeEncoding(te),");
          pw.println(timeEncoding);
          pw.print("valueDataType(vt),");
          pw.println(valueDataType);
          pw.print("valueEncoding(ve),");
          pw.println(valueEncoding);
          pw.print("compression(co),");
          pw.println(compressionType);
        } else {
          System.out.println("pagePointNum(ppn) = " + pagePointNum);
          System.out.println("numOfPagesInChunk(pic) = " + numOfPagesInChunk);
          System.out.println("chunksWritten(cw) = " + chunksWritten);
          System.out.println("timeEncoding(te) = " + timeEncoding);
          System.out.println("valueDataType(vt) = " + valueDataType);
          System.out.println("valueEncoding(ve) = " + valueEncoding);
          System.out.println("compression(co) = " + compressionType);

          pw.println("dataset,synthetic");
          pw.print("pagePointNum(ppn),");
          pw.println(pagePointNum);
          pw.print("numOfPagesInChunk(pic),");
          pw.println(numOfPagesInChunk);
          pw.print("chunksWritten(cw),");
          pw.println(chunksWritten);
          pw.print("timeEncoding(te),");
          pw.println(timeEncoding);
          pw.print("valueDataType(vt),");
          pw.println(valueDataType);
          pw.print("valueEncoding(ve),");
          pw.println(valueEncoding);
          pw.print("compression(co),");
          pw.println(compressionType);
        }

        System.out.println(
            "====================================write result====================================");
        System.out.println("output tsfilePath: " + tsfilePath);
        System.out.println("write points: " + pointNum);
        System.out.println(
            "tsfile size: " + df.format(new File(tsfilePath).length() / 1024.0 / 1024.0) + " MB");

        pw.print("totalPointNum,");
        pw.println(pointNum);
        pw.print("tsfileSize(MB),");
        pw.println(new File(tsfilePath).length() / 1024.0 / 1024.0);

        // calculate file, chunk, and page size information
        getTsfileSpaceStatistics(tsfilePath, pw);
      }
    }

    return pointNum;
  }

  public static void printEach(Map<String, List<Long>> elapsedTimeInNanoSec, PrintWriter pw) {
    System.out.println(
        "====================================[1] each step====================================");
    double cumulativeSteps = 0;
    for (Map.Entry<String, List<Long>> entry : elapsedTimeInNanoSec.entrySet()) {
      String key = entry.getKey();
      List<Long> elapsedTimes = entry.getValue();
      System.out.print(key + ":");
      pw.print(key);
      pw.print("(us),");
      double sum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (Long t : elapsedTimes) {
        sum += t / 1000.0;
        stringBuilder.append(t / 1000.0).append(", ");
      }
      System.out.print("[" + df.format(sum) + "us(SUM)," + elapsedTimes.size() + "(CNT)]:");
      System.out.println();
      //      System.out.println(stringBuilder.toString());
      pw.print(sum);
      pw.println();

      cumulativeSteps += sum;
    }
    System.out.println("sum 1-8: " + df.format(cumulativeSteps) + "us");
  }

  public static void printCategory(Map<String, List<Long>> elapsedTimeInNanoSec) {
    if (!TsFileConstant.decomposeMeasureTime) {
      return;
    }
    System.out.println(
        "======[2] category results, following the logic: (A)ChunkDigest->(B)load chunk (from disk)->(C)PageDigest->(D)load page (in memory)=====");
    double A_get_chunkMetadatas = 0;
    double B_load_on_disk_chunk = 0;
    double C_get_pageHeader = 0;
    double D_1_decompress_pageData = 0;
    double D_2_decode_pageData = 0;
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
      if (key.equals(TsFileConstant.data_decompress_PageData)
          || key.equals(TsFileConstant.D_1_data_ByteBuffer_to_ByteArray)
          || key.equals(TsFileConstant.D_1_data_decompress_PageDataByteArray)
          || key.equals(TsFileConstant.D_1_data_ByteArray_to_ByteBuffer)
          || key.equals(TsFileConstant.D_1_data_split_time_value_Buffer)) {
        D_1_decompress_pageData += sum;
      }
      if (key.equals(TsFileConstant.data_decode_time_value_Buffer)
          || key.equals(TsFileConstant.D_2_createBatchData)
          || key.equals(TsFileConstant.D_2_timeDecoder_hasNext)
          || key.equals(TsFileConstant.D_2_timeDecoder_readLong)
          || key.equals(TsFileConstant.D_2_valueDecoder_read)
          || key.equals(TsFileConstant.D_2_checkValueSatisfyOrNot)
          || key.equals(TsFileConstant.D_2_putIntoBatchData)) {
        D_2_decode_pageData += sum;
      }
    }
    double cumulativeSteps = 0;
    cumulativeSteps += A_get_chunkMetadatas;
    cumulativeSteps += B_load_on_disk_chunk;
    cumulativeSteps += C_get_pageHeader;
    cumulativeSteps += D_1_decompress_pageData;
    cumulativeSteps += D_2_decode_pageData;

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
            + df.format(D_1_decompress_pageData)
            + "us, "
            + df.format(D_1_decompress_pageData / cumulativeSteps * 100)
            + "%");
    System.out.println(
        "(D_2)decode_pageData_point_by_point = "
            + df.format(D_2_decode_pageData)
            + "us, "
            + df.format(D_2_decode_pageData / cumulativeSteps * 100)
            + "%");
    System.out.println("sum 1-8: " + df.format(cumulativeSteps) + "us");
  }

  public static void printDDetail(Map<String, List<Long>> elapsedTimeInNanoSec) {
    if (!TsFileConstant.decomposeMeasureTime || !TsFileConstant.D_decompose_each_step_further) {
      return;
    }

    System.out.println(
        "====================================[3] D_1 compare each step inside====================================");
    String[] keys =
        new String[] {
          TsFileConstant.D_1_data_ByteBuffer_to_ByteArray,
          TsFileConstant.D_1_data_decompress_PageDataByteArray,
          TsFileConstant.D_1_data_ByteArray_to_ByteBuffer,
          TsFileConstant.D_1_data_split_time_value_Buffer
        };
    double[] times = new double[keys.length];
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      List<Long> timeList = elapsedTimeInNanoSec.get(key);
      double sum = 0;
      for (long t : timeList) {
        sum += t / 1000.0; // us
      }
      times[i] = sum;
    }
    double total_D1 = Arrays.stream(times).sum();
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      System.out.println(
          key + ": " + df.format(times[i]) + "us, " + df.format(times[i] * 100.0 / total_D1) + "%");
    }

    System.out.println(
        "====================================[3] D_2 compare each step inside====================================");
    long total_D2 =
        elapsedTimeInNanoSec.get(TsFileConstant.D_2_createBatchData).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_hasNext).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_timeDecoder_readLong).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_valueDecoder_read).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_checkValueSatisfyOrNot).get(0)
            + elapsedTimeInNanoSec.get(TsFileConstant.D_2_putIntoBatchData).get(0);
    keys =
        new String[] {
          TsFileConstant.D_2_createBatchData,
          TsFileConstant.D_2_timeDecoder_hasNext,
          TsFileConstant.D_2_timeDecoder_readLong,
          TsFileConstant.D_2_valueDecoder_read,
          TsFileConstant.D_2_checkValueSatisfyOrNot,
          TsFileConstant.D_2_putIntoBatchData
        };
    for (String key : keys) {
      System.out.println(
          key
              + ": "
              + df.format(elapsedTimeInNanoSec.get(key).get(0) / 1000.0)
              + "us, "
              + df.format(elapsedTimeInNanoSec.get(key).get(0) * 100.0 / total_D2)
              + "%");
    }
  }
}

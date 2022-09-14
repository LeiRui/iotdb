package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

public class RLRepeatReadResultAvgPercCalculator {

  public static void main(String[] args) throws IOException {
    String inputFile = args[0];
    Map<String, DescriptiveStatistics> elapsedTimeInMicroSec = new TreeMap<>();
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        // 计算每一项操作的平均值
        if (line.startsWith("(A)")
            || line.startsWith("(B)")
            || line.startsWith("(C)")
            || line.startsWith("(D-1)")
            || line.startsWith("(D-2)")) {
          String[] items = line.split(",");
          String key = items[0];
          int repeatNum = items.length - 1; // 读实验重复次数
          if (!elapsedTimeInMicroSec.containsKey(key)) {
            elapsedTimeInMicroSec.put(key, new DescriptiveStatistics());
          }
          for (int i = 1; i <= repeatNum; i++) {
            elapsedTimeInMicroSec.get(key).addValue(Double.parseDouble(items[i]));
          }
        }
      }
    }
    //    System.out.println(elapsedTimeInMicroSec); // 获得各项操作的各次重复实验的耗时

    double totalTime = 0;
    double D1_totalTime = 0;
    double D2_totalTime = 0;
    for (Map.Entry<String, DescriptiveStatistics> entry : elapsedTimeInMicroSec.entrySet()) {
      totalTime += entry.getValue().getMean();
      if (entry.getKey().startsWith("(D-1)")) {
        D1_totalTime += entry.getValue().getMean();
      }
      if (entry.getKey().startsWith("(D-2)")) {
        D2_totalTime += entry.getValue().getMean();
      }
    }

    try (FileWriter fileWriter = new FileWriter(inputFile, true);
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      double A_get_chunkMetadatas = 0;
      double B_load_on_disk_chunk = 0;
      double C_get_pageHeader = 0;
      double D_1_decompress_pageData = 0;
      double D_2_decode_pageData = 0;

      // 把各项操作的平均值和百分比追加到输入文件尾部，并按照A/B/C/D-1/D-2的分类统计
      printWriter.println();
      printWriter.println("----[1] each step----");
      for (Map.Entry<String, DescriptiveStatistics> entry : elapsedTimeInMicroSec.entrySet()) {
        String key = entry.getKey();
        double mean = entry.getValue().getMean();
        printAvgPer(printWriter, mean, totalTime, key);

        if (key.startsWith(TsFileConstant.index_read_deserialize_MagicString_FileMetadataSize)
            || key.startsWith(
                TsFileConstant.index_read_deserialize_IndexRootNode_MetaOffset_BloomFilter)
            || key.startsWith(
                TsFileConstant
                    .index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forCacheWarmUp)
            || key.startsWith(
                TsFileConstant
                    .index_read_deserialize_IndexRootNode_exclude_to_TimeseriesMetadata_forExactGet)) {
          A_get_chunkMetadatas += mean;
        }
        if (key.startsWith(TsFileConstant.data_read_deserialize_ChunkHeader)
            || key.startsWith(TsFileConstant.data_read_ChunkData)) {
          B_load_on_disk_chunk += mean;
        }
        if (key.startsWith(TsFileConstant.data_deserialize_PageHeader)) {
          C_get_pageHeader += mean;
        }
        if (key.startsWith(TsFileConstant.data_decompress_PageData)
            || key.startsWith(TsFileConstant.D_1_data_ByteBuffer_to_ByteArray)
            || key.startsWith(TsFileConstant.D_1_data_decompress_PageDataByteArray)
            || key.startsWith(TsFileConstant.D_1_data_ByteArray_to_ByteBuffer)
            || key.startsWith(TsFileConstant.D_1_data_split_time_value_Buffer)) {
          D_1_decompress_pageData += mean;
        }
        if (key.startsWith(TsFileConstant.data_decode_time_value_Buffer)
            || key.startsWith(TsFileConstant.D_2_createBatchData)
            || key.startsWith(TsFileConstant.D_2_timeDecoder_hasNext)
            || key.startsWith(TsFileConstant.D_2_timeDecoder_readLong)
            || key.startsWith(TsFileConstant.D_2_valueDecoder_read)
            || key.startsWith(TsFileConstant.D_2_checkValueSatisfyOrNot)
            || key.startsWith(TsFileConstant.D_2_putIntoBatchData)) {
          D_2_decode_pageData += mean;
        }
      }

      // 把A/B/C/D-1/D-2的分类统计的平均值和百分比追加到输入文件尾部
      printWriter.println();
      printWriter.println(
          "----[2] category: (A)get ChunkStatistic->(B)load on-disk Chunk->(C)get PageStatistics->(D)load in-memory PageData----");

      printAvgPer(printWriter, A_get_chunkMetadatas, totalTime, "(A)get_chunkMetadatas");
      printAvgPer(printWriter, B_load_on_disk_chunk, totalTime, "(B)load_on_disk_chunk");
      printAvgPer(printWriter, C_get_pageHeader, totalTime, "(C)get_pageHeader");
      printAvgPer(printWriter, D_1_decompress_pageData, totalTime, "(D_1)decompress_pageData");
      printAvgPer(printWriter, D_2_decode_pageData, totalTime, "(D_2)decode_pageData");

      // 把D-1内部操作的平均值和百分比追加到输入文件尾部
      printWriter.println();
      printWriter.println("----[3] D_1 compare each step inside----");
      for (Map.Entry<String, DescriptiveStatistics> entry : elapsedTimeInMicroSec.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith("(D-1)")) {
          double mean = entry.getValue().getMean();
          printAvgPer(printWriter, mean, D1_totalTime, key);
        }
      }

      // 把D-2内部操作的平均值和百分比追加到输入文件尾部
      printWriter.println();
      printWriter.println("----[3] D_2 compare each step inside----");
      for (Map.Entry<String, DescriptiveStatistics> entry : elapsedTimeInMicroSec.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith("(D-2)")) {
          double mean = entry.getValue().getMean();
          printAvgPer(printWriter, mean, D2_totalTime, key);
        }
      }
    }
  }

  public static void printAvgPer(
      PrintWriter printWriter, double time, double totalTime, String key) {
    printWriter.print("[Avg&Per] " + key);
    printWriter.print(",");
    printWriter.print(time);
    printWriter.print(",");
    printWriter.print(100.0 * time / totalTime);
    printWriter.print(",");
    printWriter.print(time);
    printWriter.print(" us - ");
    printWriter.print(100.0 * time / totalTime);
    printWriter.println("%");
  }
}

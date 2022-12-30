package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.commons.service.metric.enums.Operation;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

public class IOMonitor {

  public static int DCP_A_GET_CHUNK_METADATAS_count = 0;
  public static long DCP_A_GET_CHUNK_METADATAS_ns = 0;

  public static int DCP_B_READ_MEM_CHUNK_count = 0;
  public static long DCP_B_READ_MEM_CHUNK_ns = 0;

  public static int DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count = 0;
  public static long DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns = 0;

  public static int DCP_D_DECODE_PAGEDATA_count = 0;
  public static long DCP_D_DECODE_PAGEDATA_ns = 0;

  public static int DCP_SeriesScanOperator_hasNext_count = 0;
  public static long DCP_SeriesScanOperator_hasNext_ns = 0;

  public static int DCP_Server_Query_Execute_count = 0;
  public static long DCP_Server_Query_Execute_ns = 0;

  public static int DCP_Server_Query_Fetch_count = 0;
  public static long DCP_Server_Query_Fetch_ns = 0;

  public static int DCP_LongDeltaDecoder_loadIntBatch_count = 0;
  public static long DCP_LongDeltaDecoder_loadIntBatch_ns = 0;

  public static void addMeasure(Operation operation, long elapsedTimeInNanosecond) {
    switch (operation) {
      case DCP_A_GET_CHUNK_METADATAS:
        DCP_A_GET_CHUNK_METADATAS_count++;
        DCP_A_GET_CHUNK_METADATAS_ns += elapsedTimeInNanosecond;
        break;
      case DCP_B_READ_MEM_CHUNK:
        DCP_B_READ_MEM_CHUNK_count++;
        DCP_B_READ_MEM_CHUNK_ns += elapsedTimeInNanosecond;
        break;
      case DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA:
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count++;
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns += elapsedTimeInNanosecond;
        break;
      case DCP_D_DECODE_PAGEDATA:
        DCP_D_DECODE_PAGEDATA_count++;
        DCP_D_DECODE_PAGEDATA_ns += elapsedTimeInNanosecond;
        break;
      case DCP_SeriesScanOperator_hasNext:
        DCP_SeriesScanOperator_hasNext_count++;
        DCP_SeriesScanOperator_hasNext_ns += elapsedTimeInNanosecond;
        break;
      case DCP_Server_Query_Execute:
        DCP_Server_Query_Execute_count++;
        DCP_Server_Query_Execute_ns += elapsedTimeInNanosecond;
        break;
      case DCP_Server_Query_Fetch:
        DCP_Server_Query_Fetch_count++;
        DCP_Server_Query_Fetch_ns += elapsedTimeInNanosecond;
        break;
      case DCP_LongDeltaDecoder_loadIntBatch:
        DCP_LongDeltaDecoder_loadIntBatch_count++;
        DCP_LongDeltaDecoder_loadIntBatch_ns += elapsedTimeInNanosecond;
        break;
      default:
        System.out.println("not supported operation type"); // this will not happen
        break;
    }
  }

  public static void addMeasure_loadIntBatch(long elapsedTimeInNanosecond, int count) {
    DCP_LongDeltaDecoder_loadIntBatch_count += count;
    DCP_LongDeltaDecoder_loadIntBatch_ns += elapsedTimeInNanosecond;
  }

  public static String print() {
    StringBuilder stringBuilder = new StringBuilder();

    stringBuilder.append("A_cnt").append(",").append(DCP_A_GET_CHUNK_METADATAS_count).append("\n");
    stringBuilder.append("B_cnt").append(",").append(DCP_B_READ_MEM_CHUNK_count).append("\n");
    stringBuilder
        .append("C_cnt")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count)
        .append("\n");
    stringBuilder.append("D_cnt").append(",").append(DCP_D_DECODE_PAGEDATA_count).append("\n");
    stringBuilder
        .append("LongDeltaDecoder_loadIntBatch_cnt")
        .append(",")
        .append(DCP_LongDeltaDecoder_loadIntBatch_count)
        .append("\n");
    stringBuilder
        .append("SeriesScanOperator_hasNext_cnt")
        .append(",")
        .append(DCP_SeriesScanOperator_hasNext_count)
        .append("\n");
    stringBuilder
        .append("Server_Query_Execute_cnt")
        .append(",")
        .append(DCP_Server_Query_Execute_count)
        .append("\n");
    stringBuilder
        .append("Server_Query_Fetch_cnt")
        .append(",")
        .append(DCP_Server_Query_Fetch_count)
        .append("\n");

    stringBuilder.append("A_ns").append(",").append(DCP_A_GET_CHUNK_METADATAS_ns).append("\n");
    stringBuilder.append("B_ns").append(",").append(DCP_B_READ_MEM_CHUNK_ns).append("\n");
    stringBuilder
        .append("C_ns")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns)
        .append("\n");
    stringBuilder.append("D_ns").append(",").append(DCP_D_DECODE_PAGEDATA_ns).append("\n");
    stringBuilder
        .append("LongDeltaDecoder_loadIntBatch_ns")
        .append(",")
        .append(DCP_LongDeltaDecoder_loadIntBatch_ns)
        .append("\n");
    stringBuilder
        .append("SeriesScanOperator_hasNext_ns")
        .append(",")
        .append(DCP_SeriesScanOperator_hasNext_ns)
        .append("\n");
    stringBuilder
        .append("Server_Query_Execute_ns")
        .append(",")
        .append(DCP_Server_Query_Execute_ns)
        .append("\n");
    stringBuilder
        .append("Server_Query_Fetch_ns")
        .append(",")
        .append(DCP_Server_Query_Fetch_ns)
        .append("\n");

    stringBuilder
        .append("ts2diff_equal")
        .append(",")
        .append(TsFileConstant.ts2diff_equal)
        .append("\n");
    stringBuilder
        .append("ts2diff_notequal")
        .append(",")
        .append(TsFileConstant.ts2diff_notEqual)
        .append("\n");

    return stringBuilder.toString();
  }
}

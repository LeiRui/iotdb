/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.reader.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

public class PageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /**
   * decoder for value column
   */
  protected Decoder valueDecoder;

  /**
   * decoder for time column
   */
  protected Decoder timeDecoder;

  /**
   * time column in memory
   */
  protected ByteBuffer timeBuffer;

  /**
   * value column in memory
   */
  protected ByteBuffer valueBuffer;

  protected Filter filter;

  /**
   * A list of deleted intervals.
   */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public PageReader(
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /**
   * @return the returned BatchData may be empty, but never be null
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);

    long[] timeData = ((LongDeltaDecoder) timeDecoder).getDataArray4CPV(timeBuffer);
    for (long timestamp : timeData) {
      // TODO delay the decode of value until the timestamp is valid, skip to the next point when t
      // is invalid
      long aLong =
          valueDecoder.readLong(valueBuffer); // hard-coded, assuming value is long data type

      if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, null))) {
        // cannot remove filter here because M4-UDF uses time filters, but we can delay the use of
        // value object
        // assuming the filter is always timeFilter

        pageData.putLong(timestamp, aLong);
      }
    }

    return pageData.flip();

//    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
//    if (filter == null || filter.satisfy(getStatistics())) {
//      while (timeDecoder.hasNext(timeBuffer)) {
//        long timestamp = timeDecoder.readLong(timeBuffer);
//        switch (dataType) {
//          case BOOLEAN:
//            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean))) {
//              pageData.putBoolean(timestamp, aBoolean);
//            }
//            break;
//          case INT32:
//            int anInt = valueDecoder.readInt(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
//              pageData.putInt(timestamp, anInt);
//            }
//            break;
//          case INT64:
//            long aLong = valueDecoder.readLong(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
//              pageData.putLong(timestamp, aLong);
//            }
//            break;
//          case FLOAT:
//            float aFloat = valueDecoder.readFloat(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
//              pageData.putFloat(timestamp, aFloat);
//            }
//            break;
//          case DOUBLE:
//            double aDouble = valueDecoder.readDouble(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
//              pageData.putDouble(timestamp, aDouble);
//            }
//            break;
//          case TEXT:
//            Binary aBinary = valueDecoder.readBinary(valueBuffer);
//            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
//              pageData.putBinary(timestamp, aBinary);
//            }
//            break;
//          default:
//            throw new UnSupportedDataTypeException(String.valueOf(dataType));
//        }
//      }
//    }
//    return pageData.flip();
  }

  /**
   * 负责当候选点因为M4 time span/删除/更新而失效而要去update的时候的update。 它会遍历这个page里的点，对取出来的点进行删除和过滤条件判断，并且按照M4 time
   * spans拆分，变成落进相应的span里的batchData和chunkMetadata。这个过程中，解读出来的batchData就会缓存下来。
   */
  public void split4CPV(
      long startTime,
      long endTime,
      long interval,
      long curStartTime,
      List<ChunkSuit4CPV> currentChunkList,
      Map<Integer, List<ChunkSuit4CPV>> splitChunkList,
      ChunkMetadata chunkMetadata) { // note: [startTime,endTime), [curStartTime,curEndTime)
    Map<Integer, BatchData> splitBatchDataMap = new HashMap<>();
    Map<Integer, ChunkMetadata> splitChunkMetadataMap = new HashMap<>();
    long[] timeData = ((LongDeltaDecoder) timeDecoder).getDataArray4CPV(timeBuffer);
    for (long timestamp : timeData) {
      // prepare corresponding batchData
      if (timestamp < curStartTime) {
        // TODO delay the decode of value until the timestamp is valid, skip to the next point when
        // t is invalid
        valueDecoder.readLong(valueBuffer); // hard-coded, assuming value is long data type
        continue;
      }
      if (timestamp >= endTime) {
        break;
      }
      int idx = (int) Math.floor((timestamp - startTime) * 1.0 / interval);
      if (!splitBatchDataMap.containsKey(idx)) {
        // create batchData
        BatchData batch1 = BatchDataFactory.createBatchData(dataType, true, false);
        splitBatchDataMap.put(idx, batch1);
        LongStatistics statistics =
            new LongStatistics(); // hard-coded, assuming value is long data type
        // create chunkMetaData
        ChunkMetadata chunkMetadata1 =
            new ChunkMetadata(
                chunkMetadata.getMeasurementUid(),
                chunkMetadata.getDataType(),
                chunkMetadata.getOffsetOfChunkHeader(),
                statistics);
        chunkMetadata1.setVersion(chunkMetadata.getVersion()); // don't miss this
        splitChunkMetadataMap.put(idx, chunkMetadata1);
      }
      BatchData batchData1 = splitBatchDataMap.get(idx);
      ChunkMetadata chunkMetadata1 = splitChunkMetadataMap.get(idx);

      // TODO delay the decode of value until the timestamp is valid, skip to the next point when t
      // is invalid
      // hard-coded, assuming value is long data type
      long aLong = valueDecoder.readLong(valueBuffer);

      if (!isDeleted(timestamp)) {
        // remove filter, only check delete, because groupByFilter is handled in this function's own
        // logic

        // update batchData1
        batchData1.putLong(timestamp, aLong);
        // update statistics of chunkMetadata1
        chunkMetadata1.getStatistics().update(timestamp, aLong);
      }
    }

    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval);
    for (Integer i : splitBatchDataMap.keySet()) {
      if (!splitBatchDataMap.get(i).isEmpty()) {
        if (i == curIdx) {
          currentChunkList.add(
              new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
        } else {
          splitChunkList.computeIfAbsent(i, k -> new ArrayList<>());
          splitChunkList
              .get(i)
              .add(
                  new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
        }
      }
    }
  }

  /**
   * 负责BP/TP的candidate update verification，对与候选点有时间重叠且版本更高的块，它会从前到后逐点判断，
   * 直到找到等于候选点时间戳的点，或者直接大于候选点时间戳说明不存在更新。
   * 这个过程中，只涉及到timeDecoder读取的那一个pack出来的timeData数组会被缓存下来。
   */
  public boolean partialScan4CPV(long candidateTimestamp) {
    long[] timeData = ((LongDeltaDecoder) timeDecoder).getDataArray4CPV(timeBuffer);
    for (long t : timeData) {
      if (t > candidateTimestamp) {
        return false; // not exist, return early
      }
      if (t == candidateTimestamp) {
        return true; // exist
      }
    }
    return false; // not exist
  }


  @Override
  public Statistics getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public void setFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = new AndFilter(this.filter, filter);
    }
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  protected boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}

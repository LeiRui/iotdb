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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.StepRegress;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class PageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /** decoder for value column */
  protected Decoder valueDecoder;

  /** decoder for time column */
  protected Decoder timeDecoder;

  /** time column in memory */
  protected ByteBuffer timeBuffer;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  public int timeBufferLength;

  protected Filter filter;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public long loadIntBatch_ns = 0; // for DCP metric
  public int loadIntBatch_cnt = 0; // for DCP metric

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
    timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /**
   * Find the point with the closet timestamp equal to or larger than the given timestamp in the
   * chunk.
   *
   * @param targetTimestamp must be within the chunk time range [startTime, endTime]
   */
  public BatchData findTheClosetPointEqualOrAfter(long targetTimestamp, boolean ascending)
      throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    StepRegress stepRegress = pageHeader.getStatistics().getStepRegress();
    // infer position starts from 1, so minus 1 here
    int estimatedPos = (int) Math.round(stepRegress.infer(targetTimestamp)) - 1;

    // search from estimatePos in the timeBuffer to find the closet timestamp equal to or larger
    // than the given timestamp
    if (timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
      while (timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
        estimatedPos++;
        // IOMonitor.incPointsTravered();
      }
    } else if (timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
      while (timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
        estimatedPos--;
        // IOMonitor.incPointsTravered();
      }
      if (timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
        estimatedPos++;
        // IOMonitor.incPointsTravered();
      } // else equal
    } // else equal

    // since we have constrained that targetTimestamp must be within the chunk time range
    // [startTime, endTime],
    // we can definitely find such a point with the closet timestamp equal to or larger than the
    // given timestamp in the chunk.
    long timestamp = timeBuffer.getLong(estimatedPos * 8);
    switch (dataType) {
        // iotdb的int类型的plain编码用的是自制的不支持random access
        //      case INT32:
        //        return new MinMaxInfo(pageReader.valueBuffer.getInt(estimatedPos * 4),
        //            pageReader.timeBuffer.getLong(estimatedPos * 8));
      case INT64:
        long longVal = valueBuffer.getLong(timeBufferLength + estimatedPos * 8);
        pageData.putLong(timestamp, longVal);
        break;
      case FLOAT:
        float floatVal = valueBuffer.getFloat(timeBufferLength + estimatedPos * 4);
        pageData.putFloat(timestamp, floatVal);
        break;
      case DOUBLE:
        double doubleVal = valueBuffer.getDouble(timeBufferLength + estimatedPos * 8);
        pageData.putDouble(timestamp, doubleVal);
        break;
      default:
        throw new IOException("Unsupported data type!");
    }

    return pageData.flip();
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    // TODO
    double targetTimestamp = (pageHeader.getStartTime() + pageHeader.getEndTime()) / 2.0;
    return findTheClosetPointEqualOrAfter((long) targetTimestamp, ascending);
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder valueBuilder = builder.getColumnBuilder(0);
    if (filter == null || filter.satisfy(getStatistics())) {
      switch (dataType) {
        case BOOLEAN:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeBoolean(aBoolean);
              builder.declarePosition();
            }
          }
          break;
        case INT32:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            int anInt = valueDecoder.readInt(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeInt(anInt);
              builder.declarePosition();
            }
          }
          break;
        case INT64:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            long aLong = valueDecoder.readLong(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeLong(aLong);
              builder.declarePosition();
            }
          }
          break;
        case FLOAT:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            float aFloat = valueDecoder.readFloat(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeFloat(aFloat);
              builder.declarePosition();
            }
          }
          break;
        case DOUBLE:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            double aDouble = valueDecoder.readDouble(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeDouble(aDouble);
              builder.declarePosition();
            }
          }
          break;
        case TEXT:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            Binary aBinary = valueDecoder.readBinary(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeBinary(aBinary);
              builder.declarePosition();
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    if (TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder())
        .equals(TSEncoding.TS_2DIFF)) {
      loadIntBatch_ns = ((LongDeltaDecoder) timeDecoder).loadIntBatch_ns;
      loadIntBatch_cnt = ((LongDeltaDecoder) timeDecoder).loadIntBatch_cnt;
      ((LongDeltaDecoder) timeDecoder).loadIntBatch_ns =
          0; // reset because timeDecoder is global for all pageReaders of a chunk
      ((LongDeltaDecoder) timeDecoder).loadIntBatch_cnt =
          0; // reset because timeDecoder is global for all pageReaders of a chunk
    }
    return builder.build();
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

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {}

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

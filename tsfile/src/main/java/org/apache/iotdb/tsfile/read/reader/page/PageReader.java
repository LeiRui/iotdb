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

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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

  protected Filter filter;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public Map<String, List<Long>> elapsedTimeInNanoSec;

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
      Filter filter,
      Map<String, List<Long>> elapsedTimeInNanoSec) {
    this.elapsedTimeInNanoSec = elapsedTimeInNanoSec;
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
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

  public int getTimeBufferSize() {
    return timeBuffer.limit() - timeBuffer.position();
  }

  public int getValueBufferSize() {
    return valueBuffer.limit() - valueBuffer.position();
  }

  public PageHeader getPageHeader() {
    return pageHeader;
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

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData;
    if (TsFileConstant.decomposeMeasureTime && TsFileConstant.D_2_decompose_each_step) {
      // 【D_2_1_createBatchData】
      long start = System.nanoTime();
      pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
      long elapsedTime = System.nanoTime() - start;
      TsFileConstant.record(
          elapsedTimeInNanoSec, TsFileConstant.D_2_createBatchData, elapsedTime, false, false);

      if (filter == null || filter.satisfy(getStatistics())) {
        while (true) {
          // 【D_2_2_timeDecoder_hasNext】
          start = System.nanoTime();
          boolean hasNext = timeDecoder.hasNext(timeBuffer);
          elapsedTime = System.nanoTime() - start;
          TsFileConstant.record(
              elapsedTimeInNanoSec,
              TsFileConstant.D_2_timeDecoder_hasNext,
              elapsedTime,
              false,
              false);

          if (!hasNext) {
            break;
          }

          // 【D_2_3_timeDecoder_readLong】
          start = System.nanoTime();
          long timestamp = timeDecoder.readLong(timeBuffer);
          elapsedTime = System.nanoTime() - start;
          TsFileConstant.record(
              elapsedTimeInNanoSec,
              TsFileConstant.D_2_timeDecoder_readLong,
              elapsedTime,
              false,
              false);

          switch (dataType) {
            case BOOLEAN:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              boolean statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putBoolean(timestamp, aBoolean);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;

            case INT32:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              int anInt = valueDecoder.readInt(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putInt(timestamp, anInt);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;

            case INT64:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              long aLong = valueDecoder.readLong(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putLong(timestamp, aLong);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;

            case FLOAT:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              float aFloat = valueDecoder.readFloat(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putFloat(timestamp, aFloat);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;

            case DOUBLE:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              double aDouble = valueDecoder.readDouble(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putDouble(timestamp, aDouble);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;

            case TEXT:
              // 【D_2_4_valueDecoder_read】
              start = System.nanoTime();
              Binary aBinary = valueDecoder.readBinary(valueBuffer);
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_valueDecoder_read,
                  elapsedTime,
                  false,
                  false);

              // 【D_2_5_checkValueSatisfyOrNot】
              start = System.nanoTime();
              statisfy =
                  !isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary));
              elapsedTime = System.nanoTime() - start;
              TsFileConstant.record(
                  elapsedTimeInNanoSec,
                  TsFileConstant.D_2_checkValueSatisfyOrNot,
                  elapsedTime,
                  false,
                  false);

              if (statisfy) {
                // 【D_2_6_putIntoBatchData】
                start = System.nanoTime();
                pageData.putBinary(timestamp, aBinary);
                elapsedTime = System.nanoTime() - start;
                TsFileConstant.record(
                    elapsedTimeInNanoSec,
                    TsFileConstant.D_2_putIntoBatchData,
                    elapsedTime,
                    false,
                    false);
              }
              break;
            default:
              throw new UnSupportedDataTypeException(String.valueOf(dataType));
          }
        }
      }
      System.out.println("done: " + TsFileConstant.data_decode_time_value_Buffer);

    } else if (TsFileConstant.decomposeMeasureTime) {
      // 【8_data_decode_time_value_Buffer】
      long start = System.nanoTime();
      pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
      if (filter == null || filter.satisfy(getStatistics())) {
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          switch (dataType) {
            case BOOLEAN:
              boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
              if (!isDeleted(timestamp)
                  && (filter == null || filter.satisfy(timestamp, aBoolean))) {
                pageData.putBoolean(timestamp, aBoolean);
              }
              break;
            case INT32:
              int anInt = valueDecoder.readInt(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
                pageData.putInt(timestamp, anInt);
              }
              break;
            case INT64:
              long aLong = valueDecoder.readLong(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
                pageData.putLong(timestamp, aLong);
              }
              break;
            case FLOAT:
              float aFloat = valueDecoder.readFloat(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
                pageData.putFloat(timestamp, aFloat);
              }
              break;
            case DOUBLE:
              double aDouble = valueDecoder.readDouble(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
                pageData.putDouble(timestamp, aDouble);
              }
              break;
            case TEXT:
              Binary aBinary = valueDecoder.readBinary(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
                pageData.putBinary(timestamp, aBinary);
              }
              break;
            default:
              throw new UnSupportedDataTypeException(String.valueOf(dataType));
          }
        }
      }

      long elapsedTime = System.nanoTime() - start;
      TsFileConstant.record(
          elapsedTimeInNanoSec,
          TsFileConstant.data_decode_time_value_Buffer,
          elapsedTime,
          true,
          true);
    } else {
      pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
      if (filter == null || filter.satisfy(getStatistics())) {
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          switch (dataType) {
            case BOOLEAN:
              boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
              if (!isDeleted(timestamp)
                  && (filter == null || filter.satisfy(timestamp, aBoolean))) {
                pageData.putBoolean(timestamp, aBoolean);
              }
              break;
            case INT32:
              int anInt = valueDecoder.readInt(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
                pageData.putInt(timestamp, anInt);
              }
              break;
            case INT64:
              long aLong = valueDecoder.readLong(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
                pageData.putLong(timestamp, aLong);
              }
              break;
            case FLOAT:
              float aFloat = valueDecoder.readFloat(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
                pageData.putFloat(timestamp, aFloat);
              }
              break;
            case DOUBLE:
              double aDouble = valueDecoder.readDouble(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
                pageData.putDouble(timestamp, aDouble);
              }
              break;
            case TEXT:
              Binary aBinary = valueDecoder.readBinary(valueBuffer);
              if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
                pageData.putBinary(timestamp, aBinary);
              }
              break;
            default:
              throw new UnSupportedDataTypeException(String.valueOf(dataType));
          }
        }
      }
    }
    return pageData.flip();
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

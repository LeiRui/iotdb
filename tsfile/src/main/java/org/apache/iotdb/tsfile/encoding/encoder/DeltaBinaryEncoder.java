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

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * DeltaBinaryEncoder is a encoder for compressing data in type of integer and long. We adapt a
 * hypothesis that contiguous data points have similar values. Thus the difference value of two
 * adjacent points is smaller than those two point values. One integer in java takes 32-bits. If a
 * positive number is less than 2^m, the bits of this integer which index from m to 31 are all 0.
 * Given an array which length is n, if all values in input data array are all positive and less
 * than 2^m, we need actually m*n, but not 32*n bits to store the array.
 *
 * <p>DeltaBinaryEncoder calculates difference between two adjacent points and record the minimum of
 * those difference values firstly. Then it saves two_diff value that difference minus minimum of
 * them, to make sure all two_diff values are positive. Then it statistics the longest bit length
 * {@code m} it takes for each two_diff value, which means the bit length that maximum two_diff
 * value takes. Only the low m bits are saved into result byte array for all two_diff values.
 */
public abstract class DeltaBinaryEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE =
      TSFileDescriptor.getInstance().getConfig().getTs2diffPackSize();
  private static final Logger logger = LoggerFactory.getLogger(DeltaBinaryEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;
  // input value is stored in deltaBlackBuffer temporarily
  protected byte[] encodingBlockBuffer;

  protected int writeIndex = -1;
  protected int writeWidth = 0;

  /**
   * constructor of DeltaBinaryEncoder.
   *
   * @param size - the number how many numbers to be packed into a block.
   */
  public DeltaBinaryEncoder(int size) {
    super(TSEncoding.TS_2DIFF);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void writeValueToBytes(int i);

  protected abstract void calcTwoDiff(int i);

  protected abstract void reset();

  protected abstract int calculateBitWidthsForDeltaBlockBuffer();

  /** write all data into {@code encodingBlockBuffer}. */
  private void writeDataWithMinWidth() {
    for (int i = 0; i < writeIndex; i++) {
      writeValueToBytes(i);
    }
    int encodingLength = (int) Math.ceil((double) (writeIndex * writeWidth) / 8.0);
    out.write(encodingBlockBuffer, 0, encodingLength);
  }

  //  private void writeHeaderToBytes() throws IOException {
  //    ReadWriteIOUtils.write(writeIndex, out);
  //    ReadWriteIOUtils.write(writeWidth, out);
  //    writeHeader();
  //  }

  protected abstract void writeHeaderToBytes() throws IOException;

  private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == -1) {
      return;
    }
    // since we store the min delta, the deltas will be converted to be the
    // difference to min delta and all positive
    this.out = out;
    for (int i = 0; i < writeIndex; i++) {
      calcTwoDiff(i);
    }
    writeWidth = calculateBitWidthsForDeltaBlockBuffer();
    writeHeaderToBytes();
    writeDataWithMinWidth();

    reset();
    writeIndex = -1;
  }

  /** calling this method to flush all values which haven't encoded to result byte array. */
  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  public static class IntDeltaEncoder extends DeltaBinaryEncoder {

    private int[] deltaBlockBuffer;
    private int firstValue;
    private int previousValue;
    private int minDeltaBase;

    public IntDeltaEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of IntDeltaEncoder which is a sub-class of DeltaBinaryEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public IntDeltaEncoder(int size) {
      super(size);
      deltaBlockBuffer = new int[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 4];
      reset();
    }

    @Override
    protected void writeHeaderToBytes() throws IOException {
      ReadWriteIOUtils.write(writeIndex, out);
      ReadWriteIOUtils.write(writeWidth, out);
      writeHeader();
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }

    private void calcDelta(int value) {
      int delta = value - previousValue; // calculate delta
      if (delta < minDeltaBase) {
        minDeltaBase = delta;
      }
      deltaBlockBuffer[writeIndex++] = delta;
    }

    /**
     * input a integer.
     *
     * @param value value to encode
     * @param out the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(int value, ByteArrayOutputStream out) {
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        return;
      }
      calcDelta(value);
      previousValue = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected void reset() {
      firstValue = 0;
      previousValue = 0;
      minDeltaBase = Integer.MAX_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        deltaBlockBuffer[i] = 0;
      }
    }

    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.intToBytes(deltaBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDeltaBase, out);
      ReadWriteIOUtils.write(firstValue, out);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 4;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 24 is: index(4)+width(4)+minDeltaBase(4)+firstValue(4)
      return (long) 24 + writeIndex * 4;
    }
  }

  public static class LongDeltaEncoder extends DeltaBinaryEncoder {

    private long[] deltaBlockBuffer;
    private long firstValue;
    private long previousValue;
    private long minDeltaBase;

    public LongDeltaEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of LongDeltaEncoder which is a sub-class of DeltaBinaryEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public LongDeltaEncoder(int size) {
      super(size);
      deltaBlockBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 8];
      reset();
    }

    private void calcDelta(long value) {
      long delta = value - previousValue; // calculate delta
      if (delta < minDeltaBase) {
        minDeltaBase = delta;
      }
      deltaBlockBuffer[writeIndex++] = delta;
    }

    @Override
    protected void writeHeaderToBytes() throws IOException {
      ReadWriteIOUtils.write(writeIndex, out);
      ReadWriteIOUtils.write(writeWidth, out);
      writeHeader();
      // TODO write (relativePos->bytes) for regular new delta
      if (TSFileDescriptor.getInstance().getConfig().isEnableRegularityTimeDecode()) {
        long newRegularDelta =
            TSFileDescriptor.getInstance().getConfig().getRegularTimeInterval() - minDeltaBase;
        if (writeWidth > 0 && 0 <= newRegularDelta && newRegularDelta < Math.pow(2, writeWidth)) {

          //          byte[][] regularBytes = new byte[8][]; // 8 relative positions.
          // relativePos->bytes
          for (int i = 0; i < 8; i++) {
            // i is the starting position in the byte from high to low bits

            int endPos = i + writeWidth - 1; // starting from 0
            int byteNum = endPos / 8 + 1;
            byte[] byteArray = new byte[byteNum];
            if (newRegularDelta != 0) {
              // put bit-packed newRegularDelta starting at position i,
              //  and pad the front and back with newRegularDeltas.
              // Otherwise if newRegularDelta=0, just leave byteArray as initial zeros

              // 1. deal with padding the first byte
              for (int x = i - 1; x >= 0; x--) {
                // y is the position in the bit-packed newRegularDelta, 0->packWidth-1 from low to
                // high bits
                int y = (i - x - 1) % writeWidth;
                // get the bit indicated by y pos
                int value = BytesUtils.getLongN(newRegularDelta, y);
                // put the bit indicated by y pos into regularBytes
                // setByte pos is from high to low starting from 0, corresponding to x
                byteArray[0] = BytesUtils.setByteN(byteArray[0], x, value);
              }

              // 2. deal with putting newRegularDeltas
              BytesUtils.longToBytes(newRegularDelta, byteArray, i, writeWidth);

              // 3. deal with padding the last byte
              for (int x = endPos + 1; x < byteNum * 8; x++) {
                // y is the position in the bit-packed newRegularDelta, 0->packWidth-1 from low to
                // high bits
                int y = writeWidth - 1 - (x - endPos - 1) % writeWidth;
                // get the bit indicated by y pos
                int value = BytesUtils.getLongN(newRegularDelta, y);
                // put the bit indicated by y pos into regularBytes
                // setByte pos is from high to low starting from 0, corresponding to x
                byteArray[byteNum - 1] = BytesUtils.setByteN(byteArray[byteNum - 1], x, value);
              }
            }
            // flush byteArray
            int num = byteArray.length;
            ReadWriteIOUtils.write(num, out);
            for (int j = 0; j < num; j++) {
              ReadWriteIOUtils.write(byteArray[j], out);
            }
          }
        }
      }
    }

    @Override
    protected void reset() {
      firstValue = 0L;
      previousValue = 0L;
      minDeltaBase = Long.MAX_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        deltaBlockBuffer[i] = 0L;
      }
    }

    private int getValueWidth(long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.longToBytes(deltaBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.longToBytes(minDeltaBase));
      out.write(BytesUtils.longToBytes(firstValue));
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 8;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
      return (long) 24 + writeIndex * 8;
    }

    /**
     * input a integer or long value.
     *
     * @param value value to encode
     * @param out - the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(long value, ByteArrayOutputStream out) {
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        return;
      }
      calcDelta(value);
      previousValue = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }
  }
}

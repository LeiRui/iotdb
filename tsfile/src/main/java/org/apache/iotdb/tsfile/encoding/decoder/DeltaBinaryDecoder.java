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

package org.apache.iotdb.tsfile.encoding.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * This class is a decoder for decoding the byte array that encoded by {@code
 * DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br> .
 *
 * @see DeltaBinaryEncoder
 */
public abstract class DeltaBinaryDecoder extends Decoder {

  public long count = 0;
  public byte[] deltaBuf;

  /**
   * the first value in one pack.
   */
  public int readIntTotalCount = 0;

  public int nextReadIndex = 0;
  /**
   * max bit length of all value in a pack.
   */
  public int packWidth;
  /**
   * data number in this pack.
   */
  public int packNum;

  /**
   * how many bytes data takes after encoding.
   */
  public int encodingLength;

  public DeltaBinaryDecoder() {
    super(TSEncoding.TS_2DIFF);
  }

  protected abstract void readHeader(ByteBuffer buffer) throws IOException;

  protected abstract void allocateDataArray();

  protected abstract void readValue(int i);

  /**
   * calculate the bytes length containing v bits.
   *
   * @param v - number of bits
   * @return number of bytes
   */
  protected int ceil(int v) {
    return (int) Math.ceil((double) (v) / 8.0);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
  }

  public static class IntDeltaDecoder extends DeltaBinaryDecoder {

    private int firstValue;
    private int[] data;
    private int previous;
    /**
     * minimum value for all difference.
     */
    private int minDeltaBase;

    public IntDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return readT(buffer);
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int loadIntBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = new byte[encodingLength];
      buffer.get(deltaBuf);
      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readInt(buffer);
      firstValue = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    protected void readValue(int i) {
      int v = BytesUtils.bytesToInt(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }

  public static class LongDeltaDecoder extends DeltaBinaryDecoder {

    public long firstValue;
    public long[] data;
    public long previous;
    /**
     * minimum value for all difference.
     */
    public long minDeltaBase;

    public LongDeltaDecoder() {
      super();
    }

    public long intervalStart = Long.MIN_VALUE;
    public long intervalStop = Long.MIN_VALUE;

    public int currentPackNum = 0; // DO NOT CHANGE the initial value
    public int currentPackWidth = -1;
    public long currentFirstValue = Long.MIN_VALUE; // DO NOT CHANGE the initial value
    public long currentMinDeltaBase = Long.MIN_VALUE;
    public int currentDeltaBufPos = -1;
    public int currentDeltaBufByteLen = -1;

    public int nextPackNum = 0; // DO NOT CHANGE the initial value
    public int nextPackWidth = -1;
    public long nextFirstValue = Long.MIN_VALUE; // DO NOT CHANGE the initial value
    public long nextMinDeltaBase = Long.MIN_VALUE;
    public int nextDeltaBufPos = -1;
    public int nextDeltaBufByteLen = -1;

    public long position = 0; // counted from 1

    /**
     * (currentPack.firstValue, nextPack.firstValue]
     *
     * <p>(-infinity, pack1.firstValue],(pack1.firstValue,
     * pack2.firstValue],...,(packN.firstValue,+infinity)
     *
     * <p>The 1, (packNum+1)+1, 2*(packNum+1)+1, ..., (N-1)*(packNum+1)+1 points
     *
     * <p>(int) Math.ceil(pointNum * 1.0 / (DeltaBinaryEncoder.BLOCK_DEFAULT_SIZE + 1)) + 1
     * intervals
     *
     * <p>Useful for monotonically increasing values
     */
    public boolean hasNextPackInterval(ByteBuffer buffer) {
      if (intervalStop == Long.MAX_VALUE) {
        return false;
      }

      // update intervalStart with previous intervalStop
      intervalStart = intervalStop;

      // increment position before currentPackNum is updated
      if (nextFirstValue
          > Long.MIN_VALUE) { // do nothing for the first interval (-infinity, pack1.firstValue]
        position += currentPackNum;
        position += 1; // for the firstValue
      }

      // update current information with the previous next information
      currentPackNum = nextPackNum;
      currentPackWidth = nextPackWidth;
      currentFirstValue = nextFirstValue;
      currentMinDeltaBase = nextMinDeltaBase;
      currentDeltaBufPos = nextDeltaBufPos;
      currentDeltaBufByteLen = nextDeltaBufByteLen;

      // read the next pack information
      if (buffer.remaining() > 0) {
        // read packNum
        nextPackNum = ReadWriteIOUtils.readInt(buffer);

        // read packWidth
        nextPackWidth = ReadWriteIOUtils.readInt(buffer);

        // read minDeltaBase
        nextMinDeltaBase = ReadWriteIOUtils.readLong(buffer);

        // read firstValue
        nextFirstValue = ReadWriteIOUtils.readLong(buffer);

        // record the next position and byteLen of deltaBuf
        nextDeltaBufPos = buffer.position();
        nextDeltaBufByteLen = (int) Math.ceil((double) (nextPackNum * nextPackWidth) / 8.0);

        // skip ceil(packNum*packWidth/8.0) bytes of deltas
        buffer.position(buffer.position() + nextDeltaBufByteLen);

        // update intervalStop with the firstValue of the next pack
        intervalStop = nextFirstValue;

      } else {
        intervalStop = Long.MAX_VALUE;
      }
      return true;
    }

    /**
     * If the queried timestamp does not exist in the buffer, return -1. Otherwise, return the
     * position of the queries timestamp in the buffer, counted from 1.
     */
    public long checkContainsTimestamp(long query, ByteBuffer buffer) {
      // locate the pack whose interval contains the queries timestamp
      // Since the packIntervals cover the whole range, query will definitely find an interval
      // falling within.
      while (hasNextPackInterval(buffer)) {
        if (intervalStart < query && intervalStop >= query) {
          break;
        }
      }

      if (intervalStop == query) { // special case
        // the firstValue of the next pack equals the queries timestamp, so no need to unpack the
        // current pack
        return position + currentPackNum + 1;
      }

      if (currentFirstValue == Long.MIN_VALUE) { // special case
        // the first interval (-Infinity, pack1.firstValue] contains the queries timestamp AND
        // pack1.firstValue does not equal the queries timestamp
        return -1;
      }

      // get and decode the corresponding deltaBuf to check whether the pack contains the queries
      // timestamp
      deltaBuf = new byte[currentDeltaBufByteLen];
      buffer.position(currentDeltaBufPos);
      buffer.get(deltaBuf);
      previous = currentFirstValue;
      long cnt = 0;
      for (int i = 0; i < currentPackNum; i++) {
        long v = BytesUtils.bytesToLong(deltaBuf, currentPackWidth * i, currentPackWidth);
        long data = previous + currentMinDeltaBase + v;
        cnt++;
        if (data == query) {
          return position + cnt;
        }
        if (data > query) { // no need to continue because monotonically increasing timestamps
          return -1;
        }
        previous = data;
      }
      return -1;
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    protected long readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    //    public long readT_RL(ByteBuffer buffer) {
    //      if (nextReadIndex == readIntTotalCount) {
    //        return loadIntBatch_RL(buffer);
    //      }
    //      // 若当前pack没有读完，不是从long data数组里面获取下一个点，而是从delta数组现场累加得到下一个点的bytes
    //      //      return data[nextReadIndex++];
    //      // TODO：返回bytes的数值，然后nextReadIndex++
    //      // TODO: previous和minDeltaBase是不是都要longToBytes
    //      // long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
    //      // data[i] = previous + minDeltaBase + v;
    //      // TODO：1. 准备一个bytes形式的previous，2. 准备一个bytes形式的minDeltaBase, 3. 每次从deltaBuf里拿出packWidth
    // *
    //      // i位置的packWidth宽度的比特，
    //      // TODO 然后把bytes形式的previous+minDeltaBase+delta
    //      return 1;
    //    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    public long loadIntBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = new byte[encodingLength];
      buffer.get(deltaBuf);
      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    public long loadIntBatch_RL(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = new byte[encodingLength];
      buffer.get(deltaBuf);
      //      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      //      readPack();
      // 若当前pack读完，获取下一个pack的操作里面的readPack函数要注释掉，因为这个函数做的是遍历deltaBuf里每个delta然后还原出long
      // dataValue。我只需要拿到deltaBuf就可以了。
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    public long readLong(ByteBuffer buffer) {

      return readT(buffer);
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readLong(buffer);
      firstValue = ReadWriteIOUtils.readLong(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    protected void readValue(int i) {
      long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }

    /**
     * @param i start from 0, exclude firstValue
     */
    public long getDelta(int i) {
      return BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth) + minDeltaBase;
    }

    /**
     * @param start       start from 0, including firstValue
     * @param destination start from 0, including firstValue
     */
    public long getValue(long currentValue, int start, int destination) {
      if (destination > start) {
        for (int i = start + 1; i <= destination; i++) {
          currentValue += getDelta(i - 1);
        }
      } else {
        for (int i = start - 1; i >= destination; i--) {
          currentValue -= getDelta(i);
        }
      }
      return currentValue;
    }

    @Override
    public void reset() {
      intervalStart = Long.MIN_VALUE;
      intervalStop = Long.MIN_VALUE;

      currentPackNum = 0; // DO NOT CHANGE the initial value
      currentPackWidth = -1;
      currentFirstValue = Long.MIN_VALUE; // DO NOT CHANGE the initial value
      currentMinDeltaBase = Long.MIN_VALUE;
      currentDeltaBufPos = -1;
      currentDeltaBufByteLen = -1;

      nextPackNum = 0; // DO NOT CHANGE the initial value
      nextPackWidth = -1;
      nextFirstValue = Long.MIN_VALUE; // DO NOT CHANGE the initial value
      nextMinDeltaBase = Long.MIN_VALUE;
      nextDeltaBufPos = -1;
      nextDeltaBufByteLen = -1;

      position = 0; // counted from 1
    }
  }
}

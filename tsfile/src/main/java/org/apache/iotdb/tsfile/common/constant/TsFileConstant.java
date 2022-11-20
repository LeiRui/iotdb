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
package org.apache.iotdb.tsfile.common.constant;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class TsFileConstant {

  public static final String TSFILE_SUFFIX = ".tsfile";
  public static final String TSFILE_HOME = "TSFILE_HOME";
  public static final String TSFILE_CONF = "TSFILE_CONF";
  public static final String PATH_ROOT = "root";
  public static final String TMP_SUFFIX = "tmp";
  public static final String PATH_SEPARATOR = ".";
  public static final char PATH_SEPARATOR_CHAR = '.';
  public static final String PATH_SEPARATER_NO_REGEX = "\\.";
  public static final char DOUBLE_QUOTE = '"';
  public static final char BACK_QUOTE = '`';
  public static final String BACK_QUOTE_STRING = "`";
  public static final String DOUBLE_BACK_QUOTE_STRING = "``";

  public static final byte TIME_COLUMN_MASK = (byte) 0x80;
  public static final byte VALUE_COLUMN_MASK = (byte) 0x40;

  private static final String IDENTIFIER_MATCHER = "([a-zA-Z0-9_\\u2E80-\\u9FFF]+)";
  public static final Pattern IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIER_MATCHER);

  private static final String NODE_NAME_MATCHER = "(\\*{0,2}[a-zA-Z0-9_\\u2E80-\\u9FFF]+\\*{0,2})";
  public static final Pattern NODE_NAME_PATTERN = Pattern.compile(NODE_NAME_MATCHER);

  private static final String NODE_NAME_IN_INTO_PATH_MATCHER = "([a-zA-Z0-9_${}\\u2E80-\\u9FFF]+)";
  public static final Pattern NODE_NAME_IN_INTO_PATH_PATTERN =
      Pattern.compile(NODE_NAME_IN_INTO_PATH_MATCHER);

  private TsFileConstant() {}

  /**
   * Pad the bytes with bit-packed newRegularDelta at 8 relative positions. This function is used
   * for regularity-aware decoding to compare regularBytes with newDeltaBytes. Example:
   * writeWidth=3, bit-packed newRegularDelta = 0b101, return regularBytes as: 0->0b10110110,
   * 1->0b11011011, 2->0b01101101, 3->0b10110110, 4->0b11011011, 5->0b01101101, 6->0b10110110 &
   * 0b11011011, 7->0b11011011 & 0b01101101.
   */
  public static byte[][] generateRegularByteArray(
      int writeWidth, long regularTimeInterval, long minDeltaBase) throws IOException {
    long newRegularDelta = regularTimeInterval - minDeltaBase;
    if (writeWidth == 0 || newRegularDelta < 0 || newRegularDelta >= Math.pow(2, writeWidth)) {
      throw new IOException(
          "writeWidth == 0 || newRegularDelta < 0 || newRegularDelta >= Math.pow(2, writeWidth)");
    }

    byte[][] regularBytes = new byte[8][]; // 8 relative positions. relativePos->bytes
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
      regularBytes[i] = byteArray;
    }
    return regularBytes;
  }

  public static byte[][] readRegularBytes(ByteBuffer buffer) {
    byte[][] regularBytes = new byte[8][];
    for (int i = 0; i < 8; i++) {
      int byteArrayLength = ReadWriteIOUtils.readInt(buffer);
      regularBytes[i] = new byte[byteArrayLength];
      for (int j = 0; j < byteArrayLength; j++) {
        regularBytes[i][j] = ReadWriteIOUtils.readByte(buffer);
      }
    }
    return regularBytes;
  }
}

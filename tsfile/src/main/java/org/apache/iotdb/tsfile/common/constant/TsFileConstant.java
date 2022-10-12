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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.IOException;

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

  public static DescriptiveStatistics timeColumnTS2DIFFLoadBatchCost = new DescriptiveStatistics();

  public static long countForRegularEqual = 0; // equal to regular
  public static long countForRegularNOTEqual = 0; // not equal to regular
  public static long countForRegularZero = 0; // packWidth=0 no need

  public static DescriptiveStatistics regularNewDeltasStatistics = new DescriptiveStatistics();

  public static long countForHitNewDeltas = 0; // allRegularBytes contains
  public static long countForNotHitNewDeltas = 0; // allRegularBytes not contains

  public static DescriptiveStatistics byteArrayLengthStatistics =
      new DescriptiveStatistics(); // regular bytes length

  public static DescriptiveStatistics allRegularBytesSize =
      new DescriptiveStatistics(); // allRegularBytes map size

  public static DescriptiveStatistics prepareAllRegulars = new DescriptiveStatistics();

  public static long bytesToLong_byteNum1_wholeByte = 0;
  public static long bytesToLong_byteNum1_smallByte = 0;
  public static long byteToLong_byteNums_firstByte_wholeByte = 0;
  public static long byteToLong_byteNums_firstByte_smallByte = 0;
  public static long byteToLong_byteNums_lastByte_wholeByte = 0;
  public static long byteToLong_byteNums_lastByte_smallByte = 0;
  public static long byteToLong_byteNums_middleWholeByte = 0;

  public static int[] endInByteMasks = {
    0b01111111, 0b00111111, 0b00011111, 0b00001111, 0b00000111, 0b00000011, 0b00000001
  };

  public static int[] startInByteMasks = {
    0b10000000, 0b11000000, 0b11100000, 0b11110000, 0b11111000, 0b11111100, 0b11111110
  };

  /**
   * example: int[] fallWithinMasks_2 = { 0b11000000, 0b01100000, 0b00110000, 0b00011000,
   * 0b00001100, 0b00000110, 0b00000011 }; int[] fallWithinMasks_3 = { 0b11100000, 0b01110000,
   * 0b00111000, 0b00011100, 0b00001110, 0b00000111 };
   */
  public static int[] generateFallWithinMasks(int packWidth) throws IOException {
    if (packWidth >= 8) {
      throw new IOException("only accept packWidth smaller than 8.");
    }
    int num = 9 - packWidth;
    int[] res = new int[num];
    int mask = (int) Math.pow(2, packWidth) - 1;
    for (int i = num - 1; i >= 0; i--) {
      res[i] = mask << (num - 1 - i);
    }
    return res;
  }

  private TsFileConstant() {}

  public static void printByteToLongStatistics() {
    System.out.println("bytesToLong_byteNum1_wholeByte=" + bytesToLong_byteNum1_wholeByte);
    System.out.println("bytesToLong_byteNum1_smallByte=" + bytesToLong_byteNum1_smallByte);
    System.out.println(
        "byteToLong_byteNums_firstByte_wholeByte=" + byteToLong_byteNums_firstByte_wholeByte);
    System.out.println(
        "byteToLong_byteNums_firstByte_smallByte=" + byteToLong_byteNums_firstByte_smallByte);
    System.out.println(
        "byteToLong_byteNums_lastByte_wholeByte=" + byteToLong_byteNums_lastByte_wholeByte);
    System.out.println(
        "byteToLong_byteNums_lastByte_smallByte=" + byteToLong_byteNums_lastByte_smallByte);
    System.out.println(
        "byteToLong_byteNums_middleWholeByte=" + byteToLong_byteNums_middleWholeByte);
  }
}

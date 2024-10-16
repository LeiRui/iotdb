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
package org.apache.iotdb.tsfile.file.metadata.statistics;

/** @author Yuyuan Kang */
public class MinMaxInfo<K> implements Comparable {

  public K val;
  public Long timestamp;

  //  public MinMaxInfo(T val, Set<Long> timestamps) {
  //    this.val = val;
  //    this.timestamps = timestamps;
  //  }

  public MinMaxInfo() {}

  public MinMaxInfo(K val, long timestamp) {
    this.val = val;
    this.timestamp = timestamp;
  }

  public MinMaxInfo(MinMaxInfo<K> minMaxInfo) {
    this.val = minMaxInfo.val;
    this.timestamp = minMaxInfo.timestamp;
  }

  public void reset(K val, long timestamp) {
    this.val = val;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return val.toString() + "[" + timestamp + "]";
  }

  @Override
  public boolean equals(Object minMaxInfo) {
    if (minMaxInfo.getClass() == this.getClass()) {
      return this.val.equals(((MinMaxInfo<K>) minMaxInfo).val)
          && this.timestamp.equals(((MinMaxInfo<K>) minMaxInfo).timestamp);
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(Object minMaxInfo) {
    if (minMaxInfo.getClass() == this.getClass()) {
      try {
        return ((Comparable) this.val).compareTo(((MinMaxInfo<K>) minMaxInfo).val);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("Input data type is not comparable");
      }
    } else if (minMaxInfo instanceof Integer
        || minMaxInfo instanceof Long
        || minMaxInfo instanceof Double
        || minMaxInfo instanceof FloatStatistics) {
      return ((Comparable) this.val).compareTo(minMaxInfo);
    } else {
      throw new IllegalArgumentException("Input object is not MinMaxInfo type");
    }
  }
}

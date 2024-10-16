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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.List;

/** Each executor calculates results of all aggregations on this series */
public interface GroupByExecutor {

  /** add reusable result cache in executor */
  void addAggregateResult(AggregateResult aggrResult);

  /** calculate result in [curStartTime, curEndTime) */
  List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException;

  List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException, QueryProcessException;

  Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime) throws IOException;
}

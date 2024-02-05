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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.QuickHullPoint;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalGroupByExecutorTri_ILTS implements GroupByExecutor {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger M4_CHUNK_METADATA = LoggerFactory.getLogger("M4_CHUNK_METADATA");

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();

  // keys: 0,1,...,(int) Math.floor((endTime * 1.0 - startTime) / interval)-1
  private final Map<Integer, List<ChunkSuit4Tri>> splitChunkList = new HashMap<>();

  private final long p1t = CONFIG.getP1t();
  private final double p1v = CONFIG.getP1v();
  private final long pnt = CONFIG.getPnt();
  private final double pnv = CONFIG.getPnv();

  private long lt;
  private double lv;

  private final int N1; // 分桶数

  private final int numIterations = CONFIG.getNumIterations(); // do not make it static

  private Filter timeFilter;

  public LocalGroupByExecutorTri_ILTS(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    //    long start = System.nanoTime();

    // get all data sources
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, this.timeFilter);

    // update filter by TTL
    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesReader seriesReader =
        new SeriesReader(
            path,
            allSensors,
            // fix bug: here use the aggregation type as the series data type,
            // not using pageReader.getAllSatisfiedPageData is ok
            dataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);

    // unpackAllOverlappedFilesToTimeSeriesMetadata
    try {
      // : this might be bad to load all chunk metadata at first
      List<ChunkSuit4Tri> futureChunkList =
          new ArrayList<>(seriesReader.getAllChunkMetadatas4Tri()); // no need sort here
      // arrange futureChunkList into each bucket
      GroupByFilter groupByFilter = (GroupByFilter) timeFilter;
      long startTime = groupByFilter.getStartTime();
      long endTime = groupByFilter.getEndTime();
      long interval = groupByFilter.getInterval();
      N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // 分桶数
      for (ChunkSuit4Tri chunkSuit4Tri : futureChunkList) {
        ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
        long chunkMinTime = chunkMetadata.getStartTime();
        long chunkMaxTime = chunkMetadata.getEndTime();
        if (chunkMinTime >= endTime || chunkMaxTime < startTime) {
          continue; // note futureChunkList is not sorted in advance, so not break, just skip
        }
        int idx1 = (int) Math.floor((chunkMinTime - startTime) * 1.0 / interval);
        idx1 = Math.max(idx1, 0);
        int idx2 = (int) Math.floor((chunkMaxTime - startTime) * 1.0 / interval);
        idx2 = Math.min(idx2, N1 - 1);
        for (int i = idx1; i <= idx2; i++) {
          splitChunkList.computeIfAbsent(i, k -> new ArrayList<>());
          splitChunkList.get(i).add(chunkSuit4Tri);
        }
      }

      //      // debug
      //      for (int i = 0; i < N1; i++) {
      //        List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(i);
      //        if (chunkSuit4TriList != null) {
      //          for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
      //            System.out.println(i + "," + chunkSuit4Tri.chunkMetadata.getStartTime());
      //          }
      //        }
      //      }

    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }

    //    IOMonitor2.addMeasure(Operation.M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS, System.nanoTime() -
    // start);
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  @Override
  public List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    // 这里用calcResult一次返回所有buckets结果（可以把MinValueAggrResult的value设为string类型，
    // 那就把所有buckets结果作为一个string返回。这样的话返回的[t]是没有意义的，只取valueString）
    // 而不是像MinMax那样在nextWithoutConstraintTri_MinMax()里调用calcResult每次计算一个bucket
    StringBuilder series_final = new StringBuilder();

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    long[] lastIter_t = new long[N1]; // N1不包括全局首尾点，初始化都是0，假设真实时间戳都大于0
    double[] lastIter_v = new double[N1]; // N1不包括全局首尾点

    // TODO: 如果和上次迭代时使用的lr一样那么这个bucket这次迭代就使用上次的采点结果，不必重复计算
    boolean[] lastSame = new boolean[N1 + 1]; // N1不包括全局首点，初始化都是false
    lastSame[N1] = true; // 把全局尾点设为true

    int num = 0; // 注意从0开始！
    for (; num < numIterations; num++) {
      // NOTE: init lt&lv at the start of each iteration is a must, because they are modified in
      // each iteration
      lt = CONFIG.getP1t();
      lv = CONFIG.getP1v();

      //      boolean[] currentSame = new boolean[N1 + 1]; // N1不包括全局首点，初始化都是false
      //      currentSame[N1] = true; // 把全局尾点设为true
      boolean allSameFlag = true; // 如果非首轮迭代全部都是true那可以提前结束迭代，因为后面都不会再有任何变化
      boolean currentLeftSame = true;

      //      StringBuilder series = new StringBuilder(); // TODO debug
      //      // 全局首点
      //      series.append(p1v).append("[").append(p1t).append("]").append(","); // TODO debug

      // 遍历分桶 Assume no empty buckets
      for (int b = 0; b < N1; b++) {
        if (CONFIG.isAcc_iterRepeat() && num > 0 && lastSame[b + 1] && currentLeftSame) {
          // 排除num=0，因为第一次迭代要全部算的
          // 不需要更新本轮迭代本桶选点，或者说本轮迭代本桶选点就是lastIter内已有结果
          // 也不需要更新currentNeedRecalc
          // 下一个桶自然地以select_t, select_v作为左桶固定点
          lt = lastIter_t[b];
          lv = lastIter_v[b];
          lastSame[b] = true; // 因为这个桶现在只会被下一轮当作右边桶读了
          continue;
        }
        double rt = 0; // must initialize as zero, because may be used as sum for average
        double rv = 0; // must initialize as zero, because may be used as sum for average
        // 计算右边桶的固定点
        if (b == N1 - 1) { // 最后一个桶
          // 全局尾点
          rt = pnt;
          rv = pnv;
        } else { // 不是最后一个桶
          if (num == 0) { // 是第一次迭代的话，就使用右边桶的平均点
            // ========计算右边桶的平均点========
            List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b + 1);
            if (chunkSuit4TriList == null) {
              throw new IOException("Empty bucket!");
            }
            long rightStartTime = startTime + (b + 1) * interval;
            long rightEndTime = startTime + (b + 2) * interval;
            int cnt = 0;
            // 遍历所有与右边桶overlap的chunks
            for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
              TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
              if (dataType != TSDataType.DOUBLE) {
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
              }
              // TODO: 用元数据sum&count加速
              //  如果chunk没有被桶切开，可以直接用元数据里的sum和count
              if (CONFIG.isAcc_avg()) {
                if (chunkSuit4Tri.chunkMetadata.getStartTime() >= rightStartTime
                    && chunkSuit4Tri.chunkMetadata.getEndTime() < rightEndTime) {
                  // TODO 以后元数据可以增加sum of timestamps，目前就基于时间戳均匀间隔1的假设来处理
                  rt +=
                      (chunkSuit4Tri.chunkMetadata.getStartTime()
                              + chunkSuit4Tri.chunkMetadata.getEndTime())
                          * chunkSuit4Tri.chunkMetadata.getStatistics().getCount()
                          / 2.0;
                  rv += chunkSuit4Tri.chunkMetadata.getStatistics().getSumDoubleValue();
                  cnt += chunkSuit4Tri.chunkMetadata.getStatistics().getCount();
                  continue;
                }
              }

              // 1. load page data if it hasn't been loaded
              if (chunkSuit4Tri.pageReader == null) {
                chunkSuit4Tri.pageReader =
                    FileLoaderUtils.loadPageReaderList4CPV(
                        chunkSuit4Tri.chunkMetadata, this.timeFilter);
                //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
                //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
              }
              // 2. 计算平均点
              PageReader pageReader = chunkSuit4Tri.pageReader;
              for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
                IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
                long timestamp = pageReader.timeBuffer.getLong(j * 8);
                if (timestamp < rightStartTime) {
                  continue;
                } else if (timestamp >= rightEndTime) {
                  break;
                } else { // rightStartTime<=t<rightEndTime
                  ByteBuffer valueBuffer = pageReader.valueBuffer;
                  double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
                  rt += timestamp;
                  rv += v;
                  cnt++;
                }
              }
            }
            if (cnt == 0) {
              throw new IOException("Empty bucket!");
            }
            rt = rt / cnt;
            rv = rv / cnt;
          } else { // 不是第一次迭代也不是最后一个桶的话，就使用上一轮迭代右边桶的采样点
            rt = lastIter_t[b + 1];
            rv = lastIter_v[b + 1];
          }
        }
        // ========找到当前桶内距离lr连线最远的点========
        double maxDistance = -1;
        long select_t = -1;
        double select_v = -1;
        List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b);
        long localCurStartTime = startTime + (b) * interval;
        long localCurEndTime = startTime + (b + 1) * interval;
        if (CONFIG.isAcc_rectangle()) {
          // TODO: 用元数据里落在桶内的FP&LP&BP&TP形成的rectangle加速
          //   边点得到距离的紧致下限，角点得到距离的非紧致上限
          //   先遍历一遍这些元数据，得到所有落在桶内的元数据点的最远点，更新maxDistance&select_t&select_v
          //   然后遍历如果这个chunk的非紧致上限<=当前已知的maxDistance，那么整个chunk都不用管了
          for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
            long[] rect_t =
                new long[] {
                  chunkSuit4Tri.chunkMetadata.getStartTime(), // FPt
                  chunkSuit4Tri.chunkMetadata.getEndTime(), // LPt
                  chunkSuit4Tri.chunkMetadata.getStatistics().getBottomTimestamp(), // BPt
                  chunkSuit4Tri.chunkMetadata.getStatistics().getTopTimestamp() // TPt
                };
            double[] rect_v =
                new double[] {
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getFirstValue(), // FPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getLastValue(), // LPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getMinValue(), // BPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getMaxValue() // TPv
                };
            // 用落在桶内的元数据点（紧致下限）更新maxDistance&select_t&select_v
            for (int i = 0; i < 4; i++) {
              if (rect_t[i] >= localCurStartTime && rect_t[i] < localCurEndTime) {
                double distance =
                    IOMonitor2.calculateDistance(lt, lv, rect_t[i], rect_v[i], rt, rv);
                if (distance > maxDistance) {
                  maxDistance = distance;
                  select_t = rect_t[i];
                  select_v = rect_v[i];
                }
              }
            }
            // 用四个角点计算每个块的相对于当前固定线的非紧致上限
            // 注意第一步是直接赋值而不是和旧的比较，因为距离是相对于线L的，每次迭代每个分桶下的L不同
            chunkSuit4Tri.distance_loose_upper_bound =
                IOMonitor2.calculateDistance(lt, lv, rect_t[0], rect_v[2], rt, rv); // FPt,BPv,左下角
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(
                        lt, lv, rect_t[0], rect_v[3], rt, rv)); // FPt,TPv,左上角
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(
                        lt, lv, rect_t[1], rect_v[2], rt, rv)); // LPt,BPv,右下角
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(
                        lt, lv, rect_t[1], rect_v[3], rt, rv)); // LPt,TPv,右上角
          }
        }
        // 遍历所有与当前桶overlap的chunks
        for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
          TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
          if (dataType != TSDataType.DOUBLE) {
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
          }
          // TODO: (continue)用元数据里落在桶内的FP&LP&BP&TP形成的rectangle加速
          //   边点得到距离的紧致下限，角点得到距离的非紧致上限
          //   如果这个chunk的非紧致上限<=当前已知的maxDistance，那么整个chunk都不用管了
          if (CONFIG.isAcc_rectangle()) {
            //   （当一个chunk的非紧致上限=紧致下限的时候，意味着有角点和边点重合，
            //   如果这个上限是maxDistance，在“用元数据点/紧致下限更新maxDistance&select_t&select_v”
            //   步骤中已经赋值了这个边点，所以这里跳过没关系）
            if (chunkSuit4Tri.distance_loose_upper_bound <= maxDistance) {
              //                            System.out.println("skip" + b + "," +
              //               chunkSuit4Tri.chunkMetadata.getStartTime());
              continue;
            }
          }
          // load page data if it hasn't been loaded
          if (chunkSuit4Tri.pageReader == null) {
            chunkSuit4Tri.pageReader =
                FileLoaderUtils.loadPageReaderList4CPV(
                    chunkSuit4Tri.chunkMetadata, this.timeFilter);
            //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
            //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
            //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
            //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
            //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
          }
          PageReader pageReader = chunkSuit4Tri.pageReader;
          // TODO: 用凸包bitmap加速
          //   如果块被分桶边界切开，那还是逐点遍历
          //   否则块完整落在桶内时，用凸包规则快速找到这个块中沿着lr连线法向量最高和最低的点，最后和全局当前最远结果点比较
          //   也可以改成先不管是不是完整落在桶里，先找到最高低点，然后如果这两个点没有当前已知最远点远那就可以排除了，
          //   否则如果最远但是不在当前桶里那还是要遍历，否则最远且在桶里就可以更新当前已知最远点。
          // //目前先这样只管完全落在桶里的
          //          if (CONFIG.isAcc_convex()
          //              && chunkSuit4Tri.chunkMetadata.getStartTime() >= localCurStartTime
          //              && chunkSuit4Tri.chunkMetadata.getEndTime() < localCurEndTime
          //              && chunkSuit4Tri.chunkMetadata.getStatistics().getCount() >= 3 // 不考虑少于三个点
          //          ) {
          //            BitSet bitSet =
          // chunkSuit4Tri.chunkMetadata.getStatistics().getQuickHullBitSet();
          //            List<QuickHullPoint> foundPoints =
          //                convexHullAcc(
          //                    lt,
          //                    lv,
          //                    rt,
          //                    rv,
          //                    pageReader,
          //                    bitSet,
          //                    chunkSuit4Tri.chunkMetadata.getStatistics().getCount()); //
          // 有可能不止两个点，当一边是平行线两端点
          //            //            System.out.println(foundPoints);
          //            for (QuickHullPoint point : foundPoints) {
          //              IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
          //              double distance = IOMonitor2.calculateDistance(lt, lv, point.t, point.v,
          // rt, rv);
          //              if (distance > maxDistance) {
          //                // 是不是因为开启了acc_rect之后，导致这里要遍历的chunk块里没有点的距离可以达到maxDistance
          //                //     从而acc_convex不会生效？！
          //                maxDistance = distance;
          //                select_t = point.t;
          //                select_v = point.v;
          //              }
          //            }
          //            continue; // note this
          //          }
          if (CONFIG.isAcc_convex()
              && chunkSuit4Tri.chunkMetadata.getStatistics().getCount() >= 3 // 不考虑少于三个点
          ) {
            BitSet bitSet = chunkSuit4Tri.chunkMetadata.getStatistics().getQuickHullBitSet();
            List<QuickHullPoint> foundPoints =
                convexHullAcc(
                    lt,
                    lv,
                    rt,
                    rv,
                    pageReader,
                    bitSet,
                    chunkSuit4Tri.chunkMetadata.getStatistics().getCount()); // 有可能不止两个点，当一边是平行线两端点
            //            System.out.println(foundPoints);
            double ch_maxDistance = -1;
            long ch_select_t = -1;
            double ch_select_v = -1;
            // 找到foundPoints里的最远点
            for (QuickHullPoint point : foundPoints) {
              IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
              double distance = IOMonitor2.calculateDistance(lt, lv, point.t, point.v, rt, rv);
              if (distance > ch_maxDistance) {
                // 是不是因为开启了acc_rect之后，导致这里要遍历的chunk块里没有点的距离可以达到maxDistance
                //     从而acc_convex不会生效？！
                ch_maxDistance = distance;
                ch_select_t = point.t;
                ch_select_v = point.v;
              }
            }
            // 和当前找到的最远距离比较
            if (ch_maxDistance <= maxDistance) {
              continue; // 这个块里一定没有比当前找到的最远点更远的点，不管块凸包最远点在不在当前桶里都不用管了
            }
            // 否则ch_maxDistance>maxDistance，还要判断落在当前桶内才行
            if (ch_select_t >= localCurStartTime && ch_select_t < localCurEndTime) {
              maxDistance = ch_maxDistance;
              select_t = ch_select_t;
              select_v = ch_select_v;
              continue; // note this
            }
            // 否则ch_maxDistance>maxDistance但是这个块的凸包最远点不在当前桶里，于是继续下面的遍历点操作
          }
          int count = chunkSuit4Tri.chunkMetadata.getStatistics().getCount();
          int j;
          for (j = 0; j < count; j++) {
            IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
            long timestamp = pageReader.timeBuffer.getLong(j * 8);
            if (timestamp < localCurStartTime) {
              continue;
            } else if (timestamp >= localCurEndTime) {
              break;
            } else { // localCurStartTime<=t<localCurEndTime
              ByteBuffer valueBuffer = pageReader.valueBuffer;
              double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
              double distance = IOMonitor2.calculateDistance(lt, lv, timestamp, v, rt, rv);
              if (distance > maxDistance) {
                // 是不是因为开启了acc_rect之后，导致这里要遍历的chunk块里没有点的距离可以达到maxDistance
                //     从而acc_convex不会生效？！
                maxDistance = distance;
                select_t = timestamp;
                select_v = v;
              }
            }
          }
        } // 遍历与当前桶有overlap的chunks结束
        //        // 记录结果 // TODO debug
        //        series.append(select_v).append("[").append(select_t).append("]").append(",");

        // 更新currentNeedRecalc,注意在记录本轮迭代本桶选点之前判断
        if (CONFIG.isAcc_iterRepeat()) {
          if (select_t != lastIter_t[b]) { // 本次迭代选点结果和上一轮不一样
            allSameFlag = false; // 无法提前退出迭代
            lastSame[b] = false; // 提示下一轮迭代左边桶的右边固定点变了从而左边桶到时候要重新计算
            currentLeftSame = false; // 提示这一轮迭代右边桶的左边固定点变了从而右边桶到时候要重新计算
            //          if (b == 0) { // 第一个桶
            //            currentSame[b + 1] = true; // 作为右边桶的左边固定点变了，所以下一轮右边桶要重新采点
            //          } else if (b == N1 - 1) { // 最后一个桶
            //            currentSame[b - 1] = true; // 作为左边桶的右边固定点变了，所以下一轮左边桶要重新采点
            //          } else {
            //            currentSame[b - 1] = true; // 作为左边桶的右边固定点变了，所以下一轮左边桶要重新采点
            //            currentSame[b + 1] = true; // 作为右边桶的左边固定点变了，所以下一轮右边桶要重新采点
            //          }
          } else {
            lastSame[b] = true;
            currentLeftSame = true;
          }
        }

        // 更新lt,lv
        // 下一个桶自然地以select_t, select_v作为左桶固定点
        lt = select_t;
        lv = select_v;
        // 记录本轮迭代本桶选点
        lastIter_t[b] = select_t;
        lastIter_v[b] = select_v;
      } // 遍历分桶结束

      //      // 全局尾点 // TODO debug
      //      series.append(pnv).append("[").append(pnt).append("]").append(",");
      //      System.out.println(series);

      if (CONFIG.isAcc_iterRepeat() && allSameFlag) {
        num++; // +1表示是完成的迭代次数
        break;
      }
      // 否则currentNeedRecalc里至少有一个true，因此继续迭代
      //      lastSame = currentSame;
      //      System.out.println(Arrays.toString(needRecalc)); // TODO debug

    } // end Iterations
    //    System.out.println("number of iterations=" + num); // TODO debug

    // 全局首点
    series_final.append(p1v).append("[").append(p1t).append("]").append(",");
    for (int i = 0; i < lastIter_t.length; i++) {
      series_final.append(lastIter_v[i]).append("[").append(lastIter_t[i]).append("]").append(",");
    }
    // 全局尾点
    series_final.append(pnv).append("[").append(pnt).append("]").append(",");
    MinValueAggrResult minValueAggrResult = (MinValueAggrResult) results.get(0);
    minValueAggrResult.updateResult(new MinMaxInfo<>(series_final.toString(), 0));

    return results;
  }

  // 在convex hull加速下找到沿着lr连线法向量方向最高和最低的pageReader的buffer中的两个位置
  // 不考虑少于三个点的情况
  public List<QuickHullPoint> convexHullAcc(
      double lt, double lv, double rt, double rv, PageReader pageReader, BitSet bitSet, int count) {
    // 连接左右固定点的线的法向量(A,B)
    double A = lv - rv;
    double B = rt - lt;

    BitSet reverseBitSet = IOMonitor2.reverse(bitSet);

    //    for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
    //      indexes.add(i);
    //    }

    long fpt = pageReader.timeBuffer.getLong(0);
    double fpv = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength);
    long lpt = pageReader.timeBuffer.getLong((count - 1) * 8);
    double lpv = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + (count - 1) * 8);

    List<QuickHullPoint> LU = new ArrayList<>();
    List<QuickHullPoint> LL = new ArrayList<>();
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    int bitSetIdx = bitSet.nextSetBit(1); // 0 must true
    long t = pageReader.timeBuffer.getLong(bitSetIdx * 8);
    double v = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + bitSetIdx * 8);
    int check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
    if (check > 0) { // p above the line connecting FP&LP
      // init LU clockwise
      LU.add(new QuickHullPoint(fpt, fpv));
      LU.add(new QuickHullPoint(t, v));
      // init LL counterclockwise
      LL.add(new QuickHullPoint(t, v));
      LL.add(new QuickHullPoint(fpt, fpv));
    } else { // p below the line connecting FP&LP
      // init LU clockwise
      LU.add(new QuickHullPoint(t, v));
      LU.add(new QuickHullPoint(fpt, fpv));
      // init LL counterclockwise
      LL.add(new QuickHullPoint(fpt, fpv));
      LL.add(new QuickHullPoint(t, v));
    }

    List<QuickHullPoint> RU = new ArrayList<>();
    List<QuickHullPoint> RL = new ArrayList<>();
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    int reverseBitSetIdx = reverseBitSet.nextSetBit(1); // 0 must true
    t = pageReader.timeBuffer.getLong((count - reverseBitSetIdx - 1) * 8); // note this reverse
    v =
        pageReader.valueBuffer.getDouble(
            pageReader.timeBufferLength + (count - reverseBitSetIdx - 1) * 8);
    check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
    if (check > 0) { // p above the line connecting FP&LP
      // init RU counterclockwise
      RU.add(new QuickHullPoint(lpt, lpv));
      RU.add(new QuickHullPoint(t, v));
      // init RL clockwise
      RL.add(new QuickHullPoint(t, v));
      RL.add(new QuickHullPoint(lpt, lpv));
    } else { // p below the line connecting FP&LP
      // init RU counterclockwise
      RU.add(new QuickHullPoint(t, v));
      RU.add(new QuickHullPoint(lpt, lpv));
      // init RL clockwise
      RL.add(new QuickHullPoint(lpt, lpv));
      RL.add(new QuickHullPoint(t, v));
    }

    boolean findHighest = false;
    boolean findLowest = false;
    List<QuickHullPoint> foundPoints = new ArrayList<>();

    while (bitSetIdx != -1 || reverseBitSetIdx != -1) { // TODO 判断如果只有两个点，会不会一直循环出不来
      // 如果两个都是-1说明两边都找完所有的点了
      // from left to right
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      bitSetIdx = bitSet.nextSetBit(bitSetIdx + 1);
      if (bitSetIdx != -1) {
        t = pageReader.timeBuffer.getLong(bitSetIdx * 8);
        v = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + bitSetIdx * 8);
        check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
        // note that below if check equals 0, that point is added to both upper and lower, which is
        // necessary
        if (check >= 0) { // p above or on the line connecting FP&LP
          LU.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, LU);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(LU.get(LU.size() - 2)); // 注意是倒数第二个点，不是最后一个
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(LU.get(LU.size() - 2)); // 注意是倒数第二个点，不是最后一个
          }
        }
        if (check <= 0) { // p below or on the line connecting FP&LP
          LL.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, LL);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(LL.get(LL.size() - 2)); // 注意是倒数第二个点，不是最后一个
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(LL.get(LL.size() - 2)); // 注意是倒数第二个点，不是最后一个
          }
        }
        if (findLowest && findHighest) {
          break;
        }
      }
      // from right to left
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      reverseBitSetIdx =
          reverseBitSet.nextSetBit(reverseBitSetIdx + 1); // note this! use reverseBitSet!!
      if (reverseBitSetIdx != -1) {
        t = pageReader.timeBuffer.getLong((count - reverseBitSetIdx - 1) * 8);
        v =
            pageReader.valueBuffer.getDouble(
                pageReader.timeBufferLength + (count - reverseBitSetIdx - 1) * 8);
        // note that below if check equals 0, that point is added to both upper and lower, which is
        // necessary
        check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
        if (check >= 0) { // p is above or on the line connecting FP&LP
          RU.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, RU);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(RU.get(RU.size() - 2)); // 注意是倒数第二个点，不是最后一个
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(RU.get(RU.size() - 2)); // 注意是倒数第二个点，不是最后一个
          }
        }
        if (check <= 0) { // p below or on the line connecting FP&LP
          RL.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, RL);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(RL.get(RL.size() - 2)); // 注意是倒数第二个点，不是最后一个
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(RL.get(RL.size() - 2)); // 注意是倒数第二个点，不是最后一个
          }
        }
        if (findLowest && findHighest) {
          break;
        }
      }
    }
    return foundPoints;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    throw new IOException("no implemented");
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    throw new IOException("no implemented");
  }
}

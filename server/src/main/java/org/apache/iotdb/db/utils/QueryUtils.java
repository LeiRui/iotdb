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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class QueryUtils {

  private QueryUtils() {
    // util class
  }

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata. <br>
   * the caller should guarantee that chunkMetaData and modifications refer to the same time series
   * paths.
   *
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void modifyChunkMetaData(
      List<ChunkMetadata> chunkMetaData, List<Modification> modifications) {

    // sort deletions by startTime. 不过这里默认modification就是delete，暂时没有考虑其它modifications。
    List<Deletion> deletions =
        modifications.stream().map(e -> (Deletion) e).sorted().collect(Collectors.toList());

    // sort chunkMetadatas by startTime
    List<ChunkMetadata> sortedChunkMetadata =
        chunkMetaData.stream().map(e -> (ChunkMetadata) e).sorted().collect(Collectors.toList());

    // TODO add startPos for chunk to iterate deletes to speed up
    int deleteStartIdx = 0;
    // 对于每个chunkMetadata，先过滤掉时间范围上不重叠的删除操作，然后再根据版本高低判断是否应用mod
    for (int metaIndex = 0; metaIndex < sortedChunkMetadata.size(); metaIndex++) {
      ChunkMetadata metaData = sortedChunkMetadata.get(metaIndex);
      //      for (Deletion deletion : deletions) {
      for (int j = deleteStartIdx; j < deletions.size(); j++) {
        // TODO use deleteStartIdx to avoid iterate from the first delete
        Deletion deletion = deletions.get(j);
        if (deletion.getStartTime() > metaData.getEndTime()) {
          break;
        }
        if (metaData.getStartTime() > deletion.getEndTime()) {
          deleteStartIdx = j + 1; // TODO update startPos for chunk to iterate deletes to speed up
          continue;
        }
        // then deals with deletes that overlap in time with the chunk
        // check the version number

        // When the chunkMetadata come from an old TsFile, the method modification.getFileOffset()
        // is gerVersionNum actually. In this case, we compare the versions of modification and
        // mataData to determine whether need to do modify.
        if (metaData.isFromOldTsFile()) {
          if (deletion.getFileOffset() > metaData.getVersion()) {
            doModifyChunkMetaData(deletion, metaData);
          }
          continue;
        }
        // The case modification.getFileOffset() == metaData.getOffsetOfChunkHeader()
        // is not supposed to exist as getFileOffset() is offset containing full chunk,
        // while getOffsetOfChunkHeader() returns the chunk header offset
        if (deletion.getFileOffset() > metaData.getOffsetOfChunkHeader()) {
          doModifyChunkMetaData(deletion, metaData);
        }
      }
    }

    // remove chunks that are completely deleted
    chunkMetaData.removeIf(
        metaData -> {
          if (metaData.getDeleteIntervalList() != null) {
            for (TimeRange range : metaData.getDeleteIntervalList()) {
              if (range.contains(metaData.getStartTime(), metaData.getEndTime())) {
                return true;
              } else {
                if (!metaData.isModified()
                    && range.overlaps(
                        new TimeRange(metaData.getStartTime(), metaData.getEndTime()))) {
                  metaData.setModified(true);
                }
              }
            }
          }
          return false;
        });
  }

  private static void doModifyChunkMetaData(Modification modification, ChunkMetadata metaData) {
    if (modification instanceof Deletion) {
      Deletion deletion = (Deletion) modification;
      //      System.out.println(
      //          "====DEBUG====: doModifyChunkMetaData/insertIntoSortedDeletions: "
      //              + "chunkTime=["
      //              + metaData.getStartTime()
      //              + ","
      //              + metaData.getEndTime()
      //              + "],deleteTime=["
      //              + deletion.getStartTime()
      //              + ","
      //              + deletion.getEndTime()
      //              + "]");
      metaData.insertIntoSortedDeletions(deletion.getStartTime(), deletion.getEndTime());
    }
  }

  public static void fillOrderIndexes(
      QueryDataSource dataSource, String deviceId, boolean ascending) {
    List<TsFileResource> unseqResources = dataSource.getUnseqResources();
    int[] orderIndex = new int[unseqResources.size() + 1];
    AtomicInteger index = new AtomicInteger();
    Map<Integer, Long> intToOrderTimeMap =
        unseqResources.stream()
            .collect(
                Collectors.toMap(
                    key -> index.getAndIncrement(),
                    resource -> resource.getOrderTime(deviceId, ascending)));
    index.set(0);
    intToOrderTimeMap.entrySet().stream()
        .sorted(
            (t1, t2) ->
                ascending
                    ? Long.compare(t1.getValue(), t2.getValue())
                    : Long.compare(t2.getValue(), t1.getValue()))
        .collect(Collectors.toList())
        .forEach(item -> orderIndex[index.getAndIncrement()] = item.getKey());
    dataSource.setUnSeqFileOrderIndex(orderIndex);
  }
}

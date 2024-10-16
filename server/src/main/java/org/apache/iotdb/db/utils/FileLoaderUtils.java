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

import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.Operation;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class FileLoaderUtils {

  private static final Logger logger = LoggerFactory.getLogger(FileLoaderUtils.class);

  private FileLoaderUtils() {}

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.resourceFileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader =
          new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath())) {
        updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
    tsFileResource.setClosed(true);
  }

  public static void updateTsFileResource(
      TsFileSequenceReader reader, TsFileResource tsFileResource) throws IOException {
    for (Entry<String, List<TimeseriesMetadata>> entry :
        reader.getAllTimeseriesMetadata().entrySet()) {
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  /**
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param filter any filter, only used to check time range
   * @author Yuyuan Kang
   */
  public static TimeseriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    long start = System.nanoTime();
    TimeseriesMetadata timeSeriesMetadata;
    if (resource.isClosed()) {
      if (!resource.getTsFile().exists()) {
        return null;
      }
      timeSeriesMetadata =
          TimeSeriesMetadataCache.getInstance()
              .get(
                  new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                      resource.getTsFilePath(),
                      seriesPath.getDevice(),
                      seriesPath.getMeasurement()),
                  allSensors,
                  context.isDebug());
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    } else {
      timeSeriesMetadata = resource.getTimeSeriesMetadata(seriesPath);
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    }

    if (timeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);
      timeSeriesMetadata.setModified(!pathModifications.isEmpty());
      if (timeSeriesMetadata.getStatistics().getStartTime()
          > timeSeriesMetadata.getStatistics().getEndTime()) {
        return null;
      }
      if (filter != null
          && !filter.satisfyStartEndTime(
              timeSeriesMetadata.getStatistics().getStartTime(),
              timeSeriesMetadata.getStatistics().getEndTime())) {
        return null;
      }
    }
    //    IOMonitor.incMeta(duration);
    IOMonitor2.addMeasure(Operation.DCP_A_GET_CHUNK_METADATAS, System.nanoTime() - start);
    return timeSeriesMetadata;
  }

  /**
   * load all chunk metadata of one time series in one file.
   *
   * @param timeSeriesMetadata the corresponding TimeSeriesMetadata in that file.
   */
  public static List<ChunkMetadata> loadChunkMetadataList(TimeseriesMetadata timeSeriesMetadata)
      throws IOException {
    return timeSeriesMetadata.loadChunkMetadataList();
  }

  /**
   * load all page readers in one chunk that satisfying the timeFilter
   *
   * @param chunkMetaData the corresponding chunk metadata
   * @param timeFilter it should be a TimeFilter instead of a ValueFilter
   */
  public static List<IPageReader> loadPageReaderList(ChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    //    long start = System.nanoTime();
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    try {
      IChunkReader chunkReader;
      IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
      if (chunkLoader instanceof MemChunkLoader) {
        MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
        chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
      } else {
        Chunk chunk = chunkLoader.loadChunk(chunkMetaData); // loads chunk data from disk to memory
        chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
        chunkReader =
            new ChunkReader(chunk, timeFilter); // decompress page data, split time&value buffers
        chunkReader.hasNextSatisfiedPage();
      }
      //      long duration = System.nanoTime() - start;
      //      IOMonitor.incDataIOTime(duration);
      return chunkReader.loadPageReaderList();
    } catch (IOException e) {
      logger.error(
          "Something wrong happened while reading chunk from " + chunkMetaData.getFilePath());
      throw e;
    }
  }

  /**
   * load all page readers in one chunk that satisfying the timeFilter
   *
   * @param chunkMetaData the corresponding chunk metadata
   * @param timeFilter it should be a TimeFilter instead of a ValueFilter
   */
  public static PageReader loadPageReaderList4CPV(ChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    //    long start = System.nanoTime();
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    try {
      IChunkReader chunkReader;
      IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
      if (chunkLoader instanceof MemChunkLoader) {
        MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
        chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
      } else {
        Chunk chunk = chunkLoader.loadChunk(chunkMetaData); // loads chunk data from disk to memory
        chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
        chunkReader =
            new ChunkReader(chunk, timeFilter); // decompress page data, split time&value buffers
        chunkReader.hasNextSatisfiedPage();
      }
      //      long duration = System.nanoTime() - start;
      //      IOMonitor.incDataIOTime(duration);
      List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
      if (pageReaderList.size() > 1) {
        // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
        //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
        //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
        //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
        // DIRECTLY),
        //  WHICH WILL INTRODUCE BUGS!
        throw new IOException("Wrong: more than one page in a chunk!");
      }
      return (PageReader) pageReaderList.get(0);
    } catch (IOException e) {
      logger.error(
          "Something wrong happened while reading chunk from " + chunkMetaData.getFilePath());
      throw e;
    }
  }

  public static List<ChunkMetadata> getChunkMetadataList(Path path, String filePath)
      throws IOException {
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(filePath, true);
    return tsFileReader.getChunkMetadataList(path);
  }

  public static TsFileMetadata getTsFileMetadata(String filePath) throws IOException {
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
    return reader.readFileMetadata();
  }
}

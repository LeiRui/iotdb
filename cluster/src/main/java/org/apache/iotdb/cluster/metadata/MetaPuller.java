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

package org.apache.iotdb.cluster.metadata;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetaPuller {

  private static final Logger logger = LoggerFactory.getLogger(MetaPuller.class);
  private MetaGroupMember metaGroupMember;

  private MetaPuller() {}

  private static class MetaPullerHolder {

    private static final MetaPuller INSTANCE = new MetaPuller();
  }

  public void init(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  /**
   * we should not use this function in other place, but only in IoTDB class
   *
   * @return
   */
  public static MetaPuller getInstance() {
    return MetaPullerHolder.INSTANCE;
  }

  /**
   * Pull the all timeseries schemas of given prefixPaths from remote nodes. All prefixPaths must
   * contain the storage group.
   */
  List<MeasurementSchema> pullMeasurementSchemas(List<PartialPath> prefixPaths)
      throws MetadataException {
    logger.debug("{}: Pulling timeseries schemas of {}", metaGroupMember.getName(), prefixPaths);
    // split the paths by the data groups that will hold them
    Map<PartitionGroup, List<PartialPath>> partitionGroupPathMap = new HashMap<>();
    for (PartialPath prefixPath : prefixPaths) {
      PartitionGroup partitionGroup =
          ClusterUtils.partitionByPathTimeWithSync(prefixPath, metaGroupMember);
      partitionGroupPathMap.computeIfAbsent(partitionGroup, g -> new ArrayList<>()).add(prefixPath);
    }

    List<MeasurementSchema> schemas = new ArrayList<>();
    // pull timeseries schema from every group involved
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: pulling schemas of {} and other {} paths from {} groups",
          metaGroupMember.getName(),
          prefixPaths.get(0),
          prefixPaths.size() - 1,
          partitionGroupPathMap.size());
    }
    for (Map.Entry<PartitionGroup, List<PartialPath>> partitionGroupListEntry :
        partitionGroupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<PartialPath> paths = partitionGroupListEntry.getValue();
      pullMeasurementSchemas(partitionGroup, paths, schemas);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: pulled {} schemas for {} and other {} paths",
          metaGroupMember.getName(),
          schemas.size(),
          prefixPaths.get(0),
          prefixPaths.size() - 1);
    }
    return schemas;
  }

  /**
   * Pull timeseries schemas of "prefixPaths" from "partitionGroup" and store them in "results". If
   * this node is a member of "partitionGroup", synchronize with the group leader and collect local
   * schemas. Otherwise pull schemas from one node in the group.
   *
   * @param partitionGroup
   * @param prefixPaths
   * @param results
   */
  private void pullMeasurementSchemas(
      PartitionGroup partitionGroup,
      List<PartialPath> prefixPaths,
      List<MeasurementSchema> results) {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the node is in the target group, synchronize with leader should be enough
      try {
        metaGroupMember
            .getLocalDataMember(partitionGroup.getHeader(), "Pull timeseries of " + prefixPaths)
            .syncLeader(null);
      } catch (CheckConsistencyException e) {
        logger.warn("Failed to check consistency.", e);
      }
      int preSize = results.size();
      for (PartialPath prefixPath : prefixPaths) {
        IoTDB.metaManager.collectSeries(prefixPath, results);
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Pulled {} timeseries schemas of {} and other {} paths from local",
            metaGroupMember.getName(),
            results.size() - preSize,
            prefixPaths.get(0),
            prefixPaths.size() - 1);
      }
      return;
    }

    // pull schemas from a remote node
    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPaths(
        prefixPaths.stream().map(PartialPath::getFullPath).collect(Collectors.toList()));

    for (Node node : partitionGroup) {
      if (pullMeasurementSchemas(node, pullSchemaRequest, results)) {
        break;
      }
    }
  }

  private boolean pullMeasurementSchemas(
      Node node, PullSchemaRequest request, List<MeasurementSchema> results) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Pulling timeseries schemas of {} and other {} paths from {}",
          metaGroupMember.getName(),
          request.getPrefixPaths().get(0),
          request.getPrefixPaths().size() - 1,
          node);
    }

    List<MeasurementSchema> schemas = null;
    try {
      schemas = pullMeasurementSchemas(node, request);
    } catch (IOException | TException e) {
      logger.error(
          "{}: Cannot pull timeseries schemas of {} and other {} paths from {}",
          metaGroupMember.getName(),
          request.getPrefixPaths().get(0),
          request.getPrefixPaths().size() - 1,
          node,
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(
          "{}: Cannot pull timeseries schemas of {} and other {} paths from {}",
          metaGroupMember.getName(),
          request.getPrefixPaths().get(0),
          request.getPrefixPaths().size() - 1,
          node,
          e);
    }

    if (schemas != null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Pulled {} timeseries schemas of {} and other {} paths from {} of {}",
            metaGroupMember.getName(),
            schemas.size(),
            request.getPrefixPaths().get(0),
            request.getPrefixPaths().size() - 1,
            node,
            request.getHeader());
      }
      results.addAll(schemas);
      return true;
    }
    return false;
  }

  private List<MeasurementSchema> pullMeasurementSchemas(Node node, PullSchemaRequest request)
      throws TException, InterruptedException, IOException {
    List<MeasurementSchema> schemas;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          metaGroupMember
              .getClientProvider()
              .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      schemas = SyncClientAdaptor.pullMeasurementSchema(client, request);
    } else {
      try (SyncDataClient syncDataClient =
          metaGroupMember
              .getClientProvider()
              .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {

        // only need measurement name
        try {
          PullSchemaResp pullSchemaResp = syncDataClient.pullMeasurementSchema(request);
          ByteBuffer buffer = pullSchemaResp.schemaBytes;
          int size = buffer.getInt();
          schemas = new ArrayList<>(size);
          for (int i = 0; i < size; i++) {
            schemas.add(MeasurementSchema.deserializeFrom(buffer));
          }
        } catch (TException e) {
          // the connection may be broken, close it to avoid it being reused
          syncDataClient.getInputProtocol().getTransport().close();
          throw e;
        }
      }
    }

    return schemas;
  }
}

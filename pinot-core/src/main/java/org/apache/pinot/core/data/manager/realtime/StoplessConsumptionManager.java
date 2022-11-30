/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// FIXME add javadoc
public class StoplessConsumptionManager {

  private static final StoplessConsumptionManager INSTANCE = new StoplessConsumptionManager();
  private static final Logger LOGGER = LoggerFactory.getLogger(StoplessConsumptionManager.class);
  private static final int UNIQUE_SEQUENCE_NUMBER = -1;

  private final ExecutorService _executorService;
  private final Map<String, LLRealtimeSegmentDataManager> _primarySegmentNameToTempSegmentDataManager;

  private StoplessConsumptionManager() {
    _executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("stopless-consumption-manager-thread-%d").build());
    _primarySegmentNameToTempSegmentDataManager = new ConcurrentHashMap<>();
  }

  public void kickOffTemporaryConsumption(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
      RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig,
      Schema schema, LLCSegmentName llcSegmentName, Semaphore partitionGroupConsumerSemaphore,
      ServerMetrics serverMetrics, StreamPartitionMsgOffset startOffset, int partitionGroupId) {
    _executorService.submit(() -> {
      String primarySegmentName = llcSegmentName.getSegmentName();
      LOGGER.info("Start stopless consumption for partition group {} after segment {}",
          llcSegmentName.getPartitionGroupId(), primarySegmentName);

      // create temp segment name
      LLCSegmentName tempLLCSegmentName =
          new LLCSegmentName(llcSegmentName.getTableName(), llcSegmentName.getPartitionGroupId(),
              UNIQUE_SEQUENCE_NUMBER, llcSegmentName.getCreationTimeMs());

      // create temp segment zk metadata
      String tempSegmentName = tempLLCSegmentName.getSegmentName();
      SegmentZKMetadata tempSegmentZKMetadata =
          new SegmentZKMetadata(new ZNRecord(segmentZKMetadata.toZNRecord(), tempSegmentName));
      tempSegmentZKMetadata.setStartOffset(startOffset.toString());

      // create temp segment data manager
      LLRealtimeSegmentDataManager tempSegmentDataManager =
          new LLRealtimeSegmentDataManager(tempSegmentZKMetadata, tableConfig, realtimeTableDataManager,
              resourceDataDir, indexLoadingConfig, schema, tempLLCSegmentName, partitionGroupConsumerSemaphore,
              serverMetrics, null, null, true); // upsert & dedup is not supported

      // add the temporary segment data manager to table data manager
      realtimeTableDataManager.registerSegment(tempSegmentName, tempSegmentDataManager);
      _primarySegmentNameToTempSegmentDataManager.put(primarySegmentName, tempSegmentDataManager);
    });
  }

  public void acquireConsumerSemaphore(String primarySegmentName) {
    LLRealtimeSegmentDataManager tempSegmentDataManager =
        _primarySegmentNameToTempSegmentDataManager.get(primarySegmentName);
    if (tempSegmentDataManager != null) {
      tempSegmentDataManager.acquireConsumerSemaphore();
    }
  }

  public static StoplessConsumptionManager getInstance() {
    return INSTANCE;
  }

  public Collection<String> getTemporarySegmentsFor(List<String> primarySegmentNames) {
    Set<String> primarySegmentSet = new HashSet<>(primarySegmentNames);
    Set<String> tempSegments = new HashSet<>();
    _primarySegmentNameToTempSegmentDataManager.forEach((primSeg, segDataManager) -> {
      if (primarySegmentSet.contains(primSeg)) {
        tempSegments.add(segDataManager.getSegmentName());
      }
    });
    return tempSegments;
  }

  public LLRealtimeSegmentDataManager validateAndGetTemporarySegment(int newPartitionGroupId,
      SegmentZKMetadata newSegmentZKMetadata, TableConfig newTableConfig, Schema newSchema) {
    for (Map.Entry<String, LLRealtimeSegmentDataManager> entry : _primarySegmentNameToTempSegmentDataManager.entrySet()) {
      String primarySegmentName = entry.getKey();
      LLRealtimeSegmentDataManager tempSegmentDataManager = entry.getValue();
      if (tempSegmentDataManager.getPartitionGroupId() == newPartitionGroupId) {
        if (!tempSegmentDataManager.getStartOffset().toString().equals(newSegmentZKMetadata.getStartOffset())
            || !newTableConfig.equals(tempSegmentDataManager.getTableConfig())
            || !areEqual(newSchema, tempSegmentDataManager.getSchema())) {
          // TODO invalidate
          _primarySegmentNameToTempSegmentDataManager.remove(primarySegmentName);
          return null;
        } else {
          _primarySegmentNameToTempSegmentDataManager.remove(primarySegmentName);
          tempSegmentDataManager.makeTemporarySegmentPermanent(newSegmentZKMetadata, newSchema);
          return tempSegmentDataManager;
        }
      }
    }
    return null;
  }

  private boolean areEqual(Schema newSchema, Schema existingSchema) {
    Schema newSchemaCloned = newSchema.clone();
    Schema existingSchemaCloned = existingSchema.clone();
    newSchemaCloned.removeField(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME);
    existingSchemaCloned.removeField(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME);
    return newSchemaCloned.equals(existingSchemaCloned);
  }
}

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
package org.apache.pinot.segment.local.dedup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;


class ConcurrentMapPartitionDedupMetadataManager implements PartitionDedupMetadataManager {

  private static final Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ConcurrentMapPartitionDedupMetadataManager.class);
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  @VisibleForTesting
  final Cache<Object, IndexSegment> _primaryKeyToSegmentMap = CacheBuilder.newBuilder()
//      .expireAfterWrite(8, TimeUnit.HOURS)
      .expireAfterWrite(500, TimeUnit.MILLISECONDS)
      .build();

  // params for clean up
  int _msgCounter = 0;
  private final int _cleanUpThreshold;

  public ConcurrentMapPartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns,
      int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
    _cleanUpThreshold = 10_000 + partitionId * 250;
    LOGGER.info("Using time-based dedup manager for table: {}, partition: {}, cleanup threshold: {}", tableNameWithType,
        partitionId, _cleanUpThreshold);
  }

  public void addSegment(IndexSegment segment) {
    // Do nothing
  }

  public void removeSegment(IndexSegment segment) {
    // Do nothing
  }

  @VisibleForTesting
  Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment) {
    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : _primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = _primaryKeyColumns.size();
    return new Iterator<PrimaryKey>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public PrimaryKey next() {
        Object[] values = new Object[numPrimaryKeyColumns];
        for (int i = 0; i < numPrimaryKeyColumns; i++) {
          Object value = columnToReaderMap.get(_primaryKeyColumns.get(i)).getValue(_docId);
          if (value instanceof byte[]) {
            value = new ByteArray((byte[]) value);
          }
          values[i] = value;
        }
        _docId++;
        return new PrimaryKey(values);
      }
    };
  }

  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    boolean present =
        _primaryKeyToSegmentMap.asMap().putIfAbsent(HashUtils.hashPrimaryKey(pk, _hashFunction), indexSegment) != null;
    if (!present) {
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          _primaryKeyToSegmentMap.size());
    }
    if (++_msgCounter % _cleanUpThreshold == 0) {
      _primaryKeyToSegmentMap.cleanUp();
    }
    return present;
  }
}

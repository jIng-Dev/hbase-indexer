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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes; 


/**
 * Essentially just logs and counts replication events arriving from a region server of the HBase
 * slave cluster.
 */ 
public final class CountingReplicationSlaveConnection extends ReplicationSlaveConnection { 
  
  static final AtomicLong NUM_RECEIVED_ACTIONS = new AtomicLong(0);
  static final List<String> RECEIVED_ROW_KEYS = Collections.synchronizedList(new ArrayList<String>());
  static final List<String> RECEIVED_CELL_VALUES = Collections.synchronizedList(new ArrayList<String>());

  private static final Log LOG = LogFactory.getLog(CountingReplicationSlaveConnection.class);
  
  /** This constructor will be called by the slave region server ReplicationSink.batch() */
  public CountingReplicationSlaveConnection(Configuration conf, ExecutorService pool, User user) throws IOException {
    super(conf, pool, user);
  } 

  @Override 
  protected void replicateBatch(List<? extends Row> actions, Object[] results, TableName tableName) 
      throws IOException, InterruptedException {    
    LOG.info("replicateBatch() received " + actions.size() + " events from table " + tableName);
    for (Row row : actions) {
      Mutation mutation = (Mutation) row;
      String rowKeyStr = Bytes.toStringBinary(mutation.getRow());
      LOG.info("receivedRowKey=" + rowKeyStr);
      RECEIVED_ROW_KEYS.add(rowKeyStr);

      CellScanner cells = mutation.cellScanner();
      while (cells.advance()) {
        Cell cell = cells.current();
//        if (cell.getTimestamp() < subscriptionTimestamp) {
//          continue;
//        }
        ByteBuffer rowKey = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        ByteBuffer value = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
//        String str = Bytes.toStringBinary(value);
        String str = Bytes.toStringBinary(kv.getValueArray());
//        String str = kv.getKeyString();
        LOG.info("receivedCellValue=" + str);
        RECEIVED_CELL_VALUES.add(str);
      }
      NUM_RECEIVED_ACTIONS.incrementAndGet();              
    }
  }

}
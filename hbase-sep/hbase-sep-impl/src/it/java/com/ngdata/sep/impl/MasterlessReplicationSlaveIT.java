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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

public class MasterlessReplicationSlaveIT {

  private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
  private static final byte[] DATA_COL_FAMILY = Bytes.toBytes("datacf");
  private static final byte[] PAYLOAD_COL_FAMILY = Bytes.toBytes("payloadcf");
  
  private static final String SUBSCRIPTION_NAME = "myIndexer";
  private static final String SLAVE_ROOT_ZK_PATH = "/ngdata/sep/hbase-slave"; 
  private static final String MASTERLESS_ROOT_ZK_PATH = "/ngdata/sep/hbase-masterless";

  private static final int WAIT_TIMEOUT = 10000;

  private static Configuration clusterConf;
  private static HBaseTestingUtility hbaseTestUtil;
  private static Table htable;
  private static Connection connection;
  private static Admin admin;
  private static ZooKeeperItf zkItf;
  
  private static final Log log = LogFactory.getLog(MasterlessReplicationSlaveIT.class);

  private List<HRegionServer> regionServers = new ArrayList<HRegionServer>();
  private List<String> replicationPeerIds = new ArrayList<String>();
  
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    LogManager.getLogger(Class.forName("org.apache.hadoop.hbase.client.ConnectionImplementation")).setLevel(Level.ERROR);
    // create source hbase cluster with an empty table
    clusterConf = HBaseConfiguration.create();
    clusterConf.setLong("replication.source.sleepforretries", 50);
    clusterConf.setInt("hbase.master.info.port", -1);
    clusterConf.setInt("hbase.regionserver.info.port", -1);

    hbaseTestUtil = new HBaseTestingUtility(clusterConf);
    hbaseTestUtil.startMiniZKCluster(1);
    hbaseTestUtil.startMiniCluster(1);
    zkItf = ZkUtil.connect("localhost:" + hbaseTestUtil.getZkCluster().getClientPort(), 30000);

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(DATA_COL_FAMILY)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(PAYLOAD_COL_FAMILY)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build();

    connection = ConnectionFactory.createConnection(clusterConf);
    admin = connection.getAdmin(); 
    admin.createTable(tableDescriptor);
    htable = connection.getTable(TableName.valueOf(TABLE_NAME));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (connection != null) {
      connection.close();
    }
    if (zkItf != null) {
      zkItf.close();
    }
    hbaseTestUtil.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    int numPeers = 2;
//    int numPeers = 1;
    for (int i = 0; i < numPeers; i++) {
      /* Start one or more slave region servers */
      
      Configuration masterlessConf = HBaseConfiguration.create();
      masterlessConf.addResource("hbase-indexer-site-masterless-defaults.xml");
      masterlessConf.addResource("hbase-indexer-site.xml"); // allow custom overrides, also see HBaseIndexerConfiguration.addHbaseIndexerResources
      masterlessConf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, CountingReplicationSlaveConnection.class.getName());
      masterlessConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, MASTERLESS_ROOT_ZK_PATH);
      masterlessConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseTestUtil.getZkCluster().getClientPort());
      masterlessConf.setInt(HConstants.REGIONSERVER_PORT, 0); // let the system pick up an ephemeral port
 
      HRegionServer regionServer = new HRegionServer(masterlessConf);
      regionServers.add(regionServer);
      Thread thread = new Thread() {
        @Override
        public void run() {
          regionServer.run();
        }
      };
      thread.setDaemon(true);
      thread.start();
      regionServer.waitForServerOnline();
      
      /*
       * manually create zk node "/ngdata/sep/hbase-slave/Indexer_myIndexer/hbaseid-->UUID" and
       * "/ngdata/sep/hbase-slave/Indexer_myIndexer/rs" 
       */
      String subscriptionId = "Indexer_" + SUBSCRIPTION_NAME + i;
      String basePath = SLAVE_ROOT_ZK_PATH + "/" + subscriptionId;
      UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(subscriptionId));

      ZkUtil.createPath(zkItf, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
      ZkUtil.createPath(zkItf, basePath + "/rs");

      String hostName = Strings
          .domainNamePointerToHostName(DNS.getDefaultHost(masterlessConf.get("hbase.regionserver.dns.interface", "default"),
              masterlessConf.get("hbase.regionserver.dns.nameserver", "default")));
      ServerName serverName = ServerName.valueOf(hostName, regionServer.getRpcServer().getListenerAddress().getPort(),
          regionServer.getStartcode());
      String zkNodePath = basePath + "/rs/" + serverName.getServerName();
      log.info("zkNodePathForSlave=" + zkNodePath);
      // Publish slave existence in ZooKeeper
      zkItf.create(zkNodePath, null, CreateMode.EPHEMERAL);

      /*
       * Plus also
       * add replication peer via official hbase Admin.addReplicationPeer() into zk node
       * /hbase/replication/peers/Indexer_myIndexer that contains protobuf that contains
       * "<zkhost>:/ngdata/sep/hbase-slave/Indexer_myIndexer" which is how hbase source cluster
       * knows where to find the list of regionservers to replicate to
       */
      String clusterKey = masterlessConf.get(HConstants.ZOOKEEPER_QUORUM) + ":" + hbaseTestUtil.getZkCluster().getClientPort()
          + ":" + basePath;
      log.info("clusterKey=" + clusterKey);
      admin.addReplicationPeer(subscriptionId, new ReplicationPeerConfig().setClusterKey(clusterKey));
      replicationPeerIds.add(subscriptionId);
    }
  }

  @After
  public void tearDown() throws IOException {
    for (HRegionServer regionServer : regionServers) {
      regionServer.stop("tearDown");
    }
    for (String replicationPeerId : replicationPeerIds) {
      admin.removeReplicationPeer(replicationPeerId);
    }
  }

  @Test
  public void testSimplePuts() throws IOException {
    Assert.assertTrue(replicationPeerIds.size() > 0);
    for (int i = 0; i < 3; i++) { // add some rows to the source hbase table
      Put put = new Put(Bytes.toBytes("row " + i));
      put.addColumn(DATA_COL_FAMILY, Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
      htable.put(put);
    }
    
    // verify that the rows are replicated and now appear in the slave hbase table
    int expectedNumEvents = 3 * replicationPeerIds.size();
    waitForEvents(expectedNumEvents); 
    Assert.assertEquals(expectedNumEvents, CountingReplicationSlaveConnection.NUM_RECEIVED_ACTIONS.get());
    log.info("receivedRowKeys=" + CountingReplicationSlaveConnection.RECEIVED_ROW_KEYS);
    log.info("receivedCellValues=" + CountingReplicationSlaveConnection.RECEIVED_CELL_VALUES);
    Assert.assertEquals(expectedNumEvents, CountingReplicationSlaveConnection.RECEIVED_ROW_KEYS.size());
    Assert.assertEquals(expectedNumEvents, CountingReplicationSlaveConnection.RECEIVED_CELL_VALUES.size());
  }

  private void waitForEvents(int expectedNumEvents) {
    long start = System.currentTimeMillis();
    while (CountingReplicationSlaveConnection.NUM_RECEIVED_ACTIONS.get() < expectedNumEvents) {
      if (System.currentTimeMillis() - start > WAIT_TIMEOUT) {
        throw new RuntimeException("Waited too long on " + expectedNumEvents + ", only have "
            + CountingReplicationSlaveConnection.NUM_RECEIVED_ACTIONS.get() + " after " + WAIT_TIMEOUT + " milliseconds");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

}

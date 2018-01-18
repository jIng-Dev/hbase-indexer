/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.SepModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class SepModelImpl implements SepModel {
    
    // Replace '-' with unicode "CANADIAN SYLLABICS HYPHEN" character in zookeeper to avoid issues
    // with HBase replication naming conventions
    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';
    
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private final String baseZkPath;
    private final String zkQuorumString;
    private final int zkClientPort;
    private Log log = LogFactory.getLog(getClass());

    public SepModelImpl(ZooKeeperItf zk, Configuration hbaseConf) {
        
        this.zkQuorumString = hbaseConf.get("hbase.zookeeper.quorum");
        if (zkQuorumString == null) {
            throw new IllegalStateException("hbase.zookeeper.quorum not supplied in configuration");
        }
        if (zkQuorumString.contains(":")) {
            throw new IllegalStateException("hbase.zookeeper.quorum should not include port number, got " + zkQuorumString);
        }
        try {
            this.zkClientPort = Integer.parseInt(hbaseConf.get("hbase.zookeeper.property.clientPort"));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Non-numeric zookeeper client port", e);
        }
        
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.baseZkPath = hbaseConf.get(ZK_ROOT_NODE_CONF_KEY, DEFAULT_ZK_ROOT_NODE);
    }

    @Override
    public void addSubscription(String name, String... tableNames) throws InterruptedException, KeeperException, IOException {
      if (!addSubscriptionSilent(name, tableNames)) {
          throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
      }
    }

    @Override
    public boolean addSubscriptionSilent(String name, String... tableNames) throws InterruptedException, KeeperException, IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        try {
            String internalName = toInternalSubscriptionName(name);
            for (ReplicationPeerDescription peer : admin.listReplicationPeers()) {
                if (internalName.equals(peer.getPeerId())) {
                    return false;
                }
            }

            String basePath = baseZkPath + "/" + internalName;
            UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
            log.debug("Before addReplicationPeer - registering /hbaseid and /rs for basePath=" + basePath);
            ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
            ZkUtil.createPath(zk, basePath + "/rs");


            try {
                ReplicationPeerConfig peerConfig = new ReplicationPeerConfig()
                    .setClusterKey(zkQuorumString + ":" + zkClientPort + ":" + basePath);
                if (tableNames.length > 0) {
                    /*
                     * CDH-62730: Performance Enhancement: It is much more efficient to filter away events
                     * from irrelevant tables already on the hbase source cluster, i.e. as early as possible.
                     * This can drastically reduce the traffic across the wire from source hbase cluster to
                     * hbase-indexer. Without this patch each indexer receives all updates from all hbase
                     * tables, regardless of whether these tables are actually indexed or not.
                     * 
                     * Consider the case where many HBase tables are indexed by many indexers to many distinct
                     * Solr collections, for example one HBase table into one distinct Solr collection, for
                     * each of N=100 indexers definitions. Before this patch each update to ANY HBase table
                     * caused an event to be sent across the wire to EACH OF the N indexers. After this patch
                     * each update to an HBase table causes an event to be sent across the wire to *one* of
                     * the N indexers, the one indexer that's actually interested in updates for this
                     * particular HBase table.
                     * 
                     * Unfortunately, for the time being we can only do this performance improvement if the
                     * table name is not a regex, i.e. if the table name in the hbase indexer definition xml
                     * file does not start with "regex:", for example "regex:user.*". If it is a regex then
                     * performance remains unchanged.
                     */
                    Map<TableName, List<String>> tablesToReplicate = new HashMap();
                    for (String tableName : tableNames) {
                        List<String> columnFamilies = new ArrayList(); // this means replicate all colFamilies
                        tablesToReplicate.put(TableName.valueOf(tableName), columnFamilies);
                    }
                    peerConfig.setReplicateAllUserTables(false);
                    peerConfig.setTableCFsMap(tablesToReplicate);
                }
                admin.addReplicationPeer(internalName, peerConfig);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot add existing peer")) {
                    return false;
                }
                throw e;
            } catch (Exception e) {
                // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                if (e instanceof InterruptedException) {
                    throw (InterruptedException)e;
                } else if (e instanceof KeeperException) {
                    throw (KeeperException)e;
                } else {
                    throw new IOException(e);
                }
            }
            log.debug("Finished addReplicationPeer with clusterkey=" + zkQuorumString + ":" + zkClientPort + ":" + basePath);

            return true;
        } finally {
            Closer.close(admin.getConnection());
            Closer.close(admin);
        }
    }

    @Override
    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    @Override
    public boolean removeSubscriptionSilent(String name) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        try {
            String internalName = toInternalSubscriptionName(name);
            log.debug("Initiating removeReplicationPeer with " + internalName);
            List<String> peerIds = new ArrayList<String>();
            for (ReplicationPeerDescription peer : admin.listReplicationPeers()) {
                peerIds.add(peer.getPeerId());
            }
            if (!peerIds.contains(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                log.debug("Before removeReplicationPeer with " + internalName);
                try {
                    admin.removeReplicationPeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            String basePath = baseZkPath + "/" + internalName;
            log.debug("After removeReplicationPeer with " + internalName);
            log.debug("Before removeReplicationPeer deletion of zk node " + basePath);
            try {
                ZkUtil.deleteNode(zk, basePath + "/hbaseid");
                for (String child : zk.getChildren(basePath + "/rs", false)) {
                    ZkUtil.deleteNode(zk, basePath + "/rs/" + child);
                }
                ZkUtil.deleteNode(zk, basePath + "/rs");
                ZkUtil.deleteNode(zk, basePath);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (KeeperException ke) {
                log.error("Cleanup in zookeeper failed on " + basePath, ke);
            }
            log.debug("After removeReplicationPeer deletion of zk node " + basePath);
            return true;
        } finally {
            Closer.close(admin.getConnection());
            Closer.close(admin);
        }
    }

    @Override
    public boolean hasSubscription(String name) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        try {
            String internalName = toInternalSubscriptionName(name);
            for (ReplicationPeerDescription peer : admin.listReplicationPeers()) {
                if (internalName.equals(peer.getPeerId())) {
                    return true;
                }
            }
            return false;
        } finally {
            Closer.close(admin.getConnection());
            Closer.close(admin);
        }
    }
    
    static String toInternalSubscriptionName(String subscriptionName) {
        if (subscriptionName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U1400");
        }
        return subscriptionName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }

    static String toExternalSubscriptionName(String subscriptionName) {
        return subscriptionName.replace(INTERNAL_HYPHEN_REPLACEMENT, '-');
    }
}

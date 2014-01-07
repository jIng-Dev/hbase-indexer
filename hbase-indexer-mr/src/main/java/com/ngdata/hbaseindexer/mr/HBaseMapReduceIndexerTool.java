/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.mr;

import java.io.ByteArrayInputStream;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.hadoop.ForkedMapReduceIndexerTool;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfReader;
import com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper;

/**
 * Top-level tool for running MapReduce-based indexing pipelines over HBase tables.
 */
public class HBaseMapReduceIndexerTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ForkedMapReduceIndexerTool.class);

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new HBaseMapReduceIndexerTool(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        HBaseIndexingOptions hbaseIndexingOpts = new HBaseIndexingOptions(getConf());
        Integer exitCode = new HBaseIndexerArgumentParser().parseArgs(args, getConf(), hbaseIndexingOpts);
        if (exitCode != null) {
          return exitCode;
        }

        return run(hbaseIndexingOpts);
    }

    public int run(HBaseIndexingOptions hbaseIndexingOpts) throws Exception {

        if (hbaseIndexingOpts.isDryRun) {
            return new IndexerDryRun(hbaseIndexingOpts, getConf(), System.out).run();
        }

        long programStartTime = System.currentTimeMillis();
        Configuration conf = getConf();

        IndexingSpecification indexingSpec = hbaseIndexingOpts.getIndexingSpecification();

        conf.set(HBaseIndexerMapper.INDEX_CONFIGURATION_CONF_KEY, indexingSpec.getIndexConfigXml());
        conf.set(HBaseIndexerMapper.INDEX_NAME_CONF_KEY, indexingSpec.getIndexerName());
        conf.set(HBaseIndexerMapper.TABLE_NAME_CONF_KEY, indexingSpec.getTableName());
        HBaseIndexerMapper.configureIndexConnectionParams(conf, indexingSpec.getIndexConnectionParams());
        
        IndexerConf indexerConf = new XmlIndexerConfReader().read(new ByteArrayInputStream(
                    indexingSpec.getIndexConfigXml().getBytes()));

        String morphlineFile = indexerConf.getGlobalParams().get(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM);
        if (hbaseIndexingOpts.morphlineFile != null) {
            morphlineFile = hbaseIndexingOpts.morphlineFile.getPath();
        }
        if (morphlineFile != null) {
            conf.set(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, new File(morphlineFile).getName());
            ForkedMapReduceIndexerTool.addDistributedCacheFile(new File(morphlineFile), conf);
        }
        
        String morphlineId = indexerConf.getGlobalParams().get(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM);
        if (hbaseIndexingOpts.morphlineId != null) {
            morphlineId = hbaseIndexingOpts.morphlineId;
        }
        if (morphlineId != null) {
            conf.set(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM, morphlineId);
        }

        conf.setBoolean(HBaseIndexerMapper.INDEX_DIRECT_WRITE_CONF_KEY, hbaseIndexingOpts.isDirectWrite());
        
        if (hbaseIndexingOpts.fairSchedulerPool != null) {
            conf.set("mapred.fairscheduler.pool", hbaseIndexingOpts.fairSchedulerPool);
        }
        
        // switch off a false warning about allegedly not implementing Tool
        // also see http://hadoop.6.n7.nabble.com/GenericOptionsParser-warning-td8103.html
        // also see https://issues.apache.org/jira/browse/HADOOP-8183
        getConf().setBoolean("mapred.used.genericoptionsparser", true);

        if (hbaseIndexingOpts.log4jConfigFile != null) {
            Utils.setLogConfigFile(hbaseIndexingOpts.log4jConfigFile, getConf());
            ForkedMapReduceIndexerTool.addDistributedCacheFile(hbaseIndexingOpts.log4jConfigFile, conf);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName(getClass().getSimpleName() + "/" + HBaseIndexerMapper.class.getSimpleName());
        job.setJarByClass(HBaseIndexerMapper.class);
//        job.setUserClassesTakesPrecedence(true);

        TableMapReduceUtil.initTableMapperJob(
                                    indexingSpec.getTableName(),
                                    hbaseIndexingOpts.getScan(),
                                    HBaseIndexerMapper.class,
                                    Text.class,
                                    SolrInputDocumentWritable.class,
                                    job);

        int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
        //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
        LOG.info("Cluster reports {} mapper slots", mappers);

        LOG.info("Using these parameters: " +
            "reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
            new Object[] {hbaseIndexingOpts.reducers, hbaseIndexingOpts.shards, hbaseIndexingOpts.fanout, hbaseIndexingOpts.maxSegments});

        if (hbaseIndexingOpts.isDirectWrite()) {
            // Run a mapper-only MR job that sends index documents directly to a live Solr instance.
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
            if (!ForkedMapReduceIndexerTool.waitForCompletion(job, hbaseIndexingOpts.isVerbose)) {
                return -1; // job failed
            }
            CloudSolrServer solrServer = new CloudSolrServer(hbaseIndexingOpts.zkHost);
            solrServer.setDefaultCollection(hbaseIndexingOpts.collection);
            solrServer.commit(false, false);
            solrServer.shutdown();
            ForkedMapReduceIndexerTool.goodbye(job, programStartTime);
            return 0;
        } else {
            FileSystem fileSystem = FileSystem.get(getConf());
            
            if (fileSystem.exists(hbaseIndexingOpts.outputDir)) {
                if (hbaseIndexingOpts.overwriteOutputDir) {
                    LOG.info("Removing existing output directory {}", hbaseIndexingOpts.outputDir);
                    if (!fileSystem.delete(hbaseIndexingOpts.outputDir, true)) {
                        LOG.error("Deleting output directory '{}' failed", hbaseIndexingOpts.outputDir);
                        return -1;
                    }
                } else {
                    LOG.error("Output directory '{}' already exists. Run with --overwrite-output-dir to " +
                    		"overwrite it, or remove it manually", hbaseIndexingOpts.outputDir);
                    return -1;
                }
            }
            
            int exitCode = ForkedMapReduceIndexerTool.runIndexingPipeline(
                                            job, getConf(), hbaseIndexingOpts.asOptions(),
                                            programStartTime,
                                            fileSystem,
                                            null, -1, // File-based parameters
                                            -1, // num mappers, only of importance for file-based indexing
                                            hbaseIndexingOpts.reducers
                                            );


            if (hbaseIndexingOpts.isGeneratedOutputDir()) {
                LOG.info("Deleting generated output directory " + hbaseIndexingOpts.outputDir);
                fileSystem.delete(hbaseIndexingOpts.outputDir, true);
            }
            return exitCode;
        }

    }

}

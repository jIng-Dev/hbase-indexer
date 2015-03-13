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
package com.ngdata.hbaseindexer.cli;

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionsJsonSerDeser;

import com.ngdata.hbaseindexer.model.impl.UpdateIndexerInputJsonSerDeser;
import com.ngdata.hbaseindexer.model.impl.UpdateIndexerInputJsonSerDeser.UpdateIndexerInput;
import com.ngdata.hbaseindexer.util.http.HttpUtil;
import java.util.List;
import joptsimple.OptionSet;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

/**
 * CLI tool to update an existing {@link IndexerDefinition}s in the {@link IndexerModel}.
 */
public class UpdateIndexerCli extends AddOrUpdateIndexerCli {

    public static void main(String[] args) throws Exception {
        new UpdateIndexerCli().run(args);
    }

    @Override
    protected String getCmdName() {
        return "update-indexer";
    }

    public void run(OptionSet options) throws Exception {
        super.run(options);

        String indexName = nameOption.value(options);

        if (!options.has("http")) {
            if (!model.hasIndexer(indexName)) {
                throw new CliException("Indexer does not exist: " + indexName);
            }

            IndexerDefinition newIndexer = null;
            String lock = model.lockIndexer(indexName);
            try {
                IndexerDefinition indexer = model.getFreshIndexer(indexName);

                IndexerDefinitionBuilder builder = buildIndexerDefinition(options, indexer);
                newIndexer = builder.build();

                if (newIndexer.equals(indexer)) {
                    System.out.println("Index already matches the specified settings, did not update it.");
                } else {
                    model.updateIndexer(newIndexer, lock);
                    System.out.println("Index updated: " + indexName);
                }
            } finally {
                // In case we requested deletion of an index, it might be that the lock is already removed
                // by the time we get here as part of the index deletion.
                boolean ignoreMissing = newIndexer != null
                        && newIndexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED;
                model.unlockIndexer(lock, ignoreMissing);
            }
        } else {
          updateIndexerHttp(options, indexName);
        }
    }

    private void updateIndexerHttp(OptionSet options, String indexerName) throws Exception {
        String path = httpOption.value(options);
        IndexerDefinition indexer = getFreshIndexer(indexerName, path);
        if (indexer == null) {
            throw new CliException("Indexer does not exist: " + indexerName);
        }

        IndexerDefinitionBuilder builder = buildIndexerDefinition(options, indexer);
        IndexerDefinition newIndexer = builder.build();

        UpdateIndexerInput input = new UpdateIndexerInput(newIndexer, indexer);
        byte [] jsonBytes = UpdateIndexerInputJsonSerDeser.INSTANCE.toJsonBytes(input);
        String putPath = path + (path.endsWith("/")?"":"/") + indexerName;
        HttpPut httpPut = new HttpPut(putPath);
        httpPut.setEntity(new ByteArrayEntity(jsonBytes));
        byte [] response = HttpUtil.sendRequest(httpPut);
    }

    private IndexerDefinition getFreshIndexer(String indexerName, String path) throws Exception {
        // FixMe: should we have an individual GetIndexer command so we don't have to iterate?
        HttpGet httpGet = new HttpGet(path);
        byte [] response = HttpUtil.sendRequest(httpGet);
        List<IndexerDefinitionBuilder> builders = IndexerDefinitionsJsonSerDeser.INSTANCE.fromJsonBytes(response);
        for (IndexerDefinitionBuilder builder : builders) {
            IndexerDefinition def = builder.build();
            if (indexerName.equals(def.getName())) {
              return def;
            }
        }
        return null;
    }
}

/*
 * Copyright 2015 NGDATA nv
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
package com.ngdata.hbaseindexer.model.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.util.json.JsonFormatException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Used for serializing/deserializing the update format for HTTP calls.
 * The format is an ObjectNode with two fields:
 * newIndexer: IndexerDefinition to update
 * baseIndexer: IndexerDefinition that was used to generate the newIndexer.
 */
public class UpdateIndexerInputJsonSerDeser {
  public static UpdateIndexerInputJsonSerDeser INSTANCE = new UpdateIndexerInputJsonSerDeser();

  public static class UpdateIndexerInput {
    private IndexerDefinition newIndexer;
    private IndexerDefinition baseIndexer;

    public UpdateIndexerInput(IndexerDefinition newIndexer, IndexerDefinition baseIndexer) {
      this.newIndexer = newIndexer;
      this.baseIndexer = baseIndexer;
    }

    public IndexerDefinition getNewIndexer() { return newIndexer; }
    public IndexerDefinition getBaseIndexer() { return baseIndexer; }
  }

  public byte [] toJsonBytes(UpdateIndexerInput input) {
    ObjectNode newIndexerJson =
      IndexerDefinitionJsonSerDeser.INSTANCE.toJson(input.getNewIndexer());
    ObjectNode baseIndexerJson =
      IndexerDefinitionJsonSerDeser.INSTANCE.toJson(input.getBaseIndexer());
    ObjectNode objJson = JsonNodeFactory.instance.objectNode();
    objJson.put("newIndexer", newIndexerJson);
    objJson.put("baseIndexer", baseIndexerJson);
    try {
      return new ObjectMapper().writeValueAsBytes(objJson);
    } catch (IOException e) {
      throw new RuntimeException("Error serializing indexer definition to JSON.", e);
    }
  }

  public UpdateIndexerInput fromJsonBytes(byte [] jsonBytes) {
    ObjectNode node = null;
    try {
      node = (ObjectNode) new ObjectMapper().readTree(new ByteArrayInputStream(jsonBytes));
    } catch (IOException e) {
      throw new JsonFormatException("Error parsing indexer definition JSON.", e);
    }
    if (node.size() != 2) {
      new JsonFormatException("Expected object field of size 2, got size " + node.size());
    }
    IndexerDefinition baseIndexer = getDefinitionFromNode(node,"baseIndexer").build();
    IndexerDefinition newIndexer = getDefinitionFromNode(node, "newIndexer").build();
    return new UpdateIndexerInput(newIndexer, baseIndexer);
  }

  private IndexerDefinitionBuilder getDefinitionFromNode(ObjectNode baseNode, String name) {
    JsonNode node = baseNode.get(name);
    if (node == null || node.isNull() || !node.isObject()) {
      throw new JsonFormatException("Unable to find object field: " + name);
    }
    return IndexerDefinitionJsonSerDeser.INSTANCE.fromJson((ObjectNode)node);
  }
}

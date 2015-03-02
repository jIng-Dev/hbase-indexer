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
package com.ngdata.hbaseindexer.compatrest;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerConcurrentModificationException;
import com.ngdata.hbaseindexer.model.api.IndexerDeleteFailedException;
import com.ngdata.hbaseindexer.model.api.IndexerException;
import com.ngdata.hbaseindexer.model.api.IndexerExistsException;
import com.ngdata.hbaseindexer.model.api.IndexerModelException;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.model.api.IndexerUpdateException;
import com.ngdata.hbaseindexer.model.api.IndexerValidityException;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionJsonSerDeser;
import com.ngdata.hbaseindexer.model.impl.UpdateIndexerInputJsonSerDeser;
import com.ngdata.hbaseindexer.model.impl.UpdateIndexerInputJsonSerDeser.UpdateIndexerInput;
import com.ngdata.hbaseindexer.servlet.IndexerServerException;
import com.ngdata.hbaseindexer.util.json.JsonFormatException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.JsonNodeFactory;

/**
 * Indexer resource, based on the com.ngdata.hbaseindexer.rest implementation.
 * Only supports existing CLI commands (list, add, delete, update).  Does the
 * model "work" (i.e. writing to zookeeper) instead of the CLI process itself.
 */
@Path("indexer")
public class CliCompatibleIndexResource {

    private final static Log log = LogFactory.getLog(CliCompatibleIndexResource.class);

    /**
     * Time (in millis) to wait for the delete operation to succeed
     */
    public static final long DELETE_TIMEOUT_MILLIS = 30000;

    @Context
    protected ServletContext servletContext;

    protected class IndexerFormatException extends IndexerException {
      public IndexerFormatException(String msg, Throwable e) {
        super(msg, e);
      }
    }

    protected class InternalIndexerException extends IndexerException {
      public InternalIndexerException(Throwable t) {
        super(t);
      }
    }

    /**
     * Get all index definitions.
     */
    @GET
    @Produces("application/json")
    public Collection<IndexerDefinition> get(@Context UriInfo uriInfo) throws IndexerServerException {
      return getModel().getIndexers();
    }

    /**
     * Delete an indexer definition
     */
    @DELETE
    @Produces("application/json")
    @Path("{name}")
    public byte[] delete(@PathParam("name") String indexerName) throws IndexerServerException, InterruptedException, KeeperException {
      if (!getModel().hasIndexer(indexerName)) {
        throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
          new IndexerNotFoundException("Indexer does not exist: " + indexerName));
      }

      try {
        IndexerDefinition indexerDef = getModel().getIndexer(indexerName);

        if (indexerDef.getLifecycleState() == LifecycleState.DELETE_REQUESTED
            || indexerDef.getLifecycleState() == LifecycleState.DELETING) {
          throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
            new IndexerConcurrentModificationException("Delete of \'" + indexerName + "\' is already in progress"));
        }

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        builder.startFrom(indexerDef);
        builder.lifecycleState(LifecycleState.DELETE_REQUESTED);

        getModel().updateIndexerInternal(builder.build());

        return waitForDeletion(indexerName);
     } catch (IndexerNotFoundException ex) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST, ex);
     } catch (IndexerConcurrentModificationException ex) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST, ex);
     } catch (IndexerValidityException ex) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST, ex);
     }
   }

   /**
    * Update an indexer definition.
    */
   @PUT
   @Path("{name}")
   @Consumes("application/json")
   @Produces("application/json")
   public byte[] put(@PathParam("name") String indexName, byte [] jsonBytes) throws IndexerServerException {
     WriteableIndexerModel model = getModel();

     if (!model.hasIndexer(indexName)) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
         new IndexerNotFoundException("Indexer does not exist: " + indexName));
     }

     UpdateIndexerInput input = null;
     try {
       input = UpdateIndexerInputJsonSerDeser.INSTANCE.fromJsonBytes(jsonBytes);
     } catch (Exception e) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
         new IndexerFormatException("Unable to create indexer definitions", e));
     }
     IndexerDefinition baseIndexer = input.getBaseIndexer();
     IndexerDefinition newIndexer = input.getNewIndexer();

     if (!newIndexer.getName().equals(indexName)) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
         new IndexerUpdateException("Requested update of indexer: " + indexName +
           " but indexer parameter has name: " + newIndexer.getName()));
     }

     try {
       String lock = model.lockIndexer(indexName);
       try {
         IndexerDefinition oldIndexer = model.getFreshIndexer(indexName);
         if (!oldIndexer.equals(baseIndexer)) {
           throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
             new IndexerConcurrentModificationException(
               "Base Indexer does not match model, model may have been modified concurrently"));
         }

         if (newIndexer.equals(oldIndexer)) {
           return toJsonResultBytes("Indexer " + indexName + " already matches the specified settings, did not update it.");
         } else {
           model.updateIndexer(newIndexer, lock);
           return toJsonResultBytes("Indexer updated: " + indexName);
         }
       } finally {
         // In case we requested deletion of an index, it might be that the lock is already removed
         // by the time we get here as part of the index deletion.
         boolean ignoreMissing = newIndexer != null
           && newIndexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED;
         model.unlockIndexer(lock, ignoreMissing);
       }
     } catch (Exception e) {
       throw new IndexerServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
         new InternalIndexerException(e));
     }
   }

   /**
    * Add an indexer definition
    */
   @POST
   @Consumes("application/json")
   @Produces("application/json")
   public byte[] post(byte [] jsonBytes) throws IndexerServerException {
     WriteableIndexerModel model = getModel();

     IndexerDefinitionBuilder builder = getBuilderFromJson(jsonBytes, null);
     IndexerDefinition indexer = builder.build();
     try {
       model.addIndexer(indexer);
     } catch (IndexerExistsException iee) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST, iee);
     } catch (IndexerValidityException ive) {
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST, ive);
     } catch (IndexerModelException ime) {
       throw new IndexerServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ime);
     }
     return toJsonResultBytes("Indexer added: " + indexer.getName());
   }

   private WriteableIndexerModel getModel() {
     return ((WriteableIndexerModel)servletContext.getAttribute("indexerModel"));
   }

   private IndexerDefinitionBuilder getBuilderFromJson(byte [] jsonBytes,
       IndexerDefinitionBuilder baseBuilder) throws IndexerServerException {
     try {
       return baseBuilder == null ?
         IndexerDefinitionJsonSerDeser.INSTANCE.fromJsonBytes(jsonBytes) :
         IndexerDefinitionJsonSerDeser.INSTANCE.fromJsonBytes(jsonBytes, baseBuilder);
     } catch (JsonFormatException jfe) {
       // throw an IndexerException so we know it a client problem.
       throw new IndexerServerException(HttpServletResponse.SC_BAD_REQUEST,
         new IndexerFormatException("Unable to create indexer definition, "+ jfe.getMessage(), jfe));
     }
   }

   private byte[] waitForDeletion(String indexerName) throws InterruptedException, KeeperException, IndexerServerException {
      long startTime = System.nanoTime();
      while (getModel().hasIndexer(indexerName)) {
        IndexerDefinition indexerDef;
        try {
          indexerDef = getModel().getFreshIndexer(indexerName);
        } catch (IndexerNotFoundException e) {
          // The indexer was deleted between the call to hasIndexer and getIndexer, that's ok
          break;
        }

        switch (indexerDef.getLifecycleState()) {
          case DELETE_FAILED:
            throw new IndexerServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              new IndexerDeleteFailedException("Delete of indexer " + indexerName + " failed"));
          case DELETE_REQUESTED:
          case DELETING:
            Thread.sleep(500);
            // timer at the end so we try at least once
            if (TimeUnit.MILLISECONDS.toNanos(DELETE_TIMEOUT_MILLIS) < System.nanoTime() - startTime) {
            throw new IndexerServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              new IndexerDeleteFailedException("Delete of indexer " + indexerName + " timed out"));
            }
            continue;
          default:
            throw new IllegalStateException("Illegal lifecycle state while deleting: "
              + indexerDef.getLifecycleState());
        }
      }
      return toJsonResultBytes("Deleted indexer \'" + indexerName + "\'");
    }

    public byte[] toJsonResultBytes(String result) {
        ObjectNode objJson = JsonNodeFactory.instance.objectNode();
        objJson.put("result", result);
        try {
            return new ObjectMapper().writeValueAsBytes(objJson);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing result", e);
        }
    }
}

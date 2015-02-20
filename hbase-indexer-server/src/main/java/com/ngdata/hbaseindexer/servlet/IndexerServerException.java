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

package com.ngdata.hbaseindexer.servlet;

import com.ngdata.hbaseindexer.model.api.IndexerException;

/**
 * Represents an IndexerException thrown by the Server.  Used for
 * tracking what error code should be returned to the client.
 */
public class IndexerServerException extends IndexerException {
  protected int httpCode;
  protected IndexerException ex;

  public IndexerServerException(int httpCode, IndexerException ex) {
    this.httpCode = httpCode;
    this.ex = ex;
  }

  public int getCode() { return httpCode; }

  public IndexerException getException() { return ex; }
}

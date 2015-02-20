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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that returns reasonable error codes to the client.
 * By default, jetty will return any uncaught exceptions as 500 (SERVER_ERROR).
 * This Filter catches exceptions represent bad requests from the client
 * and return 400 (BAD_REQUEST) instead.
 */
public class ErrorHandlerFilter implements Filter {
  private static final Logger LOG =
    LoggerFactory.getLogger(ErrorHandlerFilter.class);

  /**
   * Initializes the filter.
   * <p/>
   * This implementation is a NOP.
   *
   * @param config filter configuration.
   *
   * @throws ServletException thrown if the filter could not be initialized.
   */
  @Override
  public void init(FilterConfig config) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
  throws IOException, ServletException {
    try {
      chain.doFilter(request, response);
    } catch (ServletException e) {
       if (e.getCause() != null && e.getCause() instanceof IndexerServerException) {
        LOG.error("Got IndexerServerException from REST API", e);
        IndexerServerException ise = (IndexerServerException)e.getCause();
        HttpServletResponse httpResponse = (HttpServletResponse)response;
        httpResponse.sendError(ise.getCode(), ise.getException().getMessage());
      } else {
        throw e;
      }
    }
  }

  /**
   * Destroys the filter.
   * <p/>
   * This implementation is a NOP.
   */
  @Override
  public void destroy() {
  }
}

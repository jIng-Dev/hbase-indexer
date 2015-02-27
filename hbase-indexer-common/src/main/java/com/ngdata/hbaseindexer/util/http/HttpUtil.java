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
package com.ngdata.hbaseindexer.util.http;

import com.ngdata.hbaseindexer.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class HttpUtil {

  public static byte [] sendRequest(HttpRequestBase request) throws IOException {
    byte[] entityResponse = null;
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpResponse response = httpclient.execute(request);
    int httpStatus = response.getStatusLine().getStatusCode();

    try {
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        entityResponse = IOUtils.toByteArray(entity.getContent());
      }
    } finally {
      if (response.getEntity() != null) {
        EntityUtils.consume(response.getEntity());
      }
      request.releaseConnection();
    }

    switch (httpStatus) {
      case HttpStatus.SC_OK:
         break;
        case HttpStatus.SC_MOVED_PERMANENTLY:
        case HttpStatus.SC_MOVED_TEMPORARILY:
          throw new RuntimeException("Server at " + getHostname(request.getURI())
            + " sent back a redirect (" + httpStatus + ").");
        default:
          throw new RuntimeException("Server at " + getHostname(request.getURI())
            + " returned non ok status:" + httpStatus
            + ", message:" + response.getStatusLine().getReasonPhrase(),
                null);
    }
    return entityResponse;
  }

  public static String getResponse(byte [] json) {
    if (json == null) {
      return null;
    }

    ObjectNode node;
    try {
      node = (ObjectNode) new ObjectMapper().readTree(new ByteArrayInputStream(json));
    } catch (IOException e) {
      throw new RuntimeException("Error parsing result  JSON.", e);
    }
    return JsonUtil.getString(node, "result");
  }

  private static String getHostname(URI uri) {
    return uri.getScheme() + "://" + uri.getAuthority();
  }
}

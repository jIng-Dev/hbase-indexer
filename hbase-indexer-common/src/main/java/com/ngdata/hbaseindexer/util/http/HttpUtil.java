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
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class HttpUtil {

  public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";

  public static byte [] sendRequest(HttpRequestBase request) throws IOException {
    byte[] entityResponse = null;
    DefaultHttpClient httpclient = new DefaultHttpClient();
    loadJaasConfiguration(httpclient);
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

  private static void loadJaasConfiguration(DefaultHttpClient httpClient) {
    String configValue = System.getProperty(LOGIN_CONFIG_PROP);
    if (configValue != null) {
      final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
      String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

      // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
      // authentication mechanism can load the credentials from the JAAS configuration.
      if (useSubjectCredsVal == null) {
        System.setProperty(useSubjectCredsProp, "false");
      }
      else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
        // Don't overwrite the prop value if it's already been written to something else,
        // but warn because it is likely the Credentials won't be loaded correctly.
        System.err.println("System Property: " + useSubjectCredsProp + " set to: " + useSubjectCredsVal
          + " not false.  SPNego authentication may not be successful.");
      }

      javax.security.auth.login.Configuration.setConfiguration(new ZKJaasConfiguration());
      httpClient.getAuthSchemes().register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(true));
      // Get the credentials from the JAAS configuration rather than here
      Credentials use_jaas_creds = new Credentials() {
        public String getPassword() {
          return null;
        }

        public Principal getUserPrincipal() {
          return null;
        }
      };

      httpClient.getCredentialsProvider().setCredentials(
        AuthScope.ANY, use_jaas_creds);
    }
  }

  /**
   * Use the ZK JAAS Client appName rather than the default com.sun.security.jgss appName.
   * This simplifies client configuration; rather than requiring an appName in the JAAS conf
   * for ZooKeeper's client (default "Client") and one for the default com.sun.security.jgss
   * appName, only a single unified appName is needed.
   */
  private static class ZKJaasConfiguration extends javax.security.auth.login.Configuration {

    private javax.security.auth.login.Configuration baseConfig;
    private String zkClientLoginContext;

    // the com.sun.security.jgss appNames
    private Set<String> initiateAppNames = new HashSet<String>(
      Arrays.asList("com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate"));

    public ZKJaasConfiguration() {
      try {
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }

      // Get the app name for the ZooKeeperClient
      zkClientLoginContext = System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (baseConfig == null) return null;

      if (initiateAppNames.contains(appName)) {
        return baseConfig.getAppConfigurationEntry(zkClientLoginContext);
      }
      return baseConfig.getAppConfigurationEntry(appName);
    }
  }
}

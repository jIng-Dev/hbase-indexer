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

import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.servlet.HBaseIndexerAuthFilter;
import static com.ngdata.hbaseindexer.servlet.HBaseIndexerAuthFilter.HBASEINDEXER_PROXYUSER_PREFIX;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import static org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter.PROXYUSER_PREFIX;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.easymock.EasyMock;

public class HBaseIndexerAuthFilterTest {
  private static HBaseIndexerAuthFilter filter;

  @BeforeClass
  public static void beforeClass() throws Exception {
    filter = new HBaseIndexerAuthFilter();
  }

  @Test
  public void testDefaults() throws Exception {
    Properties props = filter.getConfiguration(null, getFilterConfig(null));
    assertEquals(props.getProperty(SolrHadoopAuthenticationFilter.AUTH_TYPE),
      PseudoAuthenticationHandler.class.getName());
    assertEquals("true", props.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }

  @Test
  public void testOverrideDefaults() throws Exception {
    Configuration conf = HBaseIndexerConfiguration.create();

    conf.set(filter.HBASEINDEXER_PREFIX + HBaseIndexerAuthFilter.AUTH_TYPE, "otherAuthType");
    conf.set(filter.HBASEINDEXER_PREFIX + PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
    Properties props = filter.getConfiguration(null, getFilterConfig(conf));

    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      if (entry.getKey().startsWith(HBASEINDEXER_PROXYUSER_PREFIX)) {
        String newKey = PROXYUSER_PREFIX + "."
          + entry.getKey().substring(HBASEINDEXER_PROXYUSER_PREFIX.length());
        assertEquals(entry.getValue(), props.get(newKey));
      }
    }
  }

  @Test
  public void testKerberos() throws Exception {
    String kerberos = "kerberos";
    String authType = filter.HBASEINDEXER_PREFIX + filter.AUTH_TYPE;
    Configuration conf = HBaseIndexerConfiguration.create();
    conf.set(authType, kerberos);
    Properties props = filter.getConfiguration(null, getFilterConfig(conf));
    assertEquals(kerberos,
      props.getProperty(HBaseIndexerAuthFilter.AUTH_TYPE));
  }

  @Test
  public void testGetProxyuserConfiguration() throws Exception {
    Configuration inputConf = HBaseIndexerConfiguration.create();
    inputConf.set(HBASEINDEXER_PROXYUSER_PREFIX + "hue.hosts", "*");
    inputConf.set(HBASEINDEXER_PROXYUSER_PREFIX + "hue.groups", "value");
    inputConf.set(HBASEINDEXER_PROXYUSER_PREFIX + "hue.somethingElse", "somethingElse");
    inputConf.set(HBASEINDEXER_PROXYUSER_PREFIX, "justPrefix");
    inputConf.set(PROXYUSER_PREFIX + "oldPrefix.hosts", "hbaseindexer");

    Configuration conf = filter.getProxyuserConfiguration(getFilterConfig(inputConf));

    Iterator<Map.Entry<String, String>> it = inputConf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      if (entry.getKey().startsWith(HBASEINDEXER_PROXYUSER_PREFIX)) {
        String newKey = PROXYUSER_PREFIX + "."
          + entry.getKey().substring(HBASEINDEXER_PROXYUSER_PREFIX.length());
        assertEquals(entry.getValue(), conf.get(newKey));
      }
    }
  }

  private FilterConfig getFilterConfig(Configuration conf) {
    ServletContext servletContext = EasyMock.createMock(ServletContext.class);
    EasyMock.expect(servletContext.getAttribute(HBaseIndexerAuthFilter.CONF_ATTRIBUTE)).andReturn(conf);
    EasyMock.replay(servletContext);

    FilterConfig filterConfig = EasyMock.createMock(FilterConfig.class);
    EasyMock.expect(filterConfig.getServletContext()).andReturn(servletContext);
    EasyMock.replay(filterConfig);
    return filterConfig;
  }
}

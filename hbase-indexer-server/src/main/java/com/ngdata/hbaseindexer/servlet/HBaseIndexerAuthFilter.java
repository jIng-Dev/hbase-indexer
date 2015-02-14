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

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import static org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter.PROXYUSER_PREFIX;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication filter that extends Hadoop-auth AuthenticationFilter to override
 * the configuration loading.
 */
public class HBaseIndexerAuthFilter extends DelegationTokenAuthenticationFilter {
  private static Logger LOG = LoggerFactory.getLogger(HBaseIndexerAuthFilter.class);
  public static final String HBASEINDEXER_PREFIX = "hbaseindexer.authentication.";
  public static final String HBASEINDEXER_PROXYUSER_PREFIX = HBASEINDEXER_PREFIX + "proxyuser.";

  /**
   * Request attribute constant for the user name.
   */
  public static final String USER_NAME = "hbaseindexer.user.name";

  /**
   * Http param for requesting ProxyUser support.
   */
  public static final String DO_AS_PARAM = "doAs";

  /**
   * ServletContext attribute that stores HBaseIndexer Configuration
   */
  public static final String CONF_ATTRIBUTE = "hbaseindexer.conf";

  /**
   * Initialize the filter.
   *
   * @param filterConfig filter configuration.
   * @throws ServletException thrown if the filter could not be initialized.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    super.init(filterConfig);
  }

  /**
   * Destroy the filter.
   */
  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * Return the ProxyUser Configuration.  Configuration properties beginning with
   * {#HBASEINDEXER_PROXYUSER_PREFIX} will be added to the configuration.
   */
  @Override
  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig)
      throws ServletException {
    Configuration conf = new Configuration(false);
    Configuration hbaseIndexerConf =
      (Configuration)filterConfig.getServletContext().getAttribute(CONF_ATTRIBUTE);
    if (hbaseIndexerConf != null) {
      Iterator<Map.Entry<String,String>> it = hbaseIndexerConf.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String key = entry.getKey();
        if (key.startsWith(HBASEINDEXER_PROXYUSER_PREFIX)) {
          conf.set(PROXYUSER_PREFIX + "." + key.substring(HBASEINDEXER_PROXYUSER_PREFIX.length()),
            entry.getValue());
        }
      }
    }
    return conf;
  }

  /**
   * Returns the Properties to be used by the authentication filter.
   * <p/>
   * All properties from the Configuration with names that starts with {@link #HBASEINDEXER_PREFIX} will
   * be returned. The keys of the returned properties are trimmed from the {@link #HBASEINDEXER_PREFIX}
   * prefix, for example the property name 'hbaseindexer.authentication.type' will
   * be just 'type'.
   *
   * @param configPrefix configuration prefix, this parameter is ignored by this implementation.
   * @param filterConfig filter configuration, this parameter is ignored by this implementation.
   * @return all properties prefixed with {@link #HBASEINDEXER_PREFIX}, without the
   * prefix.
   */
  @Override
  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) {
    Properties props = new Properties();

    //setting the cookie path to root '/' so it is used for all resources.
    props.setProperty(AuthenticationFilter.COOKIE_PATH, "/");
    Configuration hbaseIndexerConf =
      (Configuration)filterConfig.getServletContext().getAttribute(CONF_ATTRIBUTE);
    if (hbaseIndexerConf != null) {
      Iterator<Map.Entry<String,String>> it = hbaseIndexerConf.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String name = entry.getKey();
        if (name.startsWith(HBASEINDEXER_PREFIX)) {
          String value = entry.getValue();
          name = name.substring(HBASEINDEXER_PREFIX.length());
          props.setProperty(name, value);
        }
      }
    }

    // Ensure the old behavior (simple authentication)
    // is preserved if properties not specified
    String authType = props.getProperty(AUTH_TYPE);
    if (authType == null) {
      props.setProperty(AUTH_TYPE, PseudoAuthenticationHandler.class.getName());
      if (props.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED) == null) {
        props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
      }
    }

    return props;
  }

  /**
   * Enforces authentication using Hadoop-auth AuthenticationFilter.
   *
   * @param request http request.
   * @param response http response.
   * @param filterChain filter chain.
   * @throws IOException thrown if an IO error occurs.
   * @throws ServletException thrown if a servlet error occurs.
   */
  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
      throws IOException, ServletException {

    FilterChain filterChainWrapper = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
          throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        httpRequest.setAttribute(USER_NAME, httpRequest.getRemoteUser());
        filterChain.doFilter(servletRequest, servletResponse);
      }
    };

    super.doFilter(request, response, filterChainWrapper);
  }
}

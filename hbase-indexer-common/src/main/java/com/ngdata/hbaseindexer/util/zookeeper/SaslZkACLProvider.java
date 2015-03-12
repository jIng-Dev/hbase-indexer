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

package com.ngdata.hbaseindexer.util.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/**
 * ACLProvider that sets all ACLs to be owned by a configurable user
 * (typically hbase) via sasl auth, and readable by world.
 */
public class SaslZkACLProvider implements ACLProvider {
  private String saslUser;

  public SaslZkACLProvider(String saslUser) {
    this.saslUser = saslUser;
  }

  @Override
  public List<ACL> getAclForPath(String path) {
    return getDefaultAcl();
  }

  @Override
  public List<ACL> getDefaultAcl() {
    List<ACL> result = new ArrayList<ACL>();
    result.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", saslUser)));
    result.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    return result;
  }
}

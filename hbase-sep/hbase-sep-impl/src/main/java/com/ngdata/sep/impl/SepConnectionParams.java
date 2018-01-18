/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import org.apache.hadoop.hbase.TableName;

import com.google.common.base.Predicate; 

/** Parameters to be passed to a {@link SepConnection} */
final class SepConnectionParams { 
  
  private final Predicate<TableName> tableNamePredicate;
  private final long subscriptionTimestamp;
  private final SepConsumer sepConsumer;
  
  public SepConnectionParams(Predicate<TableName> tableNamePredicate, long subscriptionTimestamp,
        SepConsumer sepConsumer) {
    this.tableNamePredicate = tableNamePredicate;
    this.subscriptionTimestamp = subscriptionTimestamp;
    this.sepConsumer = sepConsumer;
  }

  public Predicate<TableName> getTableNamePredicate() {
    return tableNamePredicate;
  }

  public long getSubscriptionTimestamp() {
    return subscriptionTimestamp;
  }
  
  public SepConsumer getSepConsumer() {
    return sepConsumer;
  }
  
}
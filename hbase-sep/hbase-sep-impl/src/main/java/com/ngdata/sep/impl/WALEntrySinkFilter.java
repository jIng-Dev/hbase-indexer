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
import org.apache.hadoop.hbase.client.Connection;

public interface WALEntrySinkFilter {
  /**
   * Name of configuration to set with name of implementing WALEntrySinkFilter class.
   */
  public static final String WAL_ENTRY_FILTER_KEY = "hbase.replication.sink.walentrysinkfilter";

  /**
   * Called after Construction.
   * Use passed Connection to keep any context the filter might need.
   */
  void init(Connection connection);

  /**
   * @param table Table edit is destined for.
   * @param writeTime Time at which the edit was created on the source.
   * @return True if we are to filter out the edit.
   */
  boolean filter(TableName table, long writeTime);
}

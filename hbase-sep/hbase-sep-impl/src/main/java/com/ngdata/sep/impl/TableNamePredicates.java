/*
 * Copyright 2013 NGDATA nv
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

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;

public final class TableNamePredicates {

    public static Predicate<TableName> getTableNamePredicate(String targetTableNameExpression, boolean targetTableIsRegex) {
        if (targetTableIsRegex) {
            final Pattern tableNamePattern = Pattern.compile(targetTableNameExpression);
            return new Predicate<TableName>() {
                @Override
                public boolean apply(TableName table) {
                    return tableNamePattern.matcher(table.getNameAsString()).matches();
                }
            };
        } else {
            final byte[] tableNameBytes = Bytes.toBytes(targetTableNameExpression);
            return new Predicate<TableName>() {
                @Override
                public boolean apply(TableName table) {
                    return Bytes.equals(tableNameBytes, table.getName());
                }
            };
        }
    }
  
    public static Predicate<TableName> getAlwaysMatchingTableNamePredicate() {
        return new Predicate<TableName>() {
            @Override
            public boolean apply(TableName table) {
                return true;
            }
        };
    }

}

/**
 * Copyright 2011 Booz Allen Hamilton.
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. Booz Allen Hamilton
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bah.culvert.databaseadapter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import com.bah.culvert.adapter.DatabaseAdapter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.tableadapters.HBaseTableAdapter;

/**
 * Interact with an HBase instance. Allows easy creation of tables in the
 * instance and accessing them via a {@link TableAdapter}.
 * 
 * @see TableAdapter
 */
public class HBaseDatabaseAdapter extends DatabaseAdapter {
  private HBaseAdmin hbaseAdmin;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      hbaseAdmin = new HBaseAdmin(conf);
    } catch (MasterNotRunningException e) {
      throw new RuntimeException("Failed to create HBaseAdmin", e);
    } catch (ZooKeeperConnectionException e) {
      throw new RuntimeException("Failed to create HBaseAdmin", e);
    }
  }

  @Override
  public void create(String tableName, byte[][] splitKeys, List<CColumn> columns) {
    HTableDescriptor desc = new HTableDescriptor(tableName);

    // XXX hack to make sure that indexes put into HTables with a CF
    // use a default column if we create a table that doesn't have a any columns
    if (columns == null || columns.size() == 0) {
      HColumnDescriptor c = new HColumnDescriptor(
          HBaseTableAdapter.DEFAULT_COLUMN);
      desc.addFamily(c);
    } else {
      for (CColumn column : columns) {
        HColumnDescriptor c = new HColumnDescriptor(column.getColumnFamily());
        desc.addFamily(c);
      }
    }

    try {
      hbaseAdmin.createTable(desc, splitKeys);
    } catch (MasterNotRunningException e) {
      throw new RuntimeException("Master not running. Unable to create table",
          e);
    } catch (ZooKeeperConnectionException e) {
      throw new RuntimeException(
          "Zookeeper not running. Unable to create table", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create table", e);
    }
  }

  @Override
  public void delete(String tableName) {
    try {
      hbaseAdmin.disableTable(tableName);
      hbaseAdmin.deleteTable(tableName);
    } catch (MasterNotRunningException e) {
      throw new RuntimeException("Master not running. Unable to delete table",
          e);
    } catch (ZooKeeperConnectionException e) {
      throw new RuntimeException(
          "Zookeeper not running. Unable to delete table", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to delete table", e);
    }
  }

  @Override
  public boolean verify() {
    try {
      return hbaseAdmin.isMasterRunning();
    } catch (MasterNotRunningException e) {
      return false;
    } catch (ZooKeeperConnectionException e) {
      return false;
    }
  }

  @Override
  public TableAdapter getTableAdapter(String tableName) {
    Configuration conf = new Configuration(this.getConf());
    TableAdapter.setTableName(conf, tableName);
    TableAdapter adapter = new HBaseTableAdapter(conf);
    return adapter;
  }

  @Override
  public boolean tableExists(String tableName) {
    try {
      return hbaseAdmin.tableExists(tableName);
    } catch (TableNotFoundException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
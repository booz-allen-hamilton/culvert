/**
 * Copyright 2011 Booz Allen Hamilton.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Booz Allen Hamilton licenses this file
 * to you under the Apache License, Version 2.0 (the
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
package com.bah.culvert.adapter;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.BaseConfigurable;

/**
 * Connect to a table. This is the top level interface that used to run a query.
 * 
 * @see HBaseTableAdapter
 */
public abstract class TableAdapter extends BaseConfigurable implements
    Closeable, Writable {

  /** Configuration key for the table name the adapter should to connect to */
  public static final String TABLE_NAME_SETTING_KEY = "culvert.adapter.tablename";

  /**
   * Create a table adapter with this table name. The name is stored in the
   * configuration, so any changes to the configuration could alter the behavior
   * of this table if you don't ensure the table names are the same across
   * configurations.
   * @param tableName
   * @see #TABLE_NAME_SETTING_KEY
   */
  public TableAdapter(String tableName) {
    this.setTableName(tableName);
  }

  /**
   * Create a table adapter based on a configuration where the table name is
   * already set.
   * @param conf Replaces any other configuration already set for the table
   */
  public TableAdapter(Configuration conf) {
    this.setConf(conf);
  }

  public void setTableName(String tableName) {
    getConf().set(TABLE_NAME_SETTING_KEY, tableName);
  }

  /**
   * Get the table name that this adapter connects to
   * 
   * @return name of the table
   */
  public String getTableName() {
    return getConf().get(TABLE_NAME_SETTING_KEY);
  }

  /**
   * Do a put on the table
   * 
   * @param put
   *          to put
   * @throws TableException
   *           on general table failure
   * @throws RuntimeException
   *           if the write is not valid
   */
  public abstract void put(Put put);

  /**
   * Do a get on the local table. Gets all the CFs and CQ in the specified row.
   * 
   * @param get
   *          to perform
   * @return the results from that get
   */
  public abstract SeekingCurrentIterator get(Get get);

  /**
   * Execute a {@link ConfigurableCallable} remotely on database shards starting
   * with <tt>startKeys</tt> using the provided {@link Configuration} .
   * 
   * @param <T>
   * @param rangeKeys
   * @param conf
   * @param remoteCallable
   * @return
   */
  public abstract <T> List<T> remoteExec(byte[] startKey, byte[] endKey,
      Class<? extends RemoteOp<T>> remoteCallable,
          Object... array);

  @Override
  public void close() {
    // default is no-op
  }

  /**
   * Pairs with {@link #getEndKeys()} 1-to-1 (1st start key matches with first
   * end key
   * @return the start keys for all servers
   */
  public abstract byte[][] getStartKeys();

  /**
   * Pairs with {@link #getStartKeys()} 1-to-1 (1st start key matches with first
   * end key
   * @return the end keys for all servers.
   */
  public abstract byte[][] getEndKeys();

  /**
   * Get the hosts that this table is running over. Mostly used for MapReduce
   * integration.
   * @return the servers addresses hosting this table
   */
  public abstract Collection<String> getHosts();

  public static void setTableName(Configuration conf, String tableName) {
    conf.set(TableAdapter.TABLE_NAME_SETTING_KEY, tableName);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(in);
    setConf(conf);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getConf().write(out);
  }
}

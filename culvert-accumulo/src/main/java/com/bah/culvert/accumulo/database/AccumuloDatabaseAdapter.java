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
package com.bah.culvert.accumulo.database;

import static com.bah.culvert.accumulo.AccumuloConstants.DEFAULT_MAX_LATENCY;
import static com.bah.culvert.accumulo.AccumuloConstants.DEFAULT_MAX_MEMORY;
import static com.bah.culvert.accumulo.AccumuloConstants.DEFAULT_MAX_THREADS;
import static com.bah.culvert.accumulo.AccumuloConstants.MAX_LATENCY_KEY;
import static com.bah.culvert.accumulo.AccumuloConstants.MAX_MEMORY_KEY;
import static com.bah.culvert.accumulo.AccumuloConstants.MAX_THREADS_KEY;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.bah.culvert.accumulo.AccumuloConstants;
import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.util.Exceptions;

/**
 * Adapter for connecting to an Accumulo instance.
 */
public class AccumuloDatabaseAdapter extends DatabaseAdapter {

  private static final Log LOG = LogFactory.getLog(AccumuloDatabaseAdapter.class);

  private Connector conn;
  private Instance inst;

  /**
   * Overloading the use of this method to actually create the connection
   */
  @Override
  public boolean verify() {
    Configuration conf = this.getConf();
    try {
      this.inst = getInstance(conf);
      this.conn = inst.getConnector(conf.get(AccumuloConstants.USERNAME_KEY),
          conf.get(AccumuloConstants.PASSWORD_KEY));
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not verify/create connection to Accumulo", e);
    }
    return true;
  }

  private static Instance getInstance(Configuration conf)
      throws ClassNotFoundException {
    String instClass = conf.get(AccumuloConstants.INSTANCE_CLASS_KEY);
    Class<? extends Instance> clazz;

    // make sure that we have a valid instance
    if (instClass == null) {
      LOG.debug("No instance type specified for connecting to Accumulo, using default");
      clazz = AccumuloConstants.DEFAULT_INSTANCE_CLASS;
    } else
      clazz = Class.forName(instClass).asSubclass(Instance.class);

    // create the valid instance
    if (clazz.equals(ZooKeeperInstance.class))
      return new ZooKeeperInstance(
          conf.get(AccumuloConstants.INSTANCE_NAME_KEY),
          conf.get(AccumuloConstants.ZOOKEEPER_SERVERS_KEY));
    else if (clazz.equals(MockInstance.class))
      return new MockInstance(conf.get(AccumuloConstants.INSTANCE_NAME_KEY,
          AccumuloConstants.DEFAULT_INSTANCE_NAME));
    else
      throw new IllegalArgumentException(instClass
          + "is not a valid instance class");
  }

  @Override
  public TableAdapter getTableAdapter(String tableName) {
    Configuration conf = this.getConf();
    return new AccumuloTableAdapter(this.conn, tableName, conf.getLong(
        MAX_MEMORY_KEY, DEFAULT_MAX_MEMORY), conf.getLong(MAX_LATENCY_KEY,
        DEFAULT_MAX_LATENCY), conf.getInt(MAX_THREADS_KEY, DEFAULT_MAX_THREADS));
  }

  @Override
  public void create(String tableName, byte[][] splitKeys, List<CColumn> columns) {
    try {
      this.conn.tableOperations().create(tableName);
      // copy over the split keys into a format that accumulo can understand
      SortedSet<Text> partitionKeys = new TreeSet<Text>();
      for (byte[] split : splitKeys)
        partitionKeys.add(new Text(split));
      // and add the splits
      this.conn.tableOperations().addSplits(tableName, partitionKeys);
    } catch (Exception e) {
      throw Exceptions.asRuntime(e);
    }

  }

  @Override
  public void delete(String tableName) {
    try {
      if (this.conn.tableOperations().exists(tableName))
        this.conn.tableOperations().delete(tableName);
    } catch (Exception e) {
      throw Exceptions.asRuntime(e);
    }

  }

  @Override
  public boolean tableExists(String tableName) {
    return this.conn.tableOperations().exists(tableName);
  }

}

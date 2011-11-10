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

import java.util.Collections;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import com.bah.culvert.DatabaseAdapterTestingUtility;
import com.bah.culvert.TableAdapterTestingUtility;
import com.bah.culvert.accumulo.AccumuloConstants;
import com.google.common.base.Function;

/**
 * Integration test the {@link AccumuloDatabaseAdapter}
 */
public class ITAccumuloAdapter {

  private static Instance inst;
  private static final String INSTANCE_NAME = "TEST_INSTANCE";
  private static final Configuration conf = new Configuration();
  private static final String USERNAME = "user";
  private static final String PASSWORD = "password";

  @BeforeClass
  public static void beforeClass() throws Exception {
    inst = new MockInstance(INSTANCE_NAME);
    conf.set(AccumuloConstants.INSTANCE_CLASS_KEY, MockInstance.class.getName());
    conf.set(AccumuloConstants.INSTANCE_NAME_KEY, INSTANCE_NAME);
    // conf.setLong(AccumuloConstants.MAX_LATENCY_KEY, 1000);
    // conf.setLong(AccumuloConstants.MAX_MEMORY_KEY, 1000);
    // conf.setInt(AccumuloConstants.MAX_THREADS_KEY, 10);
    conf.set(AccumuloConstants.USERNAME_KEY, USERNAME);
    conf.set(AccumuloConstants.PASSWORD_KEY, PASSWORD);
  }

  /**
   * Test all the adapters
   * @throws Throwable
   */
  @Test
  public void testAdapters() throws Throwable {
    // test the database works
    AccumuloDatabaseAdapter db = new AccumuloDatabaseAdapter();
    db.setConf(conf);
    DatabaseAdapterTestingUtility.testDatabaseAdapter(db);

    // cleanup method
    Function<String, Void> cleanup = new Function<String, Void>() {
      @Override
      public Void apply(String input) {
        ITAccumuloAdapter.this.cleanTable(input);
        return null;
      }
    };

    // test that the table adapter works
    TableAdapterTestingUtility.testTableAdapter(db, cleanup);
    TableAdapterTestingUtility.testRemoteExecTableAdapter(db, 1, cleanup);

  }

  /**
   * Empty all the entries in a table
   * @param tableName
   */
  public static void cleanTable(String tableName) {
    try {
      Connector conn = inst.getConnector(USERNAME, PASSWORD.getBytes());
      BatchDeleter deleter = conn.createBatchDeleter(tableName,
          new Authorizations(), AccumuloConstants.DEFAULT_MAX_THREADS,
          AccumuloConstants.DEFAULT_MAX_MEMORY,
          AccumuloConstants.DEFAULT_MAX_LATENCY,
          AccumuloConstants.DEFAULT_MAX_THREADS);
      deleter.setRanges(Collections.singletonList(new Range()));
      deleter.delete();
      // sleep to make sure we deletes are propagated
      Thread.sleep(1000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

}

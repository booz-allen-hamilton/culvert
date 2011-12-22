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
package com.bah.culvert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.databaseadapter.HBaseDatabaseAdapter;
import com.bah.culvert.utils.HbaseTestProperties;

/**
 * Integration test for Culvert over HBase.
 * <p>
 * These tests need to run in sequence coded to ensure success
 */
public class CulvertHBaseIT {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private final static Configuration CONF = UTIL.getConfiguration();

  @BeforeClass
  public static void create() throws Exception {
    HbaseTestProperties.addStandardHBaseProperties(CONF);
    CONF.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "com.bah.culvert.tableadapters.HBaseCulvertCoprocessorEndpoint");
    UTIL.startMiniCluster(2);
    UTIL.getMiniHBaseCluster();
  }

  /**
   * Test that we read and write to/from the table with indexes properly
   * 
   * @throws Exception
   */
  @Test
  public void testReadWrite() throws Exception {
    // create the database for reading
    DatabaseAdapter database = new HBaseDatabaseAdapter();
    database.setConf(CONF);

    // setup the client and the database
    Client client = CulvertIntegrationTestUtility.prepare(database);

    // now test that we do insertion properly
    CulvertIntegrationTestUtility.testInsertion(client);

    // and that we can read the indexed value back out
    CulvertIntegrationTestUtility.testQuery(client);
  }

  /**
   * Shutdown the cluster
   * 
   * @throws Exception
   */
  @AfterClass
  public static void shutdown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
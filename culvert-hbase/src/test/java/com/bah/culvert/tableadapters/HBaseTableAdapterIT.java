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
package com.bah.culvert.tableadapters;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.bah.culvert.TableAdapterTestingUtility;
import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.databaseadapter.HBaseDatabaseAdapter;
import com.bah.culvert.utils.HbaseTestProperties;
import com.google.common.base.Function;

/**
 * Integration test for the {@link HBaseTableAdapter}
 */
@RunWith(JUnit4.class)
public class HBaseTableAdapterIT {
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private final static Configuration CONF = UTIL.getConfiguration();  
  private final DatabaseAdapter DATABASEADAPTER = new HBaseDatabaseAdapter();;

  /** The testing utility we're using */

  /**
   * Creates a utility and adapter for each test
   * 
   * @throws Throwable
   */
  @BeforeClass
  public static void setup() throws Throwable {
    HbaseTestProperties.addStandardHBaseProperties(CONF);
    CONF.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
	        "com.bah.culvert.tableadapters.HBaseCulvertCoprocessorEndpoint");
    UTIL.startMiniCluster(2);
    UTIL.getMiniHBaseCluster();
  }
  
  @Test
  public void testTable() throws Throwable {
    Function<String, Void> cleanup = new Function<String, Void>() {
      @Override
      public Void apply(String tableName) {
        try {
          HBaseTableAdapterIT.this.cleanupTable(tableName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };

    DATABASEADAPTER.setConf(CONF);
    TableAdapterTestingUtility.testTableAdapter(DATABASEADAPTER, cleanup);
    TableAdapterTestingUtility.testRemoteExecTableAdapter(DATABASEADAPTER, 2,
        cleanup);
  }

  /**
   * Delete all the rows from the table
   * @param tableName remove all entries from the specified table
   * @throws Exception
   */
  public void cleanupTable(String tableName) throws Exception {
    HTable table = new HTable(CONF, tableName);
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    List<Delete> deletes = new ArrayList<Delete>();
    for (org.apache.hadoop.hbase.client.Result r : scanner)
      deletes.add(new Delete(r.getRow()));
    table.delete(deletes);
  }
  
  /**
   * shuts down the mini cluster
   */
  @AfterClass
  public static void tearDown() throws Throwable {
	  UTIL.shutdownMiniCluster();
  }
}

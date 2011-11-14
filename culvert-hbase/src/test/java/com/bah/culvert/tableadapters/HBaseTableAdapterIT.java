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

import java.io.IOException;
import java.net.ConnectException;
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
import com.google.common.base.Function;

/**
 * Integration test for the {@link HBaseTableAdapter}
 */
@RunWith(JUnit4.class)
public class HBaseTableAdapterIT {

  private final static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();
  private static org.apache.hadoop.hbase.MiniHBaseCluster cluster = null;

  private static DatabaseAdapter databaseAdapter;
  /** The table adapter we're testing */
  private static HBaseTableAdapter adapter = null;

  private static final String TEST_TABLE = "TestTable";
  private static byte[] TEST_FAMILY = "col1".getBytes();

  /** The testing utility we're using */

  /**
   * Creates a utility and adapter for each test
   * 
   * @throws Throwable
   */
  @BeforeClass
  public static void setup() throws Throwable {
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "com.bah.culvert.tableadapters.HBaseCulvertCoprocessorEndpoint");

    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    System.out.println("Test Path = " + HBaseTestingUtility.getTestDir().toString());
    System.out.println("TEST_DIRECTORY_KEY = " + HBaseTestingUtility.TEST_DIRECTORY_KEY);    
=======
>>>>>>> 82545beb8c13548af08e3d67a27fc1b98e23e9cb
=======
>>>>>>> 82545beb8c13548af08e3d67a27fc1b98e23e9cb
=======
    System.out.println("Test Path = " + HBaseTestingUtility.getTestDir().toString());
    System.out.println("TEST_DIRECTORY_KEY = " + HBaseTestingUtility.TEST_DIRECTORY_KEY);    
>>>>>>> added test path output
    databaseAdapter = new HBaseDatabaseAdapter();
    databaseAdapter.setConf(util.getConfiguration());
    System.out.println("Sleep 1000");
    Thread.sleep(1000);
  }

  @Test
  public void testTable() throws Throwable {
	System.out.println("Enter testTable");
	
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

    TableAdapterTestingUtility.testTableAdapter(databaseAdapter, cleanup);
    TableAdapterTestingUtility.testRemoteExecTableAdapter(databaseAdapter, 2,
        cleanup);
    System.out.println("Leave testTable");
  }

  /**
   * Delete all the rows from the table
   * @param tableName remove all entries from the specified table
   * @throws Exception
   */
  public void cleanupTable(String tableName) throws Exception {
    HTable table = new HTable(util.getConfiguration(), tableName);
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
	System.out.println("Start tearDown");
//    HBaseTableAdapterIT.util.getMiniHBaseCluster().stopRegionServer(1);
	try{
	  System.out.println("Cluster: " + cluster.toString());
      util.shutdownMiniCluster();
	}
	catch (ConnectException con){
		System.out.println("ConnectException shutting down cluster");
		con.printStackTrace();
	}
	catch(IOException e){
		System.out.println("IOException shutting down cluster");
		e.printStackTrace();
	}
    System.out.println("Normal End of Job.");
  }
}

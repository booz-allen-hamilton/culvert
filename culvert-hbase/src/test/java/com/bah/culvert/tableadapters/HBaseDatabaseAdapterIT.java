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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.DatabaseAdapterTestingUtility;
import com.bah.culvert.adapter.DatabaseAdapter;

/**
 * Integration tests for the HBase table adapter.
 */
@RunWith(JUnit4.class)
public class HBaseDatabaseAdapterIT {

  private final static HBaseTestingUtility util = new HBaseTestingUtility();
  private static org.apache.hadoop.hbase.MiniHBaseCluster cluster = null;

  /**
   * Creates a utility and adapter for the test class
   * 
   * @throws Throwable
   */
  @BeforeClass
  public static void setup() throws Throwable {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                                HBaseCulvertCoprocessorEndpoint.class.getName());
    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
<<<<<<< HEAD
<<<<<<< HEAD
    System.out.println("Test Path = " + HBaseTestingUtility.getTestDir().toString());
    System.out.println("TEST_DIRECTORY_KEY = " + HBaseTestingUtility.TEST_DIRECTORY_KEY);
=======
>>>>>>> 82545beb8c13548af08e3d67a27fc1b98e23e9cb
=======
>>>>>>> 82545beb8c13548af08e3d67a27fc1b98e23e9cb
  }

  @Test
  public void testDatabase() throws Throwable {
    DatabaseAdapter db = new HBaseDatabaseAdapter();
    db.setConf(util.getConfiguration());
    System.out.println("Sleep 2000");
    Thread.sleep(2000);
    DatabaseAdapterTestingUtility.testDatabaseAdapter(db);
    
  }

  /**
   * Tear down the cluster after the test
   * 
   * @throws Throwable
   */
  @AfterClass
  public static void tearDown() throws Throwable {
	System.out.println("Stop cluster: " + cluster.toString());
	try{
      util.shutdownMiniCluster();
      System.out.println("Normal End of Job");
	}
	catch (ConnectException con){
		System.out.println("ConnectException shutting down cluster");
		con.printStackTrace();
	}
	catch(IOException e){
		System.out.println("IOException shutting down cluster");
		e.printStackTrace();
	}
  }
}

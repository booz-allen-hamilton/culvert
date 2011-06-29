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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;

/**
 * Integration tests for the HBase table adapter.
 */
@RunWith(JUnit4.class)
public class HBaseDatabaseAdapterIT {

  private static String TABLE_NAME_STR = "TestTable";

  private static HBaseTestingUtility util = new HBaseTestingUtility();

  private static DatabaseAdapter databaseAadapter;

  /** The table adapter we're testing */

  /**
   * Creates a utility and adapter for the test class
   * 
   * @throws Throwable
   */
  @BeforeClass
  public static void setup() throws Throwable {
    HBaseDatabaseAdapterIT.util.getConfiguration().set(
        CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        HBaseCulvertCoprocessorEndpoint.class.getName());
    HBaseDatabaseAdapterIT.util.startMiniCluster(2);
    HBaseDatabaseAdapterIT.util.getMiniHBaseCluster();
    HBaseDatabaseAdapterIT.databaseAadapter = new HBaseDatabaseAdapter();
    HBaseDatabaseAdapterIT.databaseAadapter.setConf(HBaseDatabaseAdapterIT.util
        .getConfiguration());
    Thread.sleep(2000);
  }

  /**
   * Test that the connection can be verified.
   * 
   * @throws Throwable
   */
  @Test
  public void testDatabaseConnection() throws Throwable {
    HBaseDatabaseAdapterIT.databaseAadapter.verify();
  }

  /**
   * Test that the adapter can create tables.
   * 
   * @throws Throwable
   */
  @Test
  public void testCreateTable() throws Throwable {
    List<CColumn> columns = new ArrayList<CColumn>();
    CColumn col1 = new CColumn("col1".getBytes());
    columns.add(col1);
    HBaseDatabaseAdapterIT.databaseAadapter.create(
        HBaseDatabaseAdapterIT.TABLE_NAME_STR, new byte[0][], columns);
    TableAdapter table = HBaseDatabaseAdapterIT.databaseAadapter
        .getTableAdapter(TABLE_NAME_STR);
    assertTrue(table != null);

    byte[][] t1Splits = new byte[1][];
    t1Splits[0] = new byte[] { 0, 1, 2, 3 };
    HBaseDatabaseAdapterIT.databaseAadapter.create(
        HBaseDatabaseAdapterIT.TABLE_NAME_STR + "1", t1Splits, columns);
    table = HBaseDatabaseAdapterIT.databaseAadapter
        .getTableAdapter(TABLE_NAME_STR + "1");
    assertTrue(table != null);

    byte[][] t2Splits = new byte[2][];
    t2Splits[0] = new byte[] { 0, 1, 2, 3 };
    t2Splits[1] = new byte[] { 3, 2, 12 };

    HBaseDatabaseAdapterIT.databaseAadapter.create(
        HBaseDatabaseAdapterIT.TABLE_NAME_STR + "2", t2Splits, columns);
    table = HBaseDatabaseAdapterIT.databaseAadapter
        .getTableAdapter(HBaseDatabaseAdapterIT.TABLE_NAME_STR + "2");
    assertTrue(table != null);
  }

  /**
   * Test that the adapter can delete tables.
   * 
   * @throws Throwable
   */
  @Test
  public void testDeleteTable() throws Throwable {
    HBaseDatabaseAdapterIT.databaseAadapter
        .delete(HBaseDatabaseAdapterIT.TABLE_NAME_STR);
    HBaseDatabaseAdapterIT.databaseAadapter
        .delete(HBaseDatabaseAdapterIT.TABLE_NAME_STR + "1");
    HBaseDatabaseAdapterIT.databaseAadapter
        .delete(HBaseDatabaseAdapterIT.TABLE_NAME_STR + "2");
  }

  /**
   * Tear down the cluster after the test
   * 
   * @throws Throwable
   */
  @AfterClass
  public static void tearDown() throws Throwable {
    // note we have to kill the regions for some odd reason.
    HBaseDatabaseAdapterIT.util.getMiniHBaseCluster().stopRegionServer(0);
    HBaseDatabaseAdapterIT.util.getMiniHBaseCluster().stopRegionServer(1);
    HBaseDatabaseAdapterIT.util.shutdownMiniCluster();
  }

}

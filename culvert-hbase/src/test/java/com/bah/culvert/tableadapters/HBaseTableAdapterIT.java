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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;
import com.google.common.collect.Iterators;

@RunWith(JUnit4.class)
public class HBaseTableAdapterIT {

  /**
   * Special getter for the {@link HBaseTableAdapterIT#testRemoteExec()} test.
   * We know that for that test one record is inserted, so there should be one
   * record to iterate. Returns the count of records in that region.
   */
  public static class _SpecialRecordGetter extends RemoteOp<Integer> {

    @Override
    public Integer call(Object... args) throws Exception {
      LocalTableAdapter local = this.getLocalTableAdapter();
      Get get = new Get(CRange.FULL_TABLE_RANGE);
      Iterator<Result> results = local.get(get);
      return Iterators.size(results);
    }

  }

  /**
   * Check that we properly serialize values across the wire to the the remote
   * op. Returns the number of arguments
   */
  public static class SerializationChecker extends RemoteOp<Integer> {

    @Override
    public Integer call(Object... args) throws Exception {
      if (args == null)
        throw new RuntimeException("argument array was emtpy!");

      return args.length;
    }

  }

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();

  private static DatabaseAdapter databaseAdapter;
  /** The table adapter we're testing */
  private static HBaseTableAdapter adapter;

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

    util.startMiniCluster(2);
    databaseAdapter = new HBaseDatabaseAdapter();
    databaseAdapter.setConf(util.getConfiguration());
    createTable();
    Thread.sleep(2000);
  }

  /**
   * Helper method to create a test table.
   * @throws Throwable
   */
  public static void createTable() throws Throwable {
    List<CColumn> columns = new ArrayList<CColumn>();
    CColumn col1 = new CColumn(TEST_FAMILY);
    columns.add(col1);
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = "bar1".getBytes();
    databaseAdapter.create(TEST_TABLE, splitKeys, columns);
    adapter = (HBaseTableAdapter) databaseAdapter.getTableAdapter(TEST_TABLE);
  }

  /**
   * Delete all the rows from the table
   * @throws Throwable
   */
  @After
  public void cleanupTable() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    List<Delete> deletes = new ArrayList<Delete>();
    for (org.apache.hadoop.hbase.client.Result r : scanner)
      deletes.add(new Delete(r.getRow()));
    table.delete(deletes);
  }

  /**
   * Tests that a record can be inserted without creating an exception.
   * 
   * @throws Throwable
   */
  @Test
  public void testPutGet() throws Throwable {
    List<CKeyValue> keyValues = new ArrayList<CKeyValue>();
    byte[] foo = "foo".getBytes();
    CKeyValue k1 = new CKeyValue(foo, TEST_FAMILY, "parsed text".getBytes());
    keyValues.add(k1);
    Put put = new Put(keyValues);
    adapter.put(put);

    // test getting a single value
    SeekingCurrentIterator iter = adapter.get(new Get(new CRange(foo)));
    Utils.testResultIterator(iter, 1, 1);

    // test non-inclusive end
    iter = adapter.get(new Get(new CRange(foo, true, foo, false)));
    Utils.testResultIterator(iter, 0, 0);

    // test non-inclusive start
    iter = adapter.get(new Get(new CRange(foo, false, foo, true)));
    Utils.testResultIterator(iter, 0, 0);

    // test getting multiple values
    k1 = new CKeyValue(foo, TEST_FAMILY, "otherCQ".getBytes(),
        "otherValue".getBytes());
    adapter.put(new Put(k1));

    iter = adapter.get(new Get(new CRange(foo)));
    Utils.testResultIterator(iter, 1, 2);

    iter = adapter.get(new Get(new CRange(foo), TEST_FAMILY, "otherCQ"
        .getBytes()));
    Utils.testResultIterator(iter, 1, 1);

    // test getting an empty row
    put = new Put(new CKeyValue("bar".getBytes(), TEST_FAMILY,
        "someCQ".getBytes()));
    adapter.put(put);
    iter = adapter.get(new Get(new CRange("bar".getBytes())));
    Utils.testResultIterator(iter, 1, 1);

    // test getting multiple rows
    iter = adapter.get(new Get(new CRange("bar".getBytes(), foo)));
    Utils.testResultIterator(iter, 2, 3);
  }

  /**
   * Tests that a remote exec can properly execute, assuming that there's one
   * record on one region.
   * 
   * @throws Throwable
   */
  @Test
  public void testRemoteExec() throws Throwable {

    // setting up the table
    CKeyValue k1 = new CKeyValue("foo".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    CKeyValue k2 = new CKeyValue("bar".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    adapter.put(new Put(k1, k2));

    // test access to the local table and cover the range of keys
    List<Integer> results = adapter.remoteExec(new byte[0], new byte[0],
        _SpecialRecordGetter.class);
    this.checkResultCount(results, 2, 1);

    // checking an empty argument array
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class);
    this.checkResultCount(results, 2, 0);

    // check a writable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Text("hello"));
    this.checkResultCount(results, 2, 1);

    // check a serializable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Integer(1));
    this.checkResultCount(results, 2, 1);

    // check a serializable and a writable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Integer(1), new Text("hello"));
    this.checkResultCount(results, 2, 2);

    // check an object array
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Object[] { "1", new Integer(2),
            new Text("txt") });
    this.checkResultCount(results, 2, 3);

    // check a byte [] as an object
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new byte[] { 1 });
    this.checkResultCount(results, 2, 1);
  }

  private void checkResultCount(List<Integer> results, int numRegions,
      Integer numResultsPerRegion) {
    // check the number of regions
    assertEquals(2, results.size());
    // check the value for each result
    for (Integer i : results)
      assertEquals(numResultsPerRegion, i);
  }

  /**
   * Test that we fail to serialize a non-serializable and non-writable object
   * @throws Throwable
   */
  @Test(expected = Exception.class)
  public void testFailRemoteExec() throws Throwable {
    // setup the table
    CKeyValue k2 = new CKeyValue("bar".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    adapter.put(new Put(k2));

    // do the test
    List<Integer> results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Object());
  }

  /**
   * shuts down the mini cluster
   */
  @AfterClass
  public static void tearDown() throws Throwable {
    HBaseTableAdapterIT.util.getMiniHBaseCluster().stopRegionServer(0);
    HBaseTableAdapterIT.util.getMiniHBaseCluster().stopRegionServer(1);
    util.shutdownMiniCluster();
  }
}

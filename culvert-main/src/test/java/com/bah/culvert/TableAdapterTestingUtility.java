package com.bah.culvert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Utility to help fully integration test a table adapter
 */
public class TableAdapterTestingUtility {
  
  private static final String TEST_TABLE = "TestTable";
  private static byte[] TEST_FAMILY = "col1".getBytes();
  private static TableAdapter table;

  /**
   * Test all the 'normal' functions of a simple table
   * <p>
   * The table should be fully prepared to run remote executions (preping server
   * side operations, etc).
   * @param db Database Adapter to create the tables
   * @param cleanup Function to clean all the records from the underlying table.
   *        Passed the table name as the input
   * @throws Throwable
   */
  public static void testTableAdapter(DatabaseAdapter db,
      Function<String, Void> cleanup)
      throws Throwable {
    createTable(db);
    testPutGet(table);
    cleanup.apply(TEST_TABLE);
  }

  /**
   * Test that we do all the Remote Exection i/o correctly.
   * @param db Database Adapter
   * @param numRegions number of regions that this will be spread across. This
   *        is added to allow Accumulo to not actually have multiple region
   *        servers in the test suite.
   * @param cleanup Function to clean all the records from the underlying table.
   *        Passed the table name as the input
   * @throws Throwable
   */
  public static void testRemoteExecTableAdapter(DatabaseAdapter db,
      int numRegions, Function<String, Void> cleanup) throws Throwable {
    testRemoteExec(table, numRegions);
    cleanup.apply(TEST_TABLE);
    testFailRemoteExec(table);
    cleanup.apply(TEST_TABLE);
  }

  /**
   * Helper method to create a test table.
   * @throws Throwable
   */
  public static void createTable(DatabaseAdapter databaseAdapter)
      throws Throwable {
    List<CColumn> columns = new ArrayList<CColumn>();
    CColumn col1 = new CColumn(TEST_FAMILY);
    columns.add(col1);
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = "bar1".getBytes();
    databaseAdapter.create(TEST_TABLE, splitKeys, columns);
    table = databaseAdapter.getTableAdapter(TEST_TABLE);
  }

  /**
   * Tests that a record can be inserted without creating an exception.
   * @param adapter {@link TableAdapter} to test
   * @throws Throwable
   */
  private static void testPutGet(TableAdapter adapter) throws Throwable {
    List<CKeyValue> keyValues = new ArrayList<CKeyValue>();
    byte[] foo = "foo".getBytes();
    CKeyValue k1 = new CKeyValue(foo, TEST_FAMILY, "parsed text".getBytes());
    keyValues.add(k1);
    Put put = new Put(keyValues);
    adapter.put(put);

    // test getting a single value
    SeekingCurrentIterator iter = adapter.get(new Get(new CRange(foo)));
    Utils.testResultIterator(iter, 1, Lists.newArrayList(k1));

    // test non-inclusive end
    iter = adapter.get(new Get(new CRange(foo, true, foo, false)));
    Utils.testResultIterator(iter, 0, 0);

    // test non-inclusive start
    iter = adapter.get(new Get(new CRange(foo, false, foo, true)));
    Utils.testResultIterator(iter, 0, 0);

    // test getting multiple values
    CKeyValue k2 = new CKeyValue(foo, TEST_FAMILY, "otherCQ".getBytes(),
        "otherValue".getBytes());
    adapter.put(new Put(k2));

    iter = adapter.get(new Get(new CRange(foo)));
    Utils.testResultIterator(iter, 1, Lists.newArrayList(k2, k1));

    iter = adapter.get(new Get(new CRange(foo), TEST_FAMILY, "otherCQ"
        .getBytes()));
    Utils.testResultIterator(iter, 1, Lists.newArrayList(k2));

    // test getting an empty row
    iter = adapter.get(new Get(new CRange("bar".getBytes())));
    Utils.testResultIterator(iter, 0, 0);

    // test over multiple rows
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
  private static void testRemoteExec(TableAdapter adapter, int regions)
      throws Throwable {

    // setting up the table
    CKeyValue k1 = new CKeyValue("foo".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    CKeyValue k2 = new CKeyValue("bar".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    adapter.put(new Put(k1, k2));

    // test access to the local table and cover the range of keys
    List<Integer> results = adapter.remoteExec(new byte[0], new byte[0],
        _SpecialRecordGetter.class);
    // there are two results, split between the regions.
    checkResultCount(results, regions, 2 / regions);

    // //////////////////////////////////////////////////////////////////////////////
    // Check that we serialize over valid arguments and do so with the correct
    // number of arguments
    // //////////////////////////////////////////////////////////////////////////////
    // checking an empty argument array
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class);
    checkResultCount(results, regions, 0);

    // check a writable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Text("hello"));
    checkResultCount(results, regions, 1);

    // check a serializable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Integer(1));
    checkResultCount(results, regions, 1);

    // check a serializable and a writable
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Integer(1), new Text("hello"));
    checkResultCount(results, regions, 2);

    // check an object array
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Object[] { "1", new Integer(2),
            new Text("txt") });
    checkResultCount(results, regions, 3);

    // check a byte [] as an object
    results = adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new byte[] { 1 });
    checkResultCount(results, regions, 1);
  }

  private static void checkResultCount(List<Integer> results, int numRegions,
      Integer numResultsPerRegion) {
    // check the number of regions
    assertEquals(numRegions, results.size());
    // check the value for each result

    for (Integer i : results)
      assertEquals(numResultsPerRegion, i);
  }

  /**
   * Test that we fail to serialize a non-serializable and non-writable object
   * @throws Throwable
   */
  private static void testFailRemoteExec(TableAdapter adapter) throws Throwable {
    // setup the table
    CKeyValue k2 = new CKeyValue("bar".getBytes(), TEST_FAMILY,
        "parsed text".getBytes());
    adapter.put(new Put(k2));

    // do the test
    try {
      adapter.remoteExec(new byte[0], new byte[0],
        SerializationChecker.class, new Object());
      assertTrue("Remote execution was supposed to fail", false);
    } catch (Exception e) {}
  }

  /**
   * Special getter for the {@link #testRemoteExec(TableAdapter)} test. We know
   * that for that test one record is inserted, so there should be one record to
   * iterate. Returns the count of records in that region.
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

}

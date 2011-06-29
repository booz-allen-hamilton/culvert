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
package com.bah.culvert.inmemory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;

@RunWith(JUnit4.class)
public class TestInMemoryDB {

  private static List<String> cleanupTables = new ArrayList<String>();

  /**
   * Do all necessary cleanup after the test
   * <p>
   * Removes all the created tables
   */
  @After
  public void finalCleanup()
  {
    InMemoryDB db1 = new InMemoryDB();
    for(String tableName: cleanupTables)
      db1.delete(tableName);
  }

  /**
   * Test that we can access the same tables with multiple adapters
   * @throws Exception
   */
  @Test
  public void testGetMultipleTablesAndAdapter() throws Exception
  {
    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Some table");
    // put some data
    table1.put(new Put(Arrays.asList(new CKeyValue("rowid".getBytes()))));

    // create a new adapter
    db1 = new InMemoryDB();
    assertTrue(db1.getTableAdapter("Some table") != null);
    // make sure that we get that data back out
    assertTrue(db1.getTableAdapter("Some table").get(new Get(new CRange("rowid".getBytes())))!= null);
    assertTrue(db1.getTableAdapter("another table") != null);
    cleanupTables.add("Some table");
  }

  /**
   * Test that table doesn't fail when range is found to be outside the key set
   * (start = 0, size = 0);
   * @throws Exception on failure
   */
  @Test
  public void testRangeSize() throws Exception {

    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Another table");
    // test getting a value that does not exist
    table1.get(new Get(new CRange("hello".getBytes())));
    // test getting a value past what we have stored
    table1.put(new Put(Arrays.asList(new CKeyValue(new byte[] { 1 }))));
    table1.get(new Get(new CRange(new byte[] { 2 })));


    // cleanup
    ((InMemoryDB) db1).delete("Another table");
  }

  /**
   * Test that we handle inclusive exclusive cases properly
   * @throws Exception on failure
   */
  @Test
  public void testInclusiveExclusive() throws Exception {
    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Another table");
    table1.put(new Put(new CKeyValue(new byte[] { 2 })));

    // test that we still get nothing back, since outside the range
    this.runTableGet(table1, new byte[] { 2 }, false, new byte[] { 3 }, true, 0);

    // test that we can actually get something back
    this.runTableGet(table1, new byte[] { 2 }, true, new byte[] { 3 }, false, 1);

    // test that we still get back 1 value with 2 values
    table1.put(new Put(new CKeyValue(new byte[] { 1 })));
    this.runTableGet(table1, new byte[] { 2 }, true, new byte[] { 3 }, false, 1);

    // test the other side of the inclusive/exclusive
    this.runTableGet(table1, new byte[] { 1 }, false, new byte[] { 3 }, false, 1);

    // test middle of inclusive/exclusive
    this.runTableGet(table1, new byte[] { 1 }, true, new byte[] { 3 }, false, 2);

    // test other middle of inclusive/exclusive
    this.runTableGet(table1, new byte[] { 1 }, false, new byte[] { 3 }, true, 1);

    // cleanup
    ((InMemoryDB) db1).delete("Another table");
  }

  private void runTableGet(TableAdapter table, byte[] start,
      boolean startInclusivce,
      byte[] end, boolean endInclusive, int numResults) throws Exception {
    Iterator<Result> results = table.get(new Get(new CRange(start,
        startInclusivce, end, endInclusive)));
    int i;
    for (i = 0; results.hasNext(); i++)
      results.next();
    assertEquals(numResults, i);
  }

  /**
   * Test that if we put values into the table, we can get them back out, using
   * full specification syntax (CF, CQ, timestamp) on the values
   * @throws Exception on failure
   */
  @Test
  public void testPutGetTable() throws Exception {
    // get the table
    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Another table");
    cleanupTables.add("Another table");

    // test getting on an empty table
    SeekingCurrentIterator iter = table1.get(new Get(new CRange(new byte[0])));
    assertFalse(iter.hasNext());

    // put into the table
    byte[] rowid = "rowid".getBytes();
    table1.put(new Put(Arrays.asList(new CKeyValue(rowid))));

    // get from the table
    Iterator<Result> values = table1.get(new Get(new CRange(rowid)));
    int results = 0;

    assertTrue(values.hasNext());
    while (values.hasNext()) {
      Result r = values.next();
      assertTrue(Arrays.equals(rowid, r.getRecordId()));
      results++;
    }
    assertEquals(1, results);



    values = table1.get(new Get(new CRange(rowid), new byte[0], new byte[0]));
    results = 0;
    int columns = 0;
    while (values.hasNext()) {
      Result r = values.next();
      assertTrue(Arrays.equals(rowid, r.getRecordId()));
      for (CKeyValue key : r.getKeyValues()) {
        columns++;
      }

      results++;
    }
    assertEquals(1, columns);
    assertEquals(1, results);

    values = table1.get(new Get(new CRange(rowid)));
    results = 0;
    columns = 0;
    while (values.hasNext()) {
      Result r = values.next();
      assertTrue(Arrays.equals(rowid, r.getRecordId()));
      for (CKeyValue key : r.getKeyValues()) {
        columns++;
      }

      results++;
    }
    assertEquals(1, columns);
    assertEquals(1, results);
  }

  /**
   * Test that we correctly get values out based on the timestamp
   */
  @Test
  public void testTimestamp() {
    // get the table
    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Another table");
    cleanupTables.add("Another table");

    int results = 0;
    int columns = 0;

    // put into the table
    byte[] rowid = "rowid".getBytes();
    table1.put(new Put(Arrays.asList(new CKeyValue(rowid))));

    // TODO add testing for timestamps
    // test getting a value with a later timestamp
    // long timestamp = System.currentTimeMillis() + 30;
    //
    // SeekingCurrentIterator values = table1.get(get);
    //
    //
    // while (values.hasNext()) {
    // Result r = values.next();
    // for (CKeyValue key : r.getKeyValues()) {
    // columns++;
    // }
    // results++;
    // }
    // assertEquals(0, columns);
    // assertEquals(0, results);

    Get get = new Get(new CRange(rowid));
    SeekingCurrentIterator values = table1.get(get);

    results = 0;
    columns = 0;
    while (values.hasNext()) {
      Result r = values.next();
      for (CKeyValue key : r.getKeyValues()) {
        columns++;
        assertArrayEquals(new byte[0], key.getFamily());
        assertArrayEquals(new byte[0], key.getQualifier());
        assertArrayEquals(new byte[0], key.getValue());
      }

      results++;
    }
    assertEquals(1, columns);
    assertEquals(1, results);

  }

  @Test
  public void testRange() throws Exception {
    DatabaseAdapter db1 = new InMemoryDB();
    new InMemoryDB().delete("Another table");
    TableAdapter table = db1.getTableAdapter("Another table");
    cleanupTables.add("Another table");

    table.put(new Put(new CKeyValue(new byte[] { 4 },
        "table1.rowID".getBytes(),
        new byte[] { 1 })));
    Iterator<Result> results = table.get(new Get(new CRange(new byte[] { 1 })));
    assertTrue(!results.hasNext());

    table.put(new Put(new CKeyValue(new byte[] { 6 })));
    results = table
    .get(new Get(new CRange(new byte[] { 4 },
        new byte[] { 6 })));

    assertTrue(results.hasNext());
    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    assertEquals(2, count);

    results = table.get(new Get(new CRange(new byte[0], new byte[0])));

    count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    assertEquals(2, count);
  }

  /**
   * Test that table doesn't fail when range is found to be outside the key set
   * (start = 0, size = 0);
   * @throws Exception on failure
   */
  @Test
  public void testRangeSize2() throws Exception {

    DatabaseAdapter db1 = new InMemoryDB();
    TableAdapter table1 = db1.getTableAdapter("Another table");
    // test getting a value that does not exist
    table1.get(new Get(new CRange("hello".getBytes())));
    // test getting a value past what we have stored
    table1.put(new Put(Arrays.asList(new CKeyValue(new byte[] { 1 }))));
    table1.get(new Get(new CRange(new byte[] { 2 }, new byte[] { 3 })));

    // cleanup
    ((InMemoryDB) db1).delete("Another table");
  }  

  /**
   * Test that all version of the CRange provide a hashcode;
   * @throws Exception on failure
   */
  @Test
  public void testRangeHashcode() throws Exception {

    new CRange().hashCode();    
    new CRange(new byte[] { 1 }).hashCode();
    new CRange(new byte[] { 1 }, new byte[] { 2}).hashCode();
  }

  /**
   * Test that all version of the CRange provide a hashcode;
   * @throws Exception on failure
   */
  @Test
  public void testRangeCompare() throws Exception {

    CRange c1 = new CRange();    
    CRange c2 = new CRange(new byte[] { 1 });
    CRange c3 = new CRange(new byte[] { 1 }, new byte[] { 2});
    CRange c4 = new CRange(new byte[] { 4 }, new byte[] { 3});

    if(c1.compareTo(c1) != 0){
      throw new RuntimeException("Invalid Comparison");
    }

    if(c1.compareTo(c2) == 0){
      throw new RuntimeException("Invalid Comparison");
    }

    if(c2.compareTo(c2) != 0){
      throw new RuntimeException("Invalid Comparison");
    }    

    if(c3.compareTo(c2) == 0){
      throw new RuntimeException("Invalid Comparison");
    }

    if(c3.compareTo(null) == 0){
      throw new RuntimeException("Invalid Comparison");
    }

    if(c2.compareTo(c4) == 0){
      throw new RuntimeException("Invalid Comparison");
    }            

  }
}

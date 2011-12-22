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
package com.bah.culvert.constraints.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.filter.KeyOnlyFilter;
import com.bah.culvert.constraints.filter.ResultFilter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.Bytes;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;

/**
 * "Unit" test that we correctly implemented join. Somewhat of an integration
 * test as we are using the {@link InMemoryDB}, but otherwise it is a useless
 * test.
 */
public class TestNaiveJoin {

  /**
   * Test a simple join where columns are present on both sides of the join
   * @throws Throwable
   */
  @Test
  public void testSimpleJoin() throws Throwable {

    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    String joinTable = "table";
    InMemoryTable t1 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(joinTable);
    t1.put(new Put(joinvalues));

    // setup the columns to join the left side on
    // this should yield two join rows - one on 4, one on 7
    CColumn leftColumns = new CColumn(new byte[] { 2 }, new byte[] { 3 });
    Constraint leftConstraint = new ResultFilter(t1, leftColumns);

    // add the values to the "right" side of the join
    List<CKeyValue> remotevalues = new ArrayList<CKeyValue>();
    remotevalues.add(new CKeyValue(new byte[] { 11 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 1 }));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 5 }));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 10 }));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 7 }));

    String remoteTable = "otherTable";
    InMemoryTable t2 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));

    // the right side is just joined on all the values, which is fine since
    // all the values have the same columns
    // create the join
    NaiveJoin join = new NaiveJoin(new InMemoryDB(), t1, leftConstraint,
        remoteTable, new ResultFilter(t2));

    // do the join
    SeekingCurrentIterator iter = join.getResultIterator();
    int count = 0, rows = 0;
    assertTrue(iter.hasNext());
    List<Bytes> rowids = new ArrayList<Bytes>(2);
    List<Bytes> cqs = new ArrayList<Bytes>(4);
    while (iter.hasNext()) {
      Result r = iter.next();
      rows++;
      rowids.add(new Bytes(r.getRecordId()));
      for (CKeyValue kv : r.getKeyValues()) {
        cqs.add(new Bytes(kv.getQualifier()));
        assertEquals(0, kv.getValue().length);
        count++;
      }
      if (rows == 1) {
        assertTrue(cqs.contains(new Bytes(new byte[] { 12 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 2 })));

      } else {
        assertTrue(cqs.contains(new Bytes(new byte[] { 14 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 4 })));
      }
    }
    assertEquals(2, rows);
    assertEquals(4, count);
    assertTrue(rowids.contains(new Bytes(new byte[] { 5 })));
    assertTrue(rowids.contains(new Bytes(new byte[] { 7 })));

    // now do the same test, but only with specifying the columns
    join = new NaiveJoin(new InMemoryDB(), t1, new ResultFilter(t1),
        new CColumn(new byte[] { 2 }), remoteTable, new ResultFilter(t2),
        new CColumn(new byte[] { 9 }));
    iter = join.getResultIterator();

    assertTrue(iter.hasNext());
    rows = 0;
    count = 0;
    rowids = new ArrayList<Bytes>(2);
    cqs = new ArrayList<Bytes>(4);
    while (iter.hasNext()) {
      Result r = iter.next();
      rows++;
      rowids.add(new Bytes(r.getRecordId()));
      for (CKeyValue kv : r.getKeyValues()) {
        cqs.add(new Bytes(kv.getQualifier()));
        assertEquals(0, kv.getValue().length);
        count++;
      }
      if (rows == 1) {
        assertTrue(cqs.contains(new Bytes(new byte[] { 12 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 2 })));

      } else {
        assertTrue(cqs.contains(new Bytes(new byte[] { 14 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 4 })));
      }
    }
    assertEquals(2, rows);
    assertEquals(4, count);
    assertTrue(rowids.contains(new Bytes(new byte[] { 5 })));
    assertTrue(rowids.contains(new Bytes(new byte[] { 7 })));

    // do cleanup
    new InMemoryDB().delete(remoteTable);
    new InMemoryDB().delete(joinTable);
  }

  /**
   * Test a join where the columns from the right constraint need to be paired
   * down via another selection
   * @throws Throwable
   */
  @Test
  public void testNotAllColumnsMatchJoin() throws Throwable {

    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    String joinTable = "table" + UUID.randomUUID().toString();
    InMemoryTable t1 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(joinTable);
    t1.put(new Put(joinvalues));

    // setup the columns to join the left side on
    // this should yield two join rows - one on 4, one on 7
    CColumn leftColumns = new CColumn(new byte[] { 2 }, new byte[] { 3 });
    Constraint leftConstraint = new ResultFilter(t1, leftColumns);

    // add the values to the "right" side of the join
    List<CKeyValue> remotevalues = new ArrayList<CKeyValue>();
    remotevalues.add(new CKeyValue(new byte[] { 11 }, new byte[] { 8 },
        new byte[] { 10 }, new byte[] { 1 }));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 11 }, new byte[] { 5 }));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 10 }));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 7 }));

    String remoteTable = "otherTable" + UUID.randomUUID().toString();
    TableAdapter t2 = new InMemoryDB().getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));

    // create the join
    NaiveJoin join = new NaiveJoin(new InMemoryDB(), t1, leftConstraint,
        remoteTable, new ResultFilter(t2, new CColumn(new byte[] { 9 },
            new byte[] { 10 })));

    // do the join
    SeekingCurrentIterator iter = join.getResultIterator();
    int count = 0, rows = 0;
    assertTrue(iter.hasNext());
    List<Bytes> rowids = new ArrayList<Bytes>(2);
    List<Bytes> cqs = new ArrayList<Bytes>(4);
    while (iter.hasNext()) {
      Result r = iter.next();
      rows++;
      rowids.add(new Bytes(r.getRecordId()));
      for (CKeyValue kv : r.getKeyValues()) {
        cqs.add(new Bytes(kv.getQualifier()));
        assertEquals(0, kv.getValue().length);
        count++;
      }
      if (rows == 1) {
        assertTrue(cqs.contains(new Bytes(new byte[] { 14 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 4 })));
      } else
        assertTrue(false);
    }
    assertEquals(1, rows);
    assertEquals(2, count);
    assertTrue(rowids.contains(new Bytes(new byte[] { 7 })));

    // do cleanup
    new InMemoryDB().delete(remoteTable);
    new InMemoryDB().delete(joinTable);
  }

  /**
   * Test a join where the there are no rows from the left constraint, and
   * therefore nothing to join on, leaving an empty output
   * @throws Throwable
   */
  @Test
  public void testNoLeftColumnsJoin() throws Throwable {

    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    String joinTable = "table";
    InMemoryTable t1 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(joinTable);
    t1.put(new Put(joinvalues));

    // setup the columns to join the left side on
    // this should yield two join rows - but this left constraint doesn't get
    // any of them
    CColumn leftColumns = new CColumn(new byte[] { 2 }, new byte[] { 3 });
    Constraint left = new ResultFilter(t1, new CColumn(new byte[] { 3 }));

    // add the values to the "right" side of the join
    List<CKeyValue> remotevalues = new ArrayList<CKeyValue>();
    remotevalues.add(new CKeyValue(new byte[] { 11 }, new byte[] { 8 },
        new byte[] { 10 }, new byte[] { 1 }));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 11 }, new byte[] { 5 }));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 10 }));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 7 }));

    String remoteTable = "otherTable";
    TableAdapter t2 = new InMemoryDB().getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));

    // this constraint won't return any values
    CColumn rightColumns = new CColumn(new byte[] { 9 }, new byte[] { 10 });

    // create the join
    NaiveJoin join = new NaiveJoin(new InMemoryDB(), t1, left, leftColumns,
        remoteTable, new ResultFilter(t2, new CColumn(new byte[] { 1 })),
        rightColumns);

    // do the join
    SeekingCurrentIterator iter = join.getResultIterator();
    assertFalse(iter.hasNext());

    // do cleanup
    new InMemoryDB().delete(remoteTable);
    new InMemoryDB().delete(joinTable);
  }

  /**
   * Test a join where the columns from the left side of the join are not
   * present and need to be retrieved from the actual table
   * @throws Throwable
   */
  @Test
  public void testNotAllColumnsPresentJoin() throws Throwable {

    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    String joinTable = "table";
    InMemoryTable t1 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(joinTable);
    t1.put(new Put(joinvalues));

    // setup the columns to join the left side on
    // this should yield two join rows - but this left constraint doesn't get
    // any of them
    CColumn leftColumns = new CColumn(new byte[] { 2 }, new byte[] { 3 });
    Constraint left = new KeyOnlyFilter(t1, new CColumn(new byte[] { 2 }));

    // add the values to the "right" side of the join
    List<CKeyValue> remotevalues = new ArrayList<CKeyValue>();
    remotevalues.add(new CKeyValue(new byte[] { 11 }, new byte[] { 8 },
        new byte[] { 10 }, new byte[] { 1 }));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 11 }, new byte[] { 5 }));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 10 }));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 7 }));

    String remoteTable = "otherTable";
    TableAdapter t2 = new InMemoryDB().getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));

    // this constraint will return on a single value
    CColumn rightColumns = new CColumn(new byte[] { 9 }, new byte[] { 11 });

    // create the join
    NaiveJoin join = new NaiveJoin(new InMemoryDB(), t1, left, leftColumns,
        remoteTable, new ResultFilter(t2), rightColumns);

    // do the join
    SeekingCurrentIterator iter = join.getResultIterator();
    int count = 0, rows = 0;
    assertTrue(iter.hasNext());
    List<Bytes> rowids = new ArrayList<Bytes>(2);
    List<Bytes> cqs = new ArrayList<Bytes>(4);
    while (iter.hasNext()) {
      Result r = iter.next();
      rows++;
      rowids.add(new Bytes(r.getRecordId()));
      for (CKeyValue kv : r.getKeyValues()) {
        cqs.add(new Bytes(kv.getQualifier()));
        assertEquals(0, kv.getValue().length);
        count++;
      }
      if (rows == 1) {
        assertTrue(cqs.contains(new Bytes(new byte[] { 12 })));
        assertTrue(cqs.contains(new Bytes(new byte[] { 2 })));

      } else {
        assertTrue(false);
      }
      assertEquals(1, rows);
      assertEquals(2, count);
      assertTrue(rowids.contains(new Bytes(new byte[] { 5 })));

      // do cleanup
      new InMemoryDB().delete(remoteTable);
      new InMemoryDB().delete(joinTable);
    }
  }

  @Test
  public void testReadWrite() throws IOException, InstantiationException,
      IllegalAccessException {
    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    String joinTable = "table";
    InMemoryTable t1 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(joinTable);
    t1.put(new Put(joinvalues));

    // setup the columns to join the left side on
    // this should yield two join rows - one on 4, one on 7
    CColumn leftColumns = new CColumn(new byte[] { 2 }, new byte[] { 3 });
    Constraint leftConstraint = new ResultFilter(t1, leftColumns);

    // add the values to the "right" side of the join
    List<CKeyValue> remotevalues = new ArrayList<CKeyValue>();
    remotevalues.add(new CKeyValue(new byte[] { 11 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 1 }));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 5 }));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 10 }));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, new byte[] { 7 }));

    String remoteTable = "otherTable";
    InMemoryTable t2 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));

    // the right side is just joined on all the values, which is fine since
    // all the values have the same columns
    // create the join
    NaiveJoin join = new NaiveJoin(new InMemoryDB(), t1, leftConstraint,
        remoteTable, new ResultFilter(t2));
    Utils.testReadWrite(join);
  }

}

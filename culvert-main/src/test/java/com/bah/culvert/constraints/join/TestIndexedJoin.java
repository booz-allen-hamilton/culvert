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
package com.bah.culvert.constraints.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.filter.ResultFilter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.data.index.TermBasedIndex;
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
public class TestIndexedJoin {

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
        new byte[] { 3 }, "hello".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, "world".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, "foo".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, "bar".getBytes()));

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
        new byte[] { 10 }, "other".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 10 }, "world".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, "another".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, "bar".getBytes()));

    String remoteTable = "otherTable";
    Index remoteIndex = new TermBasedIndex("testIndex", new InMemoryDB(),
        new Configuration(), remoteTable, "index", new byte[] { 9 }, new byte[] { 10 });
    InMemoryTable t2 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));
    remoteIndex.handlePut(new Put(remotevalues));

    // the right side is just joined on all the values, which is fine since
    // all the values have the same columns
    // create the join
    IndexedJoin join = new IndexedJoin(new InMemoryDB(), t1, leftConstraint,
        leftColumns, remoteIndex);

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
    }
    assertTrue(cqs.contains(new Bytes(new byte[] { 14 })));
    assertTrue(cqs.contains(new Bytes(new byte[] { 4 })));
    assertTrue(cqs.contains(new Bytes(new byte[] { 12 })));
    assertTrue(cqs.contains(new Bytes(new byte[] { 2 })));
    assertEquals(2, rows);
    assertEquals(4, count);
    assertTrue(rowids.contains(new Bytes("bar".getBytes())));
    assertTrue(rowids.contains(new Bytes("world".getBytes())));
  }

  @Test
  public void testReadWrite() throws Exception {
    // setup the tables
    // add values to the "left" side of the join
    List<CKeyValue> joinvalues = new ArrayList<CKeyValue>();
    joinvalues.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, "hello".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, "world".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, "foo".getBytes()));
    joinvalues.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, "bar".getBytes()));

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
        new byte[] { 10 }, "other".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 12 }, new byte[] { 9 },
        new byte[] { 10 }, "world".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 13 }, new byte[] { 9 },
        new byte[] { 10 }, "another".getBytes()));
    remotevalues.add(new CKeyValue(new byte[] { 14 }, new byte[] { 9 },
        new byte[] { 10 }, "bar".getBytes()));

    String remoteTable = "otherTable";
    Index remoteIndex = new TermBasedIndex("testIndex", new InMemoryDB(),
        new Configuration(), remoteTable, "index", new byte[] { 9 }, new byte[] { 10 });
    InMemoryTable t2 = (InMemoryTable) new InMemoryDB()
        .getTableAdapter(remoteTable);
    t2.put(new Put(remotevalues));
    remoteIndex.handlePut(new Put(remotevalues));

    // the right side is just joined on all the values, which is fine since
    // all the values have the same columns
    // create the join
    IndexedJoin join = new IndexedJoin(new InMemoryDB(), t1, leftConstraint,
        leftColumns, remoteIndex);
    Utils.testReadWrite(join);
  }
}

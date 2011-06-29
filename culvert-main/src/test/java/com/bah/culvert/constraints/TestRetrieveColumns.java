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
package com.bah.culvert.constraints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.bah.culvert.constraints.filter.KeyOnlyFilter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;

/**
 * Test that we can properly retrieve columns from an index giving us a list of
 * values
 */
public class TestRetrieveColumns {

  /**
   * Test that we get all the columns
   * @throws Exception
   */
  @Test
  public void testRetrieveAllColumns() throws Exception {
    // setup the table
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 5 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 6 }));
    values.add(new CKeyValue(new byte[] { 4 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 7 }));

    InMemoryTable t1 = new InMemoryTable();
    t1.put(new Put(values));

    // test that we pull back any and all columns
    SeekingCurrentIterator iter = new RetrieveColumns(new KeyOnlyFilter(t1), t1)
        .getResultIterator();
    assertTrue(iter.hasNext());
    assertCount(iter, 4, 4);
  }

  /**
   * Test that we get only specific columns
   * @throws Exception
   */
  @Test
  public void testRetrieveSpecificColumns() throws Exception {
    // setup the table
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, 0, new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 3 }, 1, new byte[] { 5 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 3 }, 2, new byte[] { 6 }));
    values.add(new CKeyValue(new byte[] { 4 }, new byte[] { 4 },
        new byte[] { 4 }, 3, new byte[] { 7 }));

    InMemoryTable t1 = new InMemoryTable();
    t1.put(new Put(values));

    // test for getting a subset of results
    SeekingCurrentIterator iter = new RetrieveColumns(new KeyOnlyFilter(t1,
        new CColumn(new byte[] { 2 }), new CColumn(new byte[] { 3 })), t1)
        .getResultIterator();
    assertTrue(iter.hasNext());
    assertCount(iter, 2, 2);

    // test for only getting results with matching columns
    iter = new RetrieveColumns(new KeyOnlyFilter(t1, new CColumn(
        new byte[] { 2 }), new CColumn(new byte[] { 3 })), t1, new CColumn(
        new byte[] { 3 })).getResultIterator();
    assertTrue(iter.hasNext());
    assertCount(iter, 1, 1);

    // get getting columns with matching timestamps
    iter = new RetrieveColumns(new KeyOnlyFilter(t1, new CColumn(
        new byte[] { 4 })), t1).getResultIterator();
    assertTrue(iter.hasNext());
    assertCount(iter, 2, 2);
  }

  private void assertCount(SeekingCurrentIterator iter, int columns, int results) {
    int cols = 0;
    int res = 0;
    while (iter.hasNext()) {
      Result r = iter.next();
      for (@SuppressWarnings("unused")
      CKeyValue kv : r.getKeyValues()) {
        cols++;
      }
      res++;
    }
    assertEquals(columns, cols);
    assertEquals(results, res);
  }

  @Test
  public void testWriteRead() throws Exception {
    // setup the table
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, 0, new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 3 }, 1, new byte[] { 5 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 3 }, 2, new byte[] { 6 }));
    values.add(new CKeyValue(new byte[] { 4 }, new byte[] { 4 },
        new byte[] { 4 }, 3, new byte[] { 7 }));

    InMemoryTable t1 = new InMemoryTable();
    t1.put(new Put(values));
    Constraint ct = new RetrieveColumns(new KeyOnlyFilter(t1, new CColumn(
        new byte[] { 2 }),
        new CColumn(new byte[] { 3 })), t1);
    Utils.testReadWrite(ct);
  }
}

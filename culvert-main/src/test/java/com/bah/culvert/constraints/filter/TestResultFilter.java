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
package com.bah.culvert.constraints.filter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.mock.MockConstraint;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;

/**
 * Test that we properly select columns from a table
 */
public class TestResultFilter {

  /**
   * Test that we correctly return all the results
   * @throws Exception
   */
  @Test
  public void selectStar() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));
    ResultFilter selectStar = new ResultFilter(table);
    Iterator<Result> results = selectStar.getResultIterator();
    assertCount(results, 3);
    assertSeek(selectStar.getResultIterator(), new byte[] { 2 }, true, 1,
        new byte[0], new byte[] { 6 });
  }

  private void assertCount(Iterator<Result> results, int total) {
    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    assertEquals(total, count);
  }

  private void assertSeek(SeekingCurrentIterator iter, byte[] seek,
      boolean valid, int numMoreResults, byte[]... values) {
    assertEquals("Test seek has not been correctly configured.",
        (valid ? numMoreResults + 1 : numMoreResults), values.length);
    int i = 0;
    iter.seek(seek);
    Result r = iter.current();
    if (valid) {
      for (CKeyValue kv : r.getKeyValues())
        assertArrayEquals(values[i++], kv.getValue());
    } else
      assertTrue(r == null);
    while (iter.hasNext()) {
      r = iter.next();
      for (CKeyValue kv : r.getKeyValues()) {
        assertArrayEquals(values[i++], kv.getValue());
      }
    }
    assertEquals("Didn't have enough results", values.length, i);
  }

  /**
   * Test that we correctly select based on the specified range
   * @throws Exception
   */
  @Test
  public void selectRange() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));
    ResultFilter range = new ResultFilter(table, new CRange(new byte[] { 1 },
        new byte[] { 3 }));
    assertCount(range.getResultIterator(), 3);
    SeekingCurrentIterator iter = range.getResultIterator();
    iter.seek(new byte[] { 2 });
    int count = 0;
    assertArrayEquals(new byte[] { 2 }, iter.current().getRecordId());
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertEquals(1, count);

    range = new ResultFilter(table, new CRange(new byte[] { 2 }));
    assertCount(range.getResultIterator(), 1);

  }

  /**
   * Test that we correctly select based on the CF
   * @throws Exception
   */
  @Test
  public void selectCF() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 3 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));
    ResultFilter families = new ResultFilter(table, new CColumn(
        new byte[] { 3 }));
    assertCount(families.getResultIterator(), 2);
    assertSeek(families.getResultIterator(), new byte[] { 2 }, true, 1,
        new byte[0], new byte[] { 6 });
    families = new ResultFilter(table, new CColumn(new byte[] { 2 }));
    assertSeek(families.getResultIterator(), new byte[] { 2 }, false, 0);
  }

  /**
   * Test that we correct select based on the CQ
   * @throws Exception
   */
  @Test
  public void selectCQ() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));
    ResultFilter families = new ResultFilter(table, new CColumn(
        new byte[] { 3 }, new byte[] { 4 }));
    assertCount(families.getResultIterator(), 1);
    families = new ResultFilter(table, new CColumn(new byte[] { 3 },
        new byte[] { 5 }));
    assertCount(families.getResultIterator(), 0);
  }

  /**
   * Test that we correctly get multiple columns from the table
   * @throws Exception
   */
  @Test
  public void selectMultipleColumns() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));

    ResultFilter columns = new ResultFilter(table,
        new CColumn(new byte[] { 3 }), new CColumn(new byte[] { 4 }));
    assertCount(columns.getResultIterator(), 2);

    columns = new ResultFilter(table, new CColumn(new byte[] { 3 }));
    assertCount(columns.getResultIterator(), 1);

    columns = new ResultFilter(table, new CColumn(new byte[] { 3 },
        new byte[] { 4 }));
    assertCount(columns.getResultIterator(), 1);

    columns = new ResultFilter(table, new CColumn(new byte[] { 3 },
        new byte[] { 5 }));
    assertCount(columns.getResultIterator(), 0);

    columns = new ResultFilter(table, new CColumn(new byte[] { 4 }));
    assertCount(columns.getResultIterator(), 1);
  }

  @Test
  public void testReadWrite() throws Exception {
    InMemoryTable table = new InMemoryTable();
    ResultFilter fitler = new ResultFilter(table);
    ResultFilter ret = (ResultFilter) Utils.testReadWrite(fitler);
    assertEquals(fitler, ret);
    fitler = new ResultFilter(CRange.FULL_TABLE_RANGE, new MockConstraint());
    ret = (ResultFilter) Utils.testReadWrite(fitler);
    assertTrue(fitler.equals(ret));
  }
}

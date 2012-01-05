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

import static org.junit.Assert.assertEquals;

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
import com.bah.culvert.test.Utils;
import com.bah.culvert.transactions.Put;

/**
 * Test reading just row ids as a constraint from a table
 */
public class KeyOnlyFilterTest {

  /**
   * Test simple range selection cases
   */
  @Test
  public void testSimpleSelection() {

    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));

    KeyOnlyFilter rows = new KeyOnlyFilter(table);
    Iterator<Result> results = rows.getResultIterator();
    int count = 0;
    while (results.hasNext()) {
      count++;
      results.next();
    }
    assertEquals(3, count);

    rows = new KeyOnlyFilter(table, new CRange(new byte[] { 2 }, new byte[0]));
    results = rows.getResultIterator();
    count = 0;
    while (results.hasNext()) {
      count++;
      results.next();
    }
    assertEquals(2, count);

    rows = new KeyOnlyFilter(table, new CRange(new byte[] { 2 }));
    results = rows.getResultIterator();
    count = 0;
    while (results.hasNext()) {
      count++;
      results.next();
    }
    assertEquals(1, count);

  }

  /**
   * Test selection with columns
   */
  @Test
  public void testWithColumnsSelected() {

    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));

    KeyOnlyFilter rows = new KeyOnlyFilter(table, new CColumn(new byte[] { 3 }));
    Iterator<Result> results = rows.getResultIterator();
    int count = 0;
    while (results.hasNext()) {
      count++;
      results.next();
    }
    assertEquals(1, count);

    rows = new KeyOnlyFilter(table, new CColumn(new byte[] { 3 }), new CColumn(
        new byte[] { 4 }));
    results = rows.getResultIterator();
    count = 0;
    while (results.hasNext()) {
      count++;
      results.next();
    }
    assertEquals(2, count);
  }

  @Test
  public void testReadWrite() throws Exception {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));

    KeyOnlyFilter rows = new KeyOnlyFilter(table, new CColumn(new byte[] { 3 }));
    Utils.testReadWrite(rows);
  }

}

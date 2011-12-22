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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.NullResult;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.mock.MockConstraint;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Utils;

public class TestFilteredConstraint {

  @Test
  public void testGetResultIterator() {
    TableAdapter table = new InMemoryTable();
    List<CKeyValue> values = new ArrayList<CKeyValue>();
    values.add(new CKeyValue(new byte[] { 1 }));
    values.add(new CKeyValue(new byte[] { 2 }, new byte[] { 3 },
        new byte[] { 4 }));
    values.add(new CKeyValue(new byte[] { 3 }, new byte[] { 4 },
        new byte[] { 5 }, 0, new byte[] { 6 }));

    table.put(new Put(values));

    FilteredConstraint filter = new FilteredConstraint(new ResultFilter(table),
        new Filter());

    Utils.testResultIterator(filter.getResultIterator(), 3, 3);

    // test the other methods not covered by simple counting
    SeekingCurrentIterator iter = filter.getResultIterator();
    assertTrue(iter.current() == null);
    iter.seek(new byte[] { 2 });
    assertTrue(iter.current() != null);
    assertTrue(iter.hasNext());
    assertArrayEquals(new byte[] { 3 }, iter.next().getRecordId());

    filter = new FilteredConstraint(new ResultFilter(table),
        new AcceptNoneFilter());

    Utils.testResultIterator(filter.getResultIterator(), 0, 0);
  }

  @Test
  public void testReadWrite() throws Exception {
    FilteredConstraint ct = new FilteredConstraint(new MockConstraint(),
        new Filter());
    Utils.testReadWrite(ct);
  }

  private static class AcceptNoneFilter extends Filter {

    public AcceptNoneFilter() {
    }

    @Override
    public Result apply(Result toFilter) {
      return NullResult.INSTANCE;
    }

  }
}

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
package com.bah.culvert.inmemory;

import java.util.Iterator;
import java.util.SortedMap;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Get;

/**
 * Local table adapter that uses an {@link InMemoryTable}.
 */
public class InMemoryLocalTableAdapter extends LocalTableAdapter {

  private final InMemoryTable inMemoryTable;

  /**
   * Create the adapter around the table
   * @param inMemoryTable
   */
  public InMemoryLocalTableAdapter(InMemoryTable inMemoryTable) {
    this.inMemoryTable = inMemoryTable;
  }

  @Override
  public Iterator<Result> get(Get get) {
    return this.inMemoryTable.get(get);
  }

  @Override
  public byte[] getStartKey() {
    SortedMap<Bytes, InMemoryFamily> map = this.inMemoryTable.getTable();
    if (map.size() == 0)
      return new byte[0];
    return map.firstKey().getBytes();
  }

  @Override
  public byte[] getEndKey() {
    SortedMap<Bytes, InMemoryFamily> map = this.inMemoryTable.getTable();
    if (map.size() == 0)
      return new byte[0];
    return map.lastKey().getBytes();
  }

}

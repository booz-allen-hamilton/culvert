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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;

/**
 * An in-memory database that just stores a bunch of in-memory tables
 */
public class InMemoryDB extends DatabaseAdapter {

  private static final Map<String, InMemoryTable> tables = new TreeMap<String, InMemoryTable>();

  @Override
  public TableAdapter getTableAdapter(String tableName) {
    synchronized (tables) {
      InMemoryTable table = tables.get(tableName);
      if (table == null) {

        table = new InMemoryTable();
        table.setTableName(tableName);
        tables.put(tableName, table);
      }
      return table;
    }
  }

  /**
   * Exposed for cleanup in testing
   * @param tableName to remove
   */
  @Override
  public void delete(String tableName) {
    synchronized (tables) {
      tables.remove(tableName);
    }
  }

  @Override
  public void create(String name, byte[][] splitKeys, List<CColumn> columns) {
    synchronized (tables) {
      tables.remove(name);
      InMemoryTable table = new InMemoryTable();
      table.setTableName(name);
      tables.put(name, table);
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    return tables.get(tableName) != null;
  }

  /**
   * Clears the whole in-memory database. This is useful when running JUnit
   * tests in the same JVM.
   */
  public static void clear() {
    tables.clear();
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // noop

  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // noop

  }

}

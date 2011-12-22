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
package com.bah.culvert.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;

public class MockDatabaseAdapter extends DatabaseAdapter {

  @Override
  public TableAdapter getTableAdapter(String tableName) {
    return new MockTableAdapter();
  }

  @Override
  public boolean verify() {
    return false;
  }

  @Override
  public boolean tableExists(String tableName) {
    return false;
  }

  @Override
  public void create(String tableName, byte[][] splitKeys, List<CColumn> columns) {
  }

  @Override
  public void delete(String tableName) {
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

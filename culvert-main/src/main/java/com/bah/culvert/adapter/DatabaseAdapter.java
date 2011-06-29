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
package com.bah.culvert.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.CColumn;
import com.bah.culvert.util.BaseConfigurable;

/**
 * A connection to a database. Subclasses of this object are used to connect to
 * a database.
 */
public abstract class DatabaseAdapter extends BaseConfigurable implements
    Configurable, Writable {


  public static final byte[][] NO_SPLIT_KEYS = new byte[0][];
  
  
  /**
   * Get a connection to the specified table
   * 
   * @param tableName
   *          to connect to
   * @return a {@link TableAdapter} connecting to the specified database
   */
  public abstract TableAdapter getTableAdapter(String tableName);

  public abstract void create(String tableName, byte[][] splitKeys,
      List<CColumn> columns);
  
  public void create(String tablename, List<CColumn> columns){
    create(tablename, NO_SPLIT_KEYS, columns);
  }

  public abstract void delete(String tableName);

  /**
   * Verifies settings to connect to a databse and access a table.
   */
  public boolean verify() {
    // noop
    return true;
  }

  public abstract boolean tableExists(String tableName) ;

  @Override
  public void readFields(DataInput in) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(in);
    this.verify();

  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.getConf().write(out);
  }
}

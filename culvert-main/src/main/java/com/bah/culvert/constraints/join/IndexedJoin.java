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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.Join;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.util.Bytes;

/**
 * Do an Indexed Join. The left constraint is first applied to the table in
 * {@link #getResultIterator()}, and the the specified column in
 * {@link #leftColumn} are used to extract the desired columns from the 'left'
 * side of the join. The {@link IndexedJoinOp} is then used remotely on the
 * output table to retrieve matching values from the Index on the "right" side
 * of the join.
 */
public class IndexedJoin extends Join {

  private Index rightIndex;

  /**
   * Nullary constructor - for use with {@link #readFields(DataInput)}
   */
  public IndexedJoin() {

  }

  /**
   * Create an index based join.
   * @param db Database to use when creating the temporary table
   * @param leftTable
   * @param left
   * @param leftColumn
   * @param rightIndex Index to use when looking up values for the join. This
   *        should be an index over the VALUEs of the rows in the primary table
   *        where the columns match the specified columns.
   */
  public IndexedJoin(DatabaseAdapter db, TableAdapter leftTable,
      Constraint left, CColumn leftColumn, Index rightIndex) {
    super(db, leftTable, left, leftColumn, rightIndex.getPrimaryTable()
        .getTableName());
    this.rightIndex = rightIndex;
  }

  @Override
  protected void doRemoteOperation(TableAdapter outputTable,
      byte[] rightOutputColumn) {
    Object[] args = new Object[4];
    args[0] = outputTable;
    args[1] = rightOutputColumn;
    args[2] = this.rightIndex;
    outputTable.remoteExec(Bytes.START_END_KEY, Bytes.START_END_KEY,
        IndexedJoinOp.class, args);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);
    ow.readFields(in);
    this.rightIndex = (Index) ow.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    new ObjectWritable(this.rightIndex).write(out);
  }

}

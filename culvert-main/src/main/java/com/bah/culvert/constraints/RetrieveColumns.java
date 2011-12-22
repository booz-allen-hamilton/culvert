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
package com.bah.culvert.constraints;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.google.common.base.Preconditions;

/**
 * Retrieve the specified columns. The subconstraint should retrieve a list of
 * results with row ids which will then be used to pull the rows from the
 * primary tables with the specified columns. This should be paired with an
 * index to pull the row ids.
 * <p>
 * Note: This must be used with a subconstraint.
 * @see Constraint
 * @see Index
 */
public class RetrieveColumns extends Constraint {

  private TableAdapter table;
  private Constraint subConstraint;
  private CColumn[] columns;

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public RetrieveColumns() {

  }

  /**
   * Get all the columns after applying the subconstraint
   * @param subConstraint This should be based off an index and just return row
   *        ids in the results, but can theoretically anything.
   * @param primaryTable table from which to retrieve rows
   */
  public RetrieveColumns(Constraint subConstraint, TableAdapter primaryTable) {
    Preconditions.checkNotNull(subConstraint,
        "Must have a subconstraint to apply to the main table.");
    Preconditions.checkNotNull(primaryTable,
        "Must have a main table from which to get rows.");
    this.subConstraint = subConstraint;
    this.columns = new CColumn[0];
    this.table = primaryTable;
  }

  /**
   * Retrieve the specified columns from the table in
   * {@link #getResultIterator()} based on the row id's returned from the
   * subCosntraint.
   * @param subConstraint to apply
   * @param primaryTable table for which to retrieve rows
   * @param columns to retrieve
   */
  public RetrieveColumns(Constraint subConstraint, TableAdapter primaryTable,
      CColumn... columns) {
    this(subConstraint, primaryTable);

    // set the columns
    this.columns = columns == null ? new CColumn[0] : columns;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.columns = new CColumn[in.readInt()];
    for (int i = 0; i < this.columns.length; i++) {
      CColumn column = new CColumn();
      column.readFields(in);
      this.columns[i] = column;
    }
    this.subConstraint = Constraint.readFromStream(in);
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);
    ow.readFields(in);
    this.table = (TableAdapter) ow.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.columns.length);
    for (int i = 0; i < this.columns.length; i++) {
      this.columns[i].write(out);
    }
    Constraint.write(this.subConstraint, out);
    new ObjectWritable(this.table).write(out);
  }

  @Override
  public SeekingCurrentIterator getResultIterator() {

    SeekingCurrentIterator results = this.subConstraint.getResultIterator();
    return new RetrieveColumnsIterator(results, this.table, this.columns);
  }

}

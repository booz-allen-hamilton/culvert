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

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.Join;
import com.bah.culvert.data.CColumn;

/**
 * SQL-style join on a single column.
 * <p>
 * This implements the inner join semantics: returns row when there is a match
 * in all the result sets.
 * <p>
 * Currently the implementation takes the results set from the 'left' constraint
 * applied to the sent table and then does a select on the specified columns
 * from those results. The output is then dumped into a temporary table. The
 * output table then runs the JoinOp on the server to pull in matching values
 * from the "right" table, where results are paired down by first running the
 * "right" constraint on that table. No extra rows or columns are retrieved from
 * the output of these results. The results of the join on each remote server
 * are then dumped into an 'output' table and iterated over.
 */
public class NaiveJoin extends Join {

  private Constraint right;
  private CColumn rightColumn;

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public NaiveJoin() {

  }

  /**
   * Join the specified constraints based on the specified column.
   * <p>
   * The left constraint is first applied to the table in
   * {@link #getResultIterator()}, and the the specified column in
   * {@link #leftColumn} are used to extract the desired columns from the 'left'
   * side of the join. The {@link Constraint} in {@link #right} is then applied
   * and the resulting values are then used for the 'right' side of the join.
   * Where values match for the left and right side of the join, a value is
   * returned in the output table.
   * @param db Database to use when creating the temporary table
   * @param leftTable from which to retrieve necessary columns
   * @param left Constraint to apply on the table before running the join. NOTE:
   *        must leave the specified columns in the output set.
   * @param leftColumn column to select from the table and join the results of
   *        the constraint on
   * @param rightTable table to get rows from the right side of the join
   * @param rightConstraint to use when doing the join. Matching values will be
   *        used in the join's comparison
   * @param rightColumn column to select from the table and join the results of
   *        the constraint on
   */
  public NaiveJoin(DatabaseAdapter db, TableAdapter leftTable, Constraint left,
      CColumn leftColumn, String rightTable, Constraint rightConstraint,
      CColumn rightColumn) {
    super(db, leftTable, left, leftColumn, rightTable);
    this.rightColumn = rightColumn;
    this.right = rightConstraint;
  }

  /**
   * Just join on the results of the left and the right constraint, using all
   * the returned columns.
   * <p>
   * Note: This only compares the values, the CFs used may not make sense IF
   * they are all not uniform
   * @param db Database to use when creating the temporary table
   * @param leftTable from which to retrieve necessary columns
   * @param left to apply to table in {@link #getResultIterator()}
   * @param right to apply to the right side of the join
   * @param rightTable table to get rows from the right side of the join
   */
  public NaiveJoin(DatabaseAdapter db, TableAdapter leftTable, Constraint left,
      String rightTable, Constraint right) {
    this(db, leftTable, left, CColumn.ALL_COLUMNS, rightTable, right,
        CColumn.ALL_COLUMNS);
  }

  @Override
  protected void doRemoteOperation(TableAdapter outputTable,
      byte[] rightOutputColumn) {
    Object[] args = new Object[4];
    args[0] = outputTable;
    args[1] = rightOutputColumn;
    args[2] = this.right;
    args[3] = this.rightColumn;
    outputTable.remoteExec(new byte[0], new byte[0], NaiveJoinOp.class, args);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.right = Constraint.readFromStream(in);
    this.rightColumn = new CColumn();
    this.rightColumn.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Constraint.write(this.right, out);
    this.rightColumn.write(out);
  }

}

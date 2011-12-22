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

import java.util.Iterator;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.IndexRangeConstraint;
import com.bah.culvert.constraints.RetrieveColumns;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;

/**
 * Implement a join on a remote table. Table is pre loaded with the row ids and
 * the values needed to do the operation. Just needs to look into the other
 * table and do a lookup in that table.
 * <p>
 * RowIds are the "key" to join on, column family is the original table name,
 * and the column qualifier is the original table's row id.
 * <p>
 * Assuming that we are doing an 'inner' join - matching value must be found in
 * all tables for the row to be added
 * <p>
 * Arguments (in order) to {@link #call(Object...)} are:
 * <ol>
 * <li>String::Name of the table</li>
 * <li>byte[]::Column family name to store the remote table's values under</li>
 * <li>TableConstraint:: To apply to the remote table to get the results</li>
 * <li>CColumn::To select from the result of the remote table</li>
 * </ol>
 * @see NaiveJoin
 */
public class IndexedJoinOp extends RemoteOp<Void> {

  /*
   * This is now running over a table with the form: rowID = value to join on CF
   * = filteredTableName.rowID CQ = the row ID.
   */
  @Override
  public Void call(Object... args) throws Exception {
    // create the table to write the output results
    final TableAdapter output = (TableAdapter) args[0];

    // name of the column in the output table for the remote values
    final byte[] remoteColumnName = (byte[]) args[1];

    // constraint on the remote table
    Index rightIndex = (Index) args[2];

    CColumn remoteColumn = new CColumn(rightIndex.getColumnFamily(),
        rightIndex.getColumnQualifier());

    final LocalTableAdapter localTable = this.getLocalTableAdapter();
    // scan through this table and see which values match
    Iterator<Result> rows = localTable.get(new Get(CRange.FULL_TABLE_RANGE));

    while (rows.hasNext()) {
      Result row = rows.next();
      Constraint retrieveColumns = new RetrieveColumns(
          new IndexRangeConstraint(rightIndex, new CRange(row.getRecordId(),
              row.getRecordId())), rightIndex.getPrimaryTable(), remoteColumn);
      SeekingCurrentIterator results = retrieveColumns.getResultIterator();
      // write each keyvalue to the output table. This ensures we don't buffer
      // more in memory than absolutely necessary
      while (results.hasNext()) {
        Result r = results.next();
        CKeyValue remoteRow = new CKeyValue(row.getRecordId(),
            remoteColumnName, r.getRecordId());
        output.put(new Put(remoteRow));
      }
    }
    return null;
  }
}

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
package com.bah.culvert.constraints.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.LexicographicByteArrayComparator;

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
public class NaiveJoinOp extends RemoteOp<Void> {

  /*
   * This is now running over a table with the form: rowID = value to join on CF
   * = filteredTableName.rowID CQ = the row ID.
   */
  @Override
  public Void call(Object... args) throws Exception {

    // create the table to write the output results
    final TableAdapter output = (TableAdapter) args[0];

    // name of the column in the output table for the remote values
    final byte[] rightOutputColumnName = (byte[]) args[1];

    // constraint on the remote table
    Constraint rightConstraint = (Constraint) args[2];

    // the columns to get from the remote results/table
    final CColumn rightColumns = (CColumn) args[3];

    // get iterator over the results, after selecting the desired columns
    Iterator<Result> remoteResults = rightConstraint.getResultIterator();

    final LocalTableAdapter localTable = this.getLocalTableAdapter();

    // if there are no values in the remote iterator, we can stop scanning the
    // local table
    while (remoteResults.hasNext()) {
      // get the remote value
      Result remoteResult = remoteResults.next();
      for (CKeyValue remoteValue : this.filterConstraint(remoteResult,
          rightColumns)) {

        // now check to see if we stored this value as a key
        Iterator<Result> localResults = localTable.get(new Get(new CRange(
            remoteValue.getValue())));
        // if there is a result (only checking the first since we are guaranteed
        // a single CF, CQ at this point
        if (localResults.hasNext()) {
          // create the returned results
          Result localRow = localResults.next();
            CKeyValue remoteRow = new CKeyValue(localRow.getRecordId(),
                rightOutputColumnName, remoteValue.getRowId());
            output.put(new Put(remoteRow));
        }
      }
    }

    return null;
  }

  /**
   * Filter the output of the constraint for the specified column and only
   * return results that have the specified value.
   * @param constraint
   * @param columns to match against
   * @return a filtered list of key-values
   */
  private List<CKeyValue> filterConstraint(Result toFilter, CColumn columns) {
    // if there are no results, just return an empty list
    if (!toFilter.getKeyValues().iterator().hasNext())
      return Collections.EMPTY_LIST;

    List<CKeyValue> rows = new ArrayList<CKeyValue>();
    LexicographicByteArrayComparator comparator = LexicographicByteArrayComparator.INSTANCE;

    for(CKeyValue kv: toFilter.getKeyValues())
    {
      //if we are accepting all CFs
      if(comparator.compare(CColumn.ALL_COLUMNS.getColumnFamily(), columns.getColumnFamily()) == 0)
        rows.add(kv);
        
        //since we aren't accepting all CFs, check the stored against the sent
      if( comparator.compare(columns.getColumnFamily(), kv.getFamily())==0)
      {
        //if we are accepting all CQs
        if(comparator.compare(CColumn.ALL_COLUMNS.getColumnQualifier(), columns.getColumnQualifier())==0)
          rows.add(kv);
        
        //since we aren't accepting all CQs, check the stored against the sent
        if( comparator.compare(columns.getColumnQualifier(), kv.getQualifier())==0)
          rows.add(kv);
      }
    }

    return rows;
  }
}

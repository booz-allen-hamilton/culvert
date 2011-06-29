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
package com.bah.culvert.constraints.filter;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * Just return the row ID for the specified rows in the table.
 * <p>
 * {@link Result} returned from {@link #getResultIterator()} only contain the
 * row id - the {@link CKeyValue} stored is/are empty.
 * <p>
 * Currently just uses the rows returned from
 * {@link ResultFilter#getResultIterator()} , and strips off the columns.
 * However, this can be drastically improved to utilizing the server-side
 * filtering mechanisms present in many NoSQL databases.
 */
public class KeyOnlyFilter extends ResultFilter {

  /**
   * For use with {@link #readFields(java.io.DataInput)}
   */
  public KeyOnlyFilter() {

  }

  /**
   * Select just the row ids from the table after applying the subconstraint
   * where the columns match the specified columns, within the specified range.
   * @param subconstraint to apply first
   * @param range to check
   * @param column to compare
   */
  public KeyOnlyFilter(Constraint subconstraint, CRange range, CColumn... column) {
    super(subconstraint, range, column);
  }

  /**
   * Get the row IDs of rows within the current range with the matching columns
   * @param primaryTable to read from
   * @param range
   * @param columns
   */
  public KeyOnlyFilter(TableAdapter primaryTable, CRange range,
      CColumn... columns) {
    super(primaryTable, range, columns);
  }

  /**
   * Get the row IDs within the specified range, after applying the
   * {@link Constraint}
   * @param subConstraint
   * @param range
   */
  public KeyOnlyFilter(Constraint subConstraint, CRange range) {
    super(range, subConstraint);
  }

  /**
   * Get the row IDs for rows with matching columns after applying the
   * {@link Constraint}
   * @param subConstraint
   * @param columns
   */
  public KeyOnlyFilter(Constraint subConstraint, CColumn... columns) {
    super(subConstraint, columns);
  }

  /**
   * Get the row IDs of fields with columns matching the given columns
   * @param primaryTable to read from
   * @param columns to compare
   */
  public KeyOnlyFilter(TableAdapter primaryTable, CColumn... columns) {
    super(primaryTable, columns);
  }

  /**
   * Get the row IDs of row within the given range
   * @param primaryTable to read from
   * @param range of row ids
   */
  public KeyOnlyFilter(TableAdapter primaryTable, CRange range) {
    super(primaryTable, range);
  }

  /**
   * Get all the row IDs for the whole table
   * @param primaryTable to read from
   */
  public KeyOnlyFilter(TableAdapter primaryTable) {
    this(primaryTable, CRange.FULL_TABLE_RANGE);
  }

  @Override
  public SeekingCurrentIterator getResultIterator() {
    SeekingCurrentIterator iter = super.getResultIterator();
    return new RowIdIterator(iter);
  }

  private class RowIdIterator extends DecoratingCurrentIterator {

    public RowIdIterator(SeekingCurrentIterator iter) {
      super(iter);
    }

    @Override
    public Result next() {
      // just return a result with the row ID in the result. The value is empty.
      Result result = super.next();
      return new Result(result.getRecordId());
    }

  }

}


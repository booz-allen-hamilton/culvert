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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.util.Bytes;

/**
 * Filter results from the subconstraint or direct table lookup.
 * <p>
 * Picks out exclusively the wanted columns from the result set. Unless no
 * columns are specified, columns not selected for are explicitly removed.
 * <p>
 * Currently does not provide implementation for filtering timestamps, but this
 * can be implemented in a subclass later.
 */
public class ResultFilter extends Constraint {

  private static final ObjectWritable ow = new ObjectWritable();
  static {
    Configuration conf = new Configuration();
    ow.setConf(conf);
  }
  private TableAdapter table;
  private Constraint subConstraint;
  private CColumn[] columns;
  private CRange range;

  /**
   * for use with {@link #readFields(DataInput)}
   */
  public ResultFilter() {

  }

  /**
   * Select the specified columns from the range of specified keys, according to
   * a specified sub-constraint
   * @param subConstraint to apply when getting results
   * @param range of keys to select from
   * @param columns to select.
   */
  public ResultFilter(Constraint subConstraint,
      CRange range, CColumn... columns) {
    this.range = range;
    this.columns = columns;
    this.subConstraint = subConstraint;
    this.table = null;
  }

  /**
   * Select the columns from rows in the matching range
   * @param primaryTable To read results from
   * @param range of rows to check
   * @param columns to return
   */
  public ResultFilter(TableAdapter primaryTable, CRange range,
      CColumn... columns) {
    this((Constraint) null, range, columns);
    this.table = primaryTable;
  }

  /**
   * Select all columns from the range of specified keys, according to a
   * specified sub-constraint.
   * @param range of keys to select from
   * @param subConstraint to apply when getting results
   */
  public ResultFilter(CRange range, Constraint subConstraint) {
    this(subConstraint, range, new CColumn[0]);
  }

  /**
   * Select all columns from the range of specified keys from the specified
   * table
   * @param primaryTable to read from
   * @param range of keys to select from
   */
  public ResultFilter(TableAdapter primaryTable, CRange range) {
    this(primaryTable, range, new CColumn[0]);
  }

  /**
   * Select the specified columns from the entire range of keys, according to a
   * specified sub-constraint. Gets the specified columns from the entire table,
   * after applying the subconstraint.
   * @param subConstraint to apply when getting results
   * @param columns to select.
   */
  public ResultFilter(Constraint subConstraint, CColumn... columns) {
    this(subConstraint, CRange.FULL_TABLE_RANGE, columns);
  }

  /**
   * Select the specified columns from the entire range of keys of the specified
   * table
   * @param primaryTable to filter when getting results
   * @param columns to select.
   */
  public ResultFilter(TableAdapter primaryTable, CColumn... columns) {
    this(primaryTable, CRange.FULL_TABLE_RANGE, columns);
  }

  /**
   * Select all columns and rows from the specified table
   * @param primaryTable to select from
   */
  public ResultFilter(TableAdapter primaryTable) {
    this(primaryTable, CRange.FULL_TABLE_RANGE);
  }

  @Override
  public boolean equals(Object compareto) {
    if (!(compareto instanceof ResultFilter)) {
      return false;
    } // has to be a logic
    ResultFilter compare = (ResultFilter) compareto;
    if (this.subConstraint == null && compare.subConstraint != null) {
      return false;
    }
    if (this.subConstraint != null && compare.subConstraint == null) {
      return false;
    }
    if (this.subConstraint != null) {
      if (this.subConstraint.equals(compare.subConstraint)
          && Arrays.equals(this.columns, compare.columns)
          && this.range.compareTo(compare.range) == 0) {
        return true;
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return this.subConstraint.hashCode() + this.columns.hashCode();
  }

  @Override
  public String toString() {
    String s = "SelectFields(" + this.columns.toString() + ", " + this.subConstraint.toString() + ")";
    return s;
  }

  @Override
  public SeekingCurrentIterator getResultIterator() {
    SeekingCurrentIterator results;
    //if there is no subconstraint, just do the get on the table
    if (this.subConstraint == null) {
      Get get = new Get(this.range);
      for (CColumn column : this.columns)
        get.addColumn(column.getColumnFamily(), column.getColumnQualifier());
      results = table.get(get);
    }
    else
      results = this.subConstraint.getResultIterator();

    // check to see if we need to filter the columns
    if (this.columns == null || this.columns.length == 0)
      return results;

    // if we do, return a filtered version
    return new FilteringIterator(this.columns, results);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean())
      this.subConstraint = Constraint.readFromStream(in);

    // read in the columns
    ow.readFields(in);
    this.columns = (CColumn[]) ow.get();

    // read in the range
    this.range = new CRange();
    this.range.readFields(in);

    // read in the table
    if (in.readBoolean()) {

      ow.readFields(in);
      this.table = (TableAdapter) ow.get();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    if (this.subConstraint == null)
      out.writeBoolean(false);
    else{
      out.writeBoolean(true);
      // write out the subconstraint
      Constraint.write(this.subConstraint, out);
    } 

    // write out the columns
    ow.set(this.columns);
    ow.write(out);

    // write out the range
    this.range.write(out);

    if (this.table == null)
      out.writeBoolean(false);
    else {
      out.writeBoolean(true);
      // write out the table
      new ObjectWritable(this.table).write(out);
    }
  }

  /**
   * Use another {@link SeekingCurrentIterator} and filter the results so that
   * only results are returned with matching columns. If there are less columns
   * than specified in the query, only the matching columns are returned.
   * 
   */
  private static class FilteringIterator implements SeekingCurrentIterator {

    private final CColumn[] columns;
    private final SeekingCurrentIterator delegate;
    private Result current = null;
    private Result next = null;

    public FilteringIterator(CColumn[] columns, SeekingCurrentIterator results) {
      this.columns = columns;
      this.delegate = results;
      this.next = getNext();
    }

    @Override
    public boolean hasNext() {
      if (this.next == null)
        markDoneWith();
      return this.next != null;
    }

    @Override
    public Result next() {
      // get the next result
      this.current = this.next;
      this.next = getNext();
      return this.current;
    }

    /**
     * @return the next valid result
     */
    private Result getNext() {

      // while there are more delegate results
      while (this.delegate.hasNext()) {
        Result r = this.delegate.next();
        List<CKeyValue> values = filterKeys(r.getKeyValues());
        
        if (values.size() == 0)
          continue;
        return new Result(values);
      }
      return null;
    }

    private List<CKeyValue> filterKeys(Iterable<CKeyValue> keyValues) {
      List<CKeyValue> values = new ArrayList<CKeyValue>();
      for (CKeyValue value : keyValues) {
        // if there are no checked columns, then add the value
        if (this.columns == null || this.columns.length == 0) {
          values.add(value);
          continue;
        }
        // now we need to check each of the columns to see if we include the
        // value
        for (CColumn column : this.columns) {
          // check the column family
          // if empty column, add the value
          if (column.getColumnFamily() == null
              || column.getColumnFamily().length == 0)
            values.add(value);
          // now check that they actually match for the column
          else if (Bytes.compareTo(column.getColumnFamily(), value.getFamily()) == 0) {
            // check the column qualifier, for blanket acceptance or matching
            if (column.getColumnQualifier() == null
                || column.getColumnQualifier().length == 0
                || Bytes.compareTo(column.getColumnQualifier(),
                    value.getQualifier()) == 0)
              // TODO add timestamp filtering
              // assumes that the underlying table only returns the most recent
              // value
              values.add(value);
          }
        }
      }
      return values;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove results from a query iterator.");
    }

    @Override
    public Result current() {
      return this.current;
    }

    @Override
    public void seek(byte[] key) {
      this.delegate.seek(key);
      Result r = this.delegate.current();

      // set the current
      if (r == null)
        this.current = null;
      else {
        List<CKeyValue> kvs = filterKeys(r.getKeyValues());
        this.current = kvs.size() == 0 ? null : new Result(kvs);
      }
      // set the next
      this.next = this.delegate.hasNext() ? getNext() : null;
    }

    @Override
    public void markDoneWith() {
      this.delegate.markDoneWith();
    }

    @Override
    public boolean isMarkedDoneWith() {
      return this.delegate.isMarkedDoneWith();
    }

  }

}

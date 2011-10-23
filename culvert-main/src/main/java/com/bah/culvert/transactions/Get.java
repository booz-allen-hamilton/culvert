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
package com.bah.culvert.transactions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CRange;
import com.bah.culvert.util.Constants;

/**
 * Get specifies how to retrieve rows from a table. TableAdapters inspect the
 * state of a Get in order to determine what rows, column families, and column
 * qualifiers to fetch from a table.
 * <p>
 * Many databases refer to this access pattern as a scan. We don't
 * differentiate, and suggest that if an underlying database can implement a
 * single-row get operation in a more efficient manner, then it should inspect
 * the gets it receives and optimize based on the get properties.
 * <ul>
 * <li>If column families and column qualifiers are empty, then all columns will
 * be fetched</li>
 * <li>If column families are non-empty and column qualifiers are empty, then
 * all column qualifiers in a family are returned</li>
 * <li>If columnFamilies is empty and column qualifiers are not, then no results
 * are returned</li>
 * <li>Only the most recent timestamped result is returned. Future iterations of
 * this class are expected to add variable timestamping.
 * </ul>
 */
public class Get {

  private final CRange rowRange;
  private final List<CColumn> columns = new ArrayList<CColumn>();

  // TODO add timestamps

  /**
   * Create a Get that will get all the rows in a table within a CRange.
   * 
   * @param ranges
   *          to get
   */
  public Get(CRange range) {
    if (range == null) {
      throw new NullPointerException("Range cannot be null");
    }
    this.rowRange = range;
  }

  /**
   * Create a Get that will get all the rows in a table within a CRange inside
   * of a column family.
   * 
   * @param ranges
   *          to get
   */
  public Get(CRange range, byte[] columnFamily) {
    if (range == null) {
      throw new NullPointerException("Range cannot be null");
    }
    this.rowRange = range;
    this.addColumn(columnFamily);
  }

  /**
   * Create a Get that will get all the rows in a table within a CRange.
   * 
   * @param ranges
   *          to get
   */
  public Get(CRange range, byte[] columnFamily, byte[] columnQualifier) {
    if (range == null) {
      throw new NullPointerException("Range cannot be null");
    }
    this.rowRange = range;
    this.addColumn(columnFamily, columnQualifier);
  }

  /**
   * Get all the column qualifiers under the specified column family
   * @param columnFamily
   */
  public void addColumn(byte[] columnFamily) {
    this.addColumn(columnFamily, Constants.EMPTY_COLUMN_QUALIFIER);
  }

  public void addColumn(byte[] columnFamily, byte[] columnQualifier) {
    this.columns.add(new CColumn(columnFamily, columnQualifier));
  }

  public void addColumn(CColumn column) {
    this.columns.add(column);
  }

  /**
   * @return the ranges to get
   */
  @Deprecated
  public List<CRange> getRanges() {
    return Arrays.asList(this.rowRange);
  }

  /**
   * Get the range of rows returned by this get.
   * 
   * @return The row range returned by this get.
   */
  public CRange getRange() {
    return this.rowRange;
  }

  /**
   * Get the column qualifiers returned by this get. If Column Families and
   * Column Qualifiers are empty, then all columns will be fetched. If Column
   * Families are non-empty and Column Qualifiers is empty, then all column
   * qualifiers in a family are returned. If Column Families are empty and
   * Column Qualifiers is not, then no results are returned.
   * <p>
   * Note that the timestamp on the stored column to retrieve should not be
   * checked for a timestamp. The current implementation does not provide for
   * timestamp checking, but instead returns only the most recent value.
   * 
   * @return the stored columns
   */
  public List<CColumn> getColumns()
  {
    return this.columns;
  }
}

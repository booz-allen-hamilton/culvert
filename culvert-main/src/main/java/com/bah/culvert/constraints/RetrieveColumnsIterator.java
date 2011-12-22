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

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;

class RetrieveColumnsIterator implements SeekingCurrentIterator {

  private final SeekingCurrentIterator delegate;
  private final TableAdapter table;
  private final CColumn[] columns;
  private Result current;

  public RetrieveColumnsIterator(SeekingCurrentIterator results,
      TableAdapter primary, CColumn[] columns) {
    this.delegate = results;
    this.table = primary;
    this.columns = columns;
  }

  @Override
  public boolean hasNext() {
    return this.delegate.hasNext();
  }

  @Override
  public Result next() {
    this.current = this.retrieveRow(this.delegate.next());
    return this.current == null ? this.next() : this.current;
  }

  @Override
  public void remove() {
    this.delegate.remove();
  }

  @Override
  public Result current() {
    return this.current;
  }

  @Override
  public void seek(byte[] key) {
    this.delegate.seek(key);
    // if delegate has seeked to a valid position, we need to update the
    // current iterator
    if (this.delegate.current() != null) {
      this.current = this.retrieveRow(this.delegate.current());
    }

  }

  private Result retrieveRow(Result current) {
    Get get = new Get(new CRange(current.getRecordId()));
    for (CColumn column : this.columns)
      get.addColumn(column.getColumnFamily(), column.getColumnQualifier());

    // get the row from the table
    SeekingCurrentIterator result = this.table.get(get);

    // if there is a result, return it
    if (result.hasNext())
      return result.next();

    // if not, return null
    return null;

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
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
package com.bah.culvert.constraints.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.NullResult;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * Apply a filter to a result set and return values only if that value is
 * present
 */
public class FilteredConstraint extends Constraint {

  private Constraint subconstraint;
  private Filter filter;

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public FilteredConstraint() {

  }

  /**
   * Create a filter on a subconstraint
   * @param subconstraint to filter
   * @param filter to apply
   */
  public FilteredConstraint(Constraint subconstraint, Filter filter) {
    this.subconstraint = subconstraint;
    this.filter = filter;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.subconstraint = Constraint.readFromStream(in);
    this.filter = new Filter();
    this.filter.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Constraint.write(this.subconstraint, out);
    this.filter.write(out);

  }

  @Override
  public SeekingCurrentIterator getResultIterator() {
    return new FilteringIterator(this.subconstraint.getResultIterator(),
        this.filter);
  }

  private static class FilteringIterator implements SeekingCurrentIterator {

    private final SeekingCurrentIterator delegate;
    private Result current;
    private Result next;
    private final Filter filter;

    public FilteringIterator(SeekingCurrentIterator resultIterator,
        Filter filter) {
      this.filter = filter;
      this.delegate = resultIterator;
      this.next = getNext();
    }

    @Override
    public boolean hasNext() {
      return this.next != null;
    }

    @Override
    public Result next() {
      this.current = this.next;
      this.next = getNext();
      return this.current;
    }

    /**
     * Get the next element from the iterator and update the
     */
    private Result getNext() {

      // if there isn't a next value, return an empty
      if (!this.delegate.hasNext())
        return null;

      // get the next value
      Result result = this.delegate.next();

      result = this.filter.apply(result);
      if (result.equals(NullResult.INSTANCE))
        result = getNext();
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Cannot remove from a FilteredConstraint");

    }

    @Override
    public Result current() {
      return this.current;
    }

    @Override
    public void seek(byte[] key) {
      this.delegate.seek(key);

      this.current = this.delegate.current() == null ? null : this.filter
          .apply(this.delegate.current());
      if (this.current.equals(NullResult.INSTANCE))
        this.current = null;
      this.next = getNext();
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

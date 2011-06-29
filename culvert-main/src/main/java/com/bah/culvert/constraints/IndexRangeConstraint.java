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
package com.bah.culvert.constraints;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.CRange;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * A constraint that only returns results over the specified indexes.
 */
public final class IndexRangeConstraint extends Constraint implements Writable {

  private CRange range = new CRange();
  private Index index;

  /**
   * Create an index range where all the indexes are [minimum, maximum)
   * 
   * @param index
   *          to examine
   * @param range
   *          to iterate over
   */
  public IndexRangeConstraint(Index index, CRange range) {
    this.range = range;
    this.index = index;
  }

  /**
   * Create an empty range constraint.
   */
  public IndexRangeConstraint() {
    // TODO Auto-generated constructor stub
  }

/** 
 * Uses the Index's handleGet method to parse for the row id.
 * @see com.bah.culvert.constraints.Constraint#getResultIterator(com.bah.culvert.adapter.TableAdapter)
 * 
 */
  @Override
  public SeekingCurrentIterator getResultIterator() {
    return index.handleGet(range.getStart(), range.getEnd());
  }

  /**
   * @return the range for this constraint
   */
  public CRange getRange() {
    return range;
  }

  @Override
  public String toString() {
    boolean startNotNull = false;
    StringBuilder sb = new StringBuilder();
    sb.append("IndexRangeConstraint(");

    if (range != null && range.getStart() != null) {
      sb.append(range.getStart().hashCode());
      startNotNull = true;
    }

    if (range != null && range.getEnd() != null) {
      if (startNotNull) {
        sb.append(", ");
      }
      sb.append(range.getEnd().hashCode());
    }
    sb.append(")");
    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    range.readFields(in);
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);
    ow.readFields(in);
    this.index = (Index) ow.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    range.write(out);
    new ObjectWritable(this.index).write(out);
  }

  @Override
  public boolean equals(Object compareto) {
    if (!(compareto instanceof IndexRangeConstraint) || compareto == null) {
      return false;
    }

    IndexRangeConstraint compare = (IndexRangeConstraint) compareto;

    if (range == null && compare.getRange() != null) {
      return false;
    }
    
    if (range == null && compare.getRange() == null) {
        return true;
      }    
    
    if (range != null && compare.getRange() == null) {
        return false;
    }
    
    return range.equals(compare.getRange());
  }

  @Override
  public int hashCode() {
    int i = 0;
    if (range != null) {
      i = range.hashCode();
    }
    return i;
  }
  
  public Index getIndex() {
    return index;
  }

}

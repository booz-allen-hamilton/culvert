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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;

import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * {@link Constraint} that implements a piece of logic.
 */
public abstract class Logic extends Constraint {

  private List<Constraint> subConstraints;
  private static ObjectWritable WRITER = new ObjectWritable();
  static {
    Configuration conf = new Configuration();
    WRITER.setConf(conf);
  }

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public Logic() {

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (Constraint b : subConstraints) {
      sb.append(b.toString()).append(",");
    }
    sb = new StringBuilder(sb.substring(0, sb.length() - 1));
    sb.append("]");
    return sb.toString();
  }

  /**
   * Create a Logic constraint that has the specified subconstraints
   * @param subConstraints2
   *          on <tt>this</tt>
   */
  public Logic(List<Constraint> subConstraints2) {
    this.subConstraints = subConstraints2;
  }

  /**
   * @return the stored sub-constraints
   */
  public List<Constraint> getSubConstraints() {
    return subConstraints;
  }

  @Override
  public boolean equals(Object compareto) {
    if (!(compareto instanceof Logic)) {
      return false;
    } // has to be a logic
    Logic compare = (Logic) compareto;
    if (this.getClass() != compareto.getClass()) {
      return false;
    } // logic type must be the same
    if (compare.subConstraints.size() != subConstraints.size()) {
      return false;
    } // length of dependency the same
    for (int i = 0; i < subConstraints.size(); i++) {
      if (!subConstraints.get(i).equals(compare.subConstraints.get(i))) {
        return false; // false is and dependency is different.
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hashcode = 0;
    for (Constraint a : subConstraints) {
      hashcode += a.hashCode();
    }
    return hashcode;
  }

  /**
   * Return the next result based of of the rules of this type of logic.
   * @param resultIterators
   *          to use when looking for the next result
   * @return the next result based on the logic employed
   */
  protected abstract Result nextResult(SeekingCurrentIterator[] resultIterators);

  @Override
  public SeekingCurrentIterator getResultIterator() {
    final SeekingCurrentIterator[] resultIterators = new SeekingCurrentIterator[subConstraints.size()];
    for (int i = 0; i < subConstraints.size(); i++) {
      resultIterators[i] = subConstraints.get(i).getResultIterator();
      if (resultIterators[i] == null)
        throw new NullPointerException("Iterator returned from constraint: "
            + subConstraints.get(i));
    }
    return new SeekingCurrentIterator() {

      Result next = nextResult(resultIterators);
      Result current = null;
      boolean done = false;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Result next() {
        current = next;
        next = nextResult(resultIterators);
        return current;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "Cannot remove results from a query iterator.");
      }

      @Override
      public Result current() {
        return current;
      }

      @Override
      public void seek(byte[] key) {
        for (SeekingCurrentIterator iterator : resultIterators) {
          iterator.seek(key);
        }
      }

      @Override
      public void markDoneWith() {
        done = true;
      }

      @Override
      public boolean isMarkedDoneWith() {
        return done;
      }

    };

  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.subConstraints = Arrays.asList((Constraint[]) WRITER.get());
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    WRITER.set(subConstraints.toArray());
    WRITER.write(out);
  }
}

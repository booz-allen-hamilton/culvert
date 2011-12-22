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

import java.util.Arrays;
import java.util.List;

import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * Class representing the logical And operation This class creates a logical And
 * operation of X number of Constraints. The set returns a set where all had
 * common UIDs across all the Constraints.
 */
public final class And extends Logic {

  /**
   * For use with {@link #readFields(java.io.DataInput)}
   */
  public And() {

  }

  /**
   * AND the specified constraints
   * @param subConstraints to use
   */
  public And(Constraint... subConstraints) {
    this(Arrays.asList(subConstraints));
  }

  public And(List<Constraint> subConstraints) {
    super(subConstraints);
  }

  @Override
  public String toString() {
    return new StringBuilder().append("And(").append(super.toString())
        .append(")").toString();
  }

  @Override
  public boolean equals(Object a) {
    if (!(a instanceof And)) {
      return false;
    }
    return super.equals(a);
  }

  @Override
  public int hashCode() {
    int code = super.hashCode();
    return code + 1;
  }

  /**
   * Get the next result that represents the intersection of the underlying
   * constraints (based on row id)
   * @param resultIterators over all possible results sets
   * @return the next logical result
   */
  @Override
  protected Result nextResult(SeekingCurrentIterator[] resultIterators) {
    while (resultIterators[0].hasNext()) {
      final Result result = resultIterators[0].next();
      boolean isCommon = true;
      for (int i = 1; i < resultIterators.length; i++) {
        boolean commonKey = progressUntil(result.getRecordId(),
            resultIterators[i]);
        if (!commonKey) {
          isCommon = false;
          break;
        }
      }
      if (!isCommon) {
        continue;
      } else {
        return result;
      }
    }
    return null;
  }

}

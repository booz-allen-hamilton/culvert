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
package com.bah.culvert.constraints.write;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;

/**
 * Apply a filter (transformation, acceptance, etc) to a given key value.
 */
public class Handler implements Writable {

  /**
   * Handle the given result from {@link Constraint#getResultIterator()} to be
   * used for a put into a table
   * 
   * By default this function just returns the same value again.
   * <p>
   * If you want to not accept the given row, return an empty list
   * @param toFilter to be filtered
   * @return List of {@link CKeyValue} that will be written to the output table
   */
  public List<CKeyValue> apply(Result toFilter) {
    return toFilter.getKeyValues();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // noop

  }

  @Override
  public void write(DataOutput out) throws IOException {
    // noop

  }

}

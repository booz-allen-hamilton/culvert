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

import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.NullResult;
import com.bah.culvert.data.Result;

/**
 * Apply a filter (transformation, acceptance, etc) to a given key value.
 * 
 */
public class Filter implements Writable {

  /**
   * Filter the given key-value.
   * <p>
   * It can be assumed that the value to be filtered will be a new object each
   * time (overwrite is ok).
   * <p>
   * By default this function just returns the same value again.
   * <p>
   * If you want to not accept the given keyValue, return a {@link NullResult}.
   * @param toFilter to be filtered
   * @return An new Result representing the Result to be filtered.
   */
  public Result apply(Result toFilter) {
    return toFilter;
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

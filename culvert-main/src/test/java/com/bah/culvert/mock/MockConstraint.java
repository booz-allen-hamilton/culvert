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
package com.bah.culvert.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.iterators.SeekingCurrentIterator;

/**
 * Mock a constraint as a "stop"
 */
public class MockConstraint extends Constraint {

  @Override
  public void readFields(DataInput arg0) throws IOException {
  }

  @Override
  public void write(DataOutput arg0) throws IOException {

  }

  @Override
  public SeekingCurrentIterator getResultIterator() {
    return null;
  }

  @Override
  public boolean equals(Object other) {
    // all mock constraints are equivalent
    if (other instanceof MockConstraint)
      return true;
    return false;
  }

}

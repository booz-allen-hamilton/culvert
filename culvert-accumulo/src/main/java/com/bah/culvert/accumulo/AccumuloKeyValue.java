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
package com.bah.culvert.accumulo;

import com.bah.culvert.accumulo.database.AccumuloTableAdapter;
import com.bah.culvert.data.CKeyValue;

/**
 * A Culvert {@link CKeyValue} that also maintains the visibility of the
 * Key/Value. This key can be used interchangeably with respect to a regular
 * {@link CKeyValue} for other TableAdapters, but visibility is taken into
 * account when using a connection via a {@link AccumuloTableAdapter}
 */
public class AccumuloKeyValue extends CKeyValue {

  private static final String DEFAULT_VISIBILTY = "";
  private final String visibility;

  /**
   * Create a Culvert {@link CKeyValue} that also takes into account visibility
   * of the key.
   * @param row
   * @param family
   * @param qualifier
   * @param vis
   * @param timestamp
   * @param value
   */
  public AccumuloKeyValue(byte[] row, byte[] family, byte[] qualifier,
      String vis, long timestamp, byte[] value) {
    super(row, family, qualifier, timestamp, value);
    this.visibility = vis;
  }

  /**
   * Create the {@link CKeyValue} for accumulo that just uses the empty, "",
   * visibility when accessing the table.
   * @param row
   * @param family
   * @param qualifier
   * @param timestamp
   * @param value
   */
  public AccumuloKeyValue(byte[] row, byte[] family, byte[] qualifier,
      long timestamp, byte[] value) {
    this(row, family, qualifier, DEFAULT_VISIBILTY, timestamp, value);
  }

  public AccumuloKeyValue(byte[] rowId) {
    super(rowId);
    this.visibility = DEFAULT_VISIBILTY;
  }

  public AccumuloKeyValue(byte[] rowId, byte[] family, byte[] qualifier) {
    this(rowId, family, qualifier, DEFAULT_VISIBILTY);
  }

  public AccumuloKeyValue(byte[] rowId, byte[] family, byte[] qualifier,
      String vis) {
    super(rowId, family, qualifier);
    this.visibility = vis;
  }

  public AccumuloKeyValue(byte[] rowId, byte[] family, byte[] qualifier,
      byte[] value) {
    this(rowId, family, qualifier, DEFAULT_VISIBILTY, value);
  }

  public AccumuloKeyValue(byte[] rowId, byte[] family, byte[] qualifier,
      String vis, byte[] value) {
    super(rowId, family, qualifier, value);
    this.visibility = vis;
  }

  public AccumuloKeyValue(CKeyValue other) {
    super(other);
    if (other instanceof AccumuloKeyValue)
      this.visibility = ((AccumuloKeyValue) other).visibility;
    else
      this.visibility = DEFAULT_VISIBILTY;

  }

  public String getVisibility() {
    return this.visibility;
  }
}

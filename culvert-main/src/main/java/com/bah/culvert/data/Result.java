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
package com.bah.culvert.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.management.Query;

import org.apache.hadoop.io.Writable;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.util.Bytes;
import com.google.common.collect.Iterables;


/**
 * Result of one record as record ID (key) and value, from doing a lookup in a
 * table.
 * @see Query
 * @see Constraint
 * @see TableAdapter
 */
public class Result implements Writable {
  protected byte[] recordId;
  protected CKeyValue values[];

  /**
   * No args constructor for serialization
   */
  public Result() {
	  this.recordId = new byte[0];
	  this.values = new CKeyValue[0];
  }

  /**
   * Create a Result with only a row id for the record
   * @param recordID row id of the record
   */
  public Result(byte[] recordID) {
	  if(recordID == null){
		  throw new NullPointerException("recordID is not allowed to be null");
	  }
      this.recordId = recordID;
      this.values = new CKeyValue[0];
  }

  /**
   * Create a result with from a set of key values.
   * <p>
   * {@link CKeyValue}s must all have the same row ID, otherwise, results are
   * undefined.
   * @param rowId of the results returned
   * @param keyValues result returned
   */
  public Result(byte[] rowId, CKeyValue... keyValues) {
	  if(rowId == null){
		  throw new NullPointerException("recordID is not allowed to be null");
	  }
	  if(keyValues == null){
		  throw new NullPointerException("keyValues is not allowed to be null");
	  }
	  
	  this.recordId = rowId;
	  this.values = keyValues;
    // sort the list of incoming values
    List<CKeyValue> values = Arrays.asList(keyValues);
    Collections.sort(values, KeyComparator.INSTANCE);
    this.values = (CKeyValue[]) values.toArray();
  }

  /**
   * Create a result with from a set of key values.
   * <p>
   * {@link CKeyValue}s must all have the same row ID, otherwise, results are
   * undefined.
   * @param keyValues result returned
   */
  public Result(CKeyValue... keyValues) {
    this(keyValues[0].getRowId(), keyValues);
	}
  
  /**
   * Create a result from the provided keyvalues iterable.
   * @param keyValues The keyvalues to put in this result.
   */
  public Result(Iterable<CKeyValue> keyValues){
    this(Iterables.toArray(keyValues, CKeyValue.class));
  }

  /**
   * @return row key of the result
   */
	public byte[] getRecordId() {
		return this.recordId;
	}


  /**
   * @return value of the result record
   */
  public Iterable<CKeyValue> getKeyValues() {
	  if(this.values == null){
		  return null;
	  } else {
		  return Arrays.asList(this.values);
	  }
	}

  /**
   * Find the value associated with the given family and qualifier
   * @param family for the value
   * @param qualifier qualifier for the value
   * @return the full {@link CKeyValue} for that value. If that family and
   *         qualifier are not present, returns <tt>null</tt>
   */
  public CKeyValue getValue(byte[] family, byte[] qualifier) {
    // quick failure case check
    if (this.values.length == 0)
      return null;

    // now check all the values
    List<CKeyValue> values = Arrays.asList(this.values);
    // since we know that they are sorted at this point, just do the lookup as a
    // binary search
    int index = Collections.binarySearch(values, new CKeyValue(this.recordId,
        family, qualifier), KeyComparator.INSTANCE);
    if (index < 0)
      return null;
    return values.get(index);
  }

  /**
   * @param recordId the recordId to set
   */
  public void setRecordId(byte[] recordId) {
    this.recordId = recordId;
  }

  /**
   * @param values the values to set
   */
  public void setValues(CKeyValue... values) {
    this.values = values;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.recordId = Bytes.readByteArray(in);
    this.values = new CKeyValue[in.readInt()];
    for (int i = 0; i < this.values.length; i++) {
      this.values[i] = new CKeyValue();
      this.values[i].readFields(in);
    }

  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.recordId);
    out.writeInt(this.values.length);
    for (CKeyValue kv : this.values)
      kv.write(out);

  }

  /**
   * Does a shallow copy of the specified result
   * @param result to copy
   */
  public void copy(Result result) {
    this.recordId = result.getRecordId();
    this.values = result.values;
  }
}

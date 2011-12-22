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
package com.bah.culvert.transactions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.CKeyValue;
import com.google.common.collect.Iterables;

/**
 * Put a {@link List} of keys and values into a table
 */
public class Put implements Writable {

  /**
   * List of the key/value pairs that are in this put.
   */
  private final List<CKeyValue> keyValueList;

  public Put() {
    this.keyValueList = new ArrayList<CKeyValue>();
  }

  /**
   * Put a list of keys and values
   * 
   * @param keyValueList to put
   */
  public Put(Iterable<CKeyValue> keyValueList) {
    this.keyValueList = new ArrayList<CKeyValue>();
    for (CKeyValue keyValue : keyValueList) {
      this.keyValueList.add(keyValue);
    }
  }

  /**
   * Convenience constructor for putting rows
   * 
   * @param cKeyValues to put
   */
  public Put(CKeyValue... cKeyValues) {
    this.keyValueList = Arrays.asList(cKeyValues);
  }

  /**
   * @return the keys and values to put
   */
  public Iterable<CKeyValue> getKeyValueList() {
    return keyValueList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Put) {
      return Iterables.elementsEqual(((Put) obj).keyValueList,
          this.keyValueList);
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(keyValueList.size());
    for (CKeyValue keyValue : keyValueList) {
      keyValue.write(out);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int hash = "Put".hashCode();
    for (CKeyValue kv : keyValueList) {
      hash *= 127;
      hash += kv.hashCode();
    }
    hash *= 127;
    return hash;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    keyValueList.clear();
    int len = in.readInt();
    for (int i = 0; i < len; i++) {
      CKeyValue kv = new CKeyValue();
      kv.readFields(in);
      keyValueList.add(kv);
    }
  }
}

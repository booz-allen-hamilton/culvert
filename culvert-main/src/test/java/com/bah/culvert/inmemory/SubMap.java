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
package com.bah.culvert.inmemory;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class SubMap<T> implements Comparable<SubMap<T>> {

  private final Bytes name;
  protected Map<Bytes, T> map = Collections
      .synchronizedSortedMap(new TreeMap<Bytes, T>());

  public SubMap(Bytes name) {
    this.name = name;
  }

  public Bytes getName() {
    return name;
  }

  @Override
  public int compareTo(SubMap<T> o) {
    return this.name.compareTo(o.name);
  }

  public T put(Bytes key, T value) {
    return map.put(key, value);

  }

}

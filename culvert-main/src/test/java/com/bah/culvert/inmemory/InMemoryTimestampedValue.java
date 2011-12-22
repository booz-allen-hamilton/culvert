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

import com.bah.culvert.util.LexicographicBytesComparator;

public class InMemoryTimestampedValue implements
    Comparable<InMemoryTimestampedValue> {

  private final long timestamp;
  private final Bytes bytes;

  public InMemoryTimestampedValue(byte[] bytes, long timestamp) {
    this(new Bytes(bytes), timestamp);
  }

  public InMemoryTimestampedValue(Bytes bytes, long timestamp) {
    this.bytes = bytes;
    this.timestamp = timestamp;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the bytes
   */
  public byte[] getBytes() {
    return bytes.getBytes();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof InMemoryTimestampedValue) {
      return this.compareTo((InMemoryTimestampedValue) o) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(timestamp).hashCode() + bytes.hashCode();
  }

  @Override
  public int compareTo(InMemoryTimestampedValue o) {
    int compare = Long.valueOf(timestamp).compareTo(o.timestamp);
    if (compare == 0)
      compare = LexicographicBytesComparator.INSTANCE.compare(
          this.bytes.getBytes(), o.getBytes());
    return 0;
  }

}

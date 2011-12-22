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

import java.util.Arrays;

import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Helper class to wrap a byte []
 */
public class Bytes implements Comparable<Bytes> {

  private byte[] bytes;

  public Bytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public int compareTo(Bytes o) {
    return LexicographicBytesComparator.INSTANCE.compare(bytes, o.bytes);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Bytes) {
      return compareTo((Bytes) obj) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 127 * Arrays.hashCode(this.bytes);
  }

  /**
   * Convenience method for comparing byte[]
   * @param other
   * @return
   */
  public int compareTo(byte[] other) {
    return this.compareTo(new Bytes(other));
  }
}

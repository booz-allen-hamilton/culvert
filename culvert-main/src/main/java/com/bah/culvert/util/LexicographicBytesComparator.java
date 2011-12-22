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
package com.bah.culvert.util;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Simple comparator that implements big-endian lexicographic sorting of byte
 * arrays. This is used as a default comparator for <tt>byte[]</tt> object
 * serializations that have no comparator.
 */
public class LexicographicBytesComparator implements Comparator<ByteBuffer> {

  /** Singleton instance of this class */
  public static final LexicographicBytesComparator INSTANCE = new LexicographicBytesComparator();

  /**
   * This object can be a singleton since it has no fields or static references.
   */
  private LexicographicBytesComparator() {
  }

  public int compare(final byte[] o1, final byte[] o2) {
    return compare(ByteBuffer.wrap(o1), ByteBuffer.wrap(o2));
  }

  @Override
  public int compare(final ByteBuffer o1, final ByteBuffer o2) {
    final int o1limit = o1.limit();
    final int o2limit = o2.limit();
    for (int i = 0; i < o1limit && i < o2limit; i++) {
      final int o1i = (o1.get(i) & 0xff);
      final int o2i = (o2.get(i) & 0xff);
      if (o1i < o2i) {
        return -1;
      } else if (o1i > o2i) {
        return 1;
      }
    }
    if (o1limit < o2limit) {
      return -1;
    } else if (o1limit > o2limit) {
      return 1;
    } else {
      return 0;
    }
  }

}

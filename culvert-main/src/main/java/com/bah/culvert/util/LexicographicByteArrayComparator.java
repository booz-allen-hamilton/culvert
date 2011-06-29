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
package com.bah.culvert.util;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Comparator for byte [].
 * <p>
 * Currently uses the {@link LexicographicBytesComparator} to do comparisons,
 * but clearly this is inefficient as creates 2 extra {@link ByteBuffer}s to do
 * the comparison.
 */
public class LexicographicByteArrayComparator implements Comparator<byte[]> {

  public static LexicographicByteArrayComparator INSTANCE = new LexicographicByteArrayComparator();
  private final LexicographicBytesComparator delegate = LexicographicBytesComparator.INSTANCE;

  private LexicographicByteArrayComparator() {

  }

  // TODO replace this with a real implementation, rather than delegation
  @Override
  public int compare(byte[] o1, byte[] o2) {
    return this.delegate.compare(o1, o2);
  }

}

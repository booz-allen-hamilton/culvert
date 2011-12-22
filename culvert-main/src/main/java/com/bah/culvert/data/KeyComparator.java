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
package com.bah.culvert.data;

import java.util.Comparator;

import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Compares only the "key" portion of a {@link CKeyValue} for equivalence. It
 * does not care if the values are the same, it only checks to see if the
 * rowIds, family and value sort properly.
 */
public class KeyComparator implements Comparator<CKeyValue> {

  /**
   * Get a comparator for {@link CKeyValue}.
   */
  public static final Comparator<? super CKeyValue> INSTANCE = new KeyComparator();

  private KeyComparator() {

  }

  @Override
  public int compare(CKeyValue arg0, CKeyValue arg1) {
    LexicographicBytesComparator comparator = LexicographicBytesComparator.INSTANCE;
    int compare = comparator.compare(arg0.getRowId(), arg1.getRowId());
    if (compare == 0) {
      compare = comparator.compare(arg0.getFamily(), arg1.getFamily());
      if (compare == 0)
        compare = comparator.compare(arg0.getQualifier(), arg1.getQualifier());
    }
    return compare;
  }

}

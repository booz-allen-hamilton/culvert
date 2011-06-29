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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;

import org.junit.Test;

/**
 * Test that the key comparator compares keys properly
 */
public class TestKeyComparator {

  /**
   * Compare two row ids
   * @throws Exception on failure
   */
  @Test
  public void testKeyCompare() throws Exception {
    Comparator<? super CKeyValue> comparator = KeyComparator.INSTANCE;
    CKeyValue one = new CKeyValue(new byte[0]);
    CKeyValue two = new CKeyValue(new byte[1]);
    int compare = comparator.compare(one, two);
    assertTrue(compare < 0);

    one = new CKeyValue(new byte[] { 1 });
    two = new CKeyValue(new byte[] { 1, 2 });
    compare = comparator.compare(one, two);
    assertTrue(compare < 0);

    one = new CKeyValue(new byte[] { 2 });
    two = new CKeyValue(new byte[] { 1, 2 });
    compare = comparator.compare(one, two);
    assertTrue(compare > 0);

    compare = comparator.compare(two, two);
    assertEquals(0, compare);

  }

  /**
   * Compare two row ids and families
   * @throws Exception on failure
   */
  @Test
  public void testFullKeysCompare() throws Exception {
    Comparator<? super CKeyValue> comparator = KeyComparator.INSTANCE;
    CKeyValue one = new CKeyValue(new byte[0], new byte[0], new byte[] { 2 });
    CKeyValue two = new CKeyValue(new byte[1], new byte[1], new byte[] { 1 });
    int compare = comparator.compare(one, two);
    assertTrue(compare < 0);

    one = new CKeyValue(new byte[] { 1 }, new byte[] { 1 }, new byte[] { 1 });
    two = new CKeyValue(new byte[] { 1, 2 }, new byte[] { 1, 2 },
        new byte[] { 1 });
    compare = comparator.compare(one, two);
    assertTrue(compare < 0);

    one = new CKeyValue(new byte[] { 1, 2 }, new byte[] { 2 }, new byte[] { 1 });
    two = new CKeyValue(new byte[] { 1, 2 }, new byte[1], new byte[] { 1 });
    compare = comparator.compare(one, two);
    assertTrue(compare > 0);

    compare = comparator.compare(two, two);
    assertEquals(0, compare);

  }
}

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.transactions.Put;

/**
 * Test that the local table adapter performs as expected
 */
public class InMemoryLocalTableAdapterTest {

  /**
   * Test that properly gets the start and end keys
   */
  @Test
  public void testStartEnd() {
    InMemoryTable table = new InMemoryTable();
    InMemoryLocalTableAdapter local = new InMemoryLocalTableAdapter(table);
    assertEquals(0, local.getStartKey().length);
    assertEquals(0, local.getEndKey().length);

    byte[] foo = "foo".getBytes();
    table.put(new Put(new CKeyValue(foo)));
    assertArrayEquals(foo, local.getStartKey());
    assertArrayEquals(foo, local.getEndKey());

    byte[] bar = "bar".getBytes();
    table.put(new Put(new CKeyValue(bar)));
    assertArrayEquals(bar, local.getStartKey());
    assertArrayEquals(foo, local.getEndKey());

  }

}

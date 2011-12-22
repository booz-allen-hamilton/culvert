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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test logic in the {@link Result}.
 */
public class TestResult {

  /**
   * Test that we find the right values in a result
   * @throws Throwable on failure
   */
  @Test
  public void testGetValues() throws Throwable {

    List<CKeyValue> keys = new ArrayList<CKeyValue>();
    keys.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    keys.add(new CKeyValue(new byte[] { 1 }, new byte[] { 2 },
        new byte[] { 5 }, new byte[] { 4, 1 }));
    keys.add(new CKeyValue(new byte[] { 1 }, new byte[] { 6 },
        new byte[] { 5 }, new byte[] { 4 }));
    keys.add(new CKeyValue(new byte[] { 2 }, new byte[] { 2 },
        new byte[] { 3 }, new byte[] { 4 }));
    Result result = new Result(keys.toArray(new CKeyValue[0]));

    Assert.assertArrayEquals(new byte[] { 4 },
        result.getValue(new byte[] { 2 }, new byte[] { 3 }).getValue());
    Assert.assertArrayEquals(new byte[] { 4, 1 },
        result.getValue(new byte[] { 2 }, new byte[] { 5 }).getValue());
    Assert.assertArrayEquals(new byte[] { 4 },
        result.getValue(new byte[] { 6 }, new byte[] { 5 }).getValue());
    Assert.assertArrayEquals(new byte[] { 4 },
        result.getValue(new byte[] { 2 }, new byte[] { 3 }).getValue());
    Assert.assertEquals(null,
        result.getValue(new byte[] { 2 }, new byte[] { 2 }));
  }
}

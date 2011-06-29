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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.util.Utils;

public class CRangeTest {
	
	@Before
	public void setup(){}

	@Test
	public void testCRangeConstructors(){		
		CRange c2 = new CRange(new byte[]{0});
		CRange c3 = new CRange(new byte[]{0}, new byte[]{5});
		CRange c4 = new CRange(new byte[]{0}, true, new byte[]{5}, false);		
		
		Assert.assertArrayEquals(new byte[]{0}, c2.getStart());
		Assert.assertArrayEquals(new byte[]{5}, c3.getEnd());
		Assert.assertTrue(c3.isStartInclusive());
		Assert.assertFalse(c4.isEndInclusive());
	}
	
	@Test
	public void testCRangeEquals(){
		CRange c1 = new CRange();
		CRange c2 = new CRange(new byte[]{0});
		CRange c3 = new CRange(new byte[]{0}, new byte[]{5});
		CRange c4 = new CRange(new byte[]{0}, true, new byte[]{5}, false);
		
		Assert.assertFalse(c1.equals(null));
		Assert.assertFalse(c1.equals(c2));
		Assert.assertTrue(c2.equals(c2));
		Assert.assertFalse(c2.equals(c3));
		Assert.assertFalse(c3.equals(c4));
	}
		
	@Test
  public void testCRangeWriteRead() throws Exception {
		
		CRange c1 = new CRange(new byte[]{0}, true, new byte[]{5}, false);
    c1 = (CRange) Utils.testReadWrite(c1);
    Assert.assertArrayEquals(new byte[] { 0 }, c1.getStart());
    Assert.assertArrayEquals(new byte[] { 5 }, c1.getEnd());
    Assert.assertTrue(c1.isStartInclusive());
    Assert.assertFalse(c1.isEndInclusive());

	}
	

}

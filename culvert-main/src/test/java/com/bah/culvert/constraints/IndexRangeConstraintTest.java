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
package com.bah.culvert.constraints;


import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import com.bah.culvert.data.CRange;
import com.bah.culvert.mock.MockIndex;
import com.bah.culvert.util.Utils;

public class IndexRangeConstraintTest {

	/**
	 * Simple constructor test
	 * 
	 * Tests a few ways to construct the object to make sure constructor is
	 * robust.
	 */
	@Test
	public void testIndexRangeCreation() {
		CRange range = new CRange();
		new IndexRangeConstraint(null, null);
		new IndexRangeConstraint(null, range);			
	}
	
	/**
	 * Test ToString
	 * 
	 * Tests a few ways to construct the object to make sure constructor is
	 * robust.
	 */
	@Test
	public void testIndexRangeToString() {
		CRange range = new CRange();
		CRange range2 = new CRange(new byte[]{ 0 });
		CRange range3 = new CRange(new byte[]{ 0 }, new byte[]{ 3 });
		Assert.assertEquals("IndexRangeConstraint()", new IndexRangeConstraint(null, null).toString());
		Assert.assertNotSame("IndexRangeConstraint()", new IndexRangeConstraint(null, range).toString());
		Assert.assertNotSame("IndexRangeConstraint()", new IndexRangeConstraint(null, range2).toString());
		Assert.assertNotSame("IndexRangeConstraint()", new IndexRangeConstraint(null, range3).toString());
		Assert.assertNotSame(new IndexRangeConstraint(null, range3).toString(), new IndexRangeConstraint(null, range2).toString());		
	}	

	
	/**
	 * Test Equals
	 * 
	 * Tests a few ways to construct the object to make sure constructor is
	 * robust.
	 */
	@Test
	public void testIndexRangeEquals() {
		CRange range = new CRange();
		CRange range2 = new CRange(new byte[]{ 0 });
		CRange range3 = new CRange(new byte[]{ 0 }, new byte[]{ 3 });
		IndexRangeConstraint i1 = new IndexRangeConstraint(null, null);
		IndexRangeConstraint i2 = new IndexRangeConstraint(null, range);			
		IndexRangeConstraint i3 = new IndexRangeConstraint(null, range2);
		IndexRangeConstraint i4 = new IndexRangeConstraint(null, range3); 
		
		Assert.assertFalse(i1.equals(null));
		Assert.assertTrue(i1.equals(i1));
		Assert.assertFalse(i1.equals(i2));
		Assert.assertFalse(i3.equals(i4));
					
	}		

	
	/**
	 * Test ToString
	 * 
	 * Tests a few ways to construct the object to make sure constructor is
	 * robust.
	 */
	@Test
	public void testIndexRangeHashcode() {
		CRange range = new CRange();
		CRange range2 = new CRange(new byte[]{ 0 });
		CRange range3 = new CRange(new byte[]{ 0 }, new byte[]{ 3 });
		Assert.assertEquals(0, new IndexRangeConstraint(null, null).hashCode());
		Assert.assertNotSame(0, new IndexRangeConstraint(null, range).hashCode());			
		Assert.assertNotSame(0, new IndexRangeConstraint(null, range2).hashCode());
		Assert.assertNotSame(0, new IndexRangeConstraint(null, range3).hashCode());
	}		
	
  @Test
  public void testReadWrite() throws Exception {
    IndexRangeConstraint ct = new IndexRangeConstraint(new MockIndex(),
        new CRange(new byte[] { 2 }));
    ct = (IndexRangeConstraint) Utils.testReadWrite(ct);
    assertEquals(new CRange(new byte[] { 2 }), ct.getRange());
  }
}

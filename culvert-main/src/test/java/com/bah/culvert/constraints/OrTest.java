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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Test;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.mock.MockConstraint;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.Utils;

public class OrTest
{

	/**
	 * Simple constructor test
	 * 
	 * Tests a few ways to construct the object to make sure constructor is robust.
	 */
	@Test
	public void testOr()
	{
		Constraint myconstraint1 = createMock(Constraint.class);
		Constraint myconstraint2 = createMock(Constraint.class);
		Constraint myconstraint3 = createMock(Constraint.class);
		Constraint myconstraint4 = createMock(Constraint.class);

		new Or(myconstraint1);
		new Or(myconstraint1, myconstraint2);
		new Or(myconstraint1, myconstraint2, myconstraint3);
		new Or(myconstraint1, myconstraint2, myconstraint3, myconstraint4);

		new Or(null, myconstraint2);
		new Or(myconstraint1, null);
		new Or(null, myconstraint2, myconstraint3);
		new Or(myconstraint1, null, myconstraint3);
		new Or(myconstraint1, myconstraint2, null);

		new Or(null, myconstraint2, myconstraint3, myconstraint4);
		new Or(myconstraint1, null, myconstraint3, myconstraint4);
		new Or(myconstraint1, myconstraint2, null, myconstraint4);
		new Or(myconstraint1, myconstraint2, myconstraint3, null);
	}

	/**
	 * Test Handing of null iterator from constraint
	 * 
	 * Passage of this test indicates that the system can handle a null iterator from a Constraint which may occur if the result set of a Constraint is blank
	 * TODO: determine proper behavior for constraint for empty sets Currently fails in Logic.java which does not check for nulls
	 */
	@Test
	public void testgetResultIteratorwithnull()
	{
		Constraint myconstraint1 = createMock(Constraint.class); // iterator will be null
		Constraint myconstraint2 = createMock(Constraint.class); // iterator will be null
		TableAdapter table = createMock(TableAdapter.class);

		Or myor = new Or(myconstraint1, myconstraint2);
		boolean exceptionSiteIsMeaningful = false;
		try
		{
			myor.getResultIterator();
		}
		catch (NullPointerException e)
		{
			if (e.getMessage().contains(myconstraint1.toString()))
			{
				exceptionSiteIsMeaningful = true;
			}
		}
		assertTrue("Exception site should be meaninful", exceptionSiteIsMeaningful);

	}

	/**
	 * Test Handing of empty iterator from constraint
	 * 
	 * Passage of this test indicates that the system can handle an empty iterator from a Constraint which may occur if the result set of a Constraint is blank
	 * It seems to be failing because the current() call can return null as in empty set.
	 */
	@Test
	public void testgetResultIteratorwithblank()
	{
		int[] a = {};
		int[] b = {};

		int count = simpleOrCount(a, b);

		if (count != 0)
		{
			fail("Iterator is expected to contain 0 items, got: " + count);
		}
	}

	/**
	 * Simple iteration test
	 * 
	 * tests that iteration over two constraints containing two records each (overlap) will produce two results. fails because of a call to a function in a
	 * variable which is null.
	 */
	@Test
	public void simpleiterationtest() {
		int[] a = { 1, 2 };
		int[] b = { 1, 2 };
		int count = simpleOrCount(a, b);

		if (count != 2)
		{
			fail("Iterator is expected to contain 2 items, got: " + count);
		}
	}

	/**
	 * Test override of ToString on Constraint
	 * 
	 */
	@Test
	public void testToString() {
		Constraint myconstraint1 = createMock(Constraint.class);
		Constraint myconstraint2 = createMock(Constraint.class);

		Or myOr = new Or(myconstraint1, myconstraint2);
		Assert.assertEquals("Or([EasyMock for class com.bah.culvert.constraints.Constraint,EasyMock for class com.bah.culvert.constraints.Constraint])", 
				myOr.toString());
	}
	
	/**
	 * Test override of Hashcode on Constraint
	 * 
	 */
	@Test
	public void testHashcode() {
		Constraint myconstraint1 = createMock(Constraint.class);
		Constraint myconstraint2 = createMock(Constraint.class);

		Or myOr = new Or(myconstraint1, myconstraint2);
		Assert.assertNotSame(0, myOr.hashCode());
	}
	
	/**
	 * Test override of Equals on Constraint
	 * 
	 */
	@Test
	public void testEquals() {
		Constraint myconstraint1 = createMock(Constraint.class);
		Constraint myconstraint2 = createMock(Constraint.class);
		
		Constraint myconstraint3 = createMock(Constraint.class);
		Constraint myconstraint4 = createMock(Constraint.class);		

		Or myOr1 = new Or(myconstraint1, myconstraint2);
		Or myOr2 = new Or(myconstraint4, myconstraint3);
		Assert.assertFalse(myOr1.equals(null));
		Assert.assertFalse(myOr1.equals(myOr2));
		Assert.assertTrue(myOr1.equals(myOr1));
	}		
	
	@Test
	/**
	 * Simple Exclusive iteration test
	 * 
	 * tests that two constraints combined into an Or
	 * return a full list of all the elements as each id is unique
	 * fails because of a call to a function in a variable which is null.
	 */
	public void simpleexclusiveterationtest()
	{
		int[] a = { 1, 2, 3, 4, 5, 6, 7 };
		int[] b = { 8, 9, 10, 11, 12, 13, 14 };
		int count = simpleOrCount(a, b);

		if (count != 14)
		{
			fail("Iterator is expected to contain 14 items, got: " + count);
		}
	}

	@Test
	/**
	 * Complex Exclusion Test
	 * 
	 * This test attempts a few trials where the inclusion and exclusion
	 * of the Or operator is tested in a few ways
	 */
	public void complexExclusionTest()
	{

		int[] a = { 1, 2, 3, 4, 11, 12, 13, 14 };
		int[] b = { 1, 2, 3, 4, 21, 22, 23, 24 };
		int count = simpleOrCount(a, b);

		if (count != 12)
		{
			fail("Iterator is expected to contain 12 items, got: " + count);
		}

		int[] a2 = { 11, 12, 13, 14, 1, 2, 3, 4, 5 };
		int[] b2 = { 1, 2, 3, 4, 5, 21, 22, 23, 24 };
		count = simpleOrCount(a2, b2);
		if (count != 13)
		{
			fail("Iterator is expected to contain 12 items, got: " + count);
		}

		int[] a3 = { 1, 2, 3, 11, 12, 13, 14, 4, 5, 6 };
		int[] b3 = { 5, 21, 1, 22, 2, 23, 24, 4, 3, 1000, 6 };
		count = simpleOrCount(a3, b3);
		if (count != 15)
		{
			fail("Iterator is expected to contain 15 items, got: " + count);
		}
	}

	@Test
	/**
	 * Multi Complex Exclusion Test
	 * 
	 * This test attempts a few trials where the inclusion and exclusion
	 * of the Or operator is tested in a few ways
	 */
	public void complexMultiExclusionTest()
	{

		int[] a = { 1, 2, 3, 4, 11, 12, 13, 14 };
		int[] b = { 1, 2, 3, 4, 21, 22, 23, 24 };
		int[] c = { 1, 2, 3, 1775, 3434, 42323, 5453 };
		int count = simpleOrCount(a, b, c);

		if (count != 16)
		{
			fail("Iterator is expected to contain 16 items, got: " + count);
		}

		int[] a2 = { 11, 12, 13, 14, 1, 2, 3, 4, 5 };
		int[] b2 = { 1, 2, 3, 4, 5, 21, 22, 23, 24 };
		int[] c2 = { 1, 2, 3, 4, 5, 21, 22, 23, 24 };
		int[] d2 = { 1, 2, 3, 4, 5, 21, 22, 23, 24 };
		count = simpleOrCount(a2, b2, c2, d2);
		if (count != 13)
		{
			fail("Iterator is expected to contain 13 items, got: " + count);
		}

		int[] a3 = { 1, 2, 3, 11, 12, 13, 14, 4, 5, 6 };
		int[] b3 = { 5, 21, 1, 22, 2, 23, 24, 4, 3, 1000, 6 };
		int[] c3 = { 2, 23, 24, 4, 3, 5, 21, 1, 22, 1000, 6 };
		int[] d3 = { 5, 22, 2, 23, 21, 1, 24, 4, 3, 1000, 6 };
		int[] e3 = { 5, 21, 4, 3, 1000, 6, 1, 22, 2, 23, 24 };
		count = simpleOrCount(a3, b3, c3, d3, e3);
		if (count != 15)
		{
			fail("Iterator is expected to contain 15 items, got: " + count);
		}
	}

	/**
	 * Simple utility function to make Or checking easier
	 * 
	 * Finds out the number of elements that will be returned with Or from Anding the list of sets given. Each individual set is sorted in this function
	 * additionally.
	 * 
	 * @param int[]... Takes any number of arrays (representing a list) for running the Or operation on it
	 * @return int number of elements resulting from Or operation.
	 */
	public int simpleOrCount(final int[]... a)
	{
		TableAdapter table = createMock(TableAdapter.class);
		CKeyValue keyValue = createMock(CKeyValue.class);

		Constraint[] myconstraints = new Constraint[a.length];

		for (int i = 0; i < a.length; i++)
		{
			myconstraints[i] = createStrictMock(Constraint.class);
			final int index = i;
			EasyMock.expect(myconstraints[i].getResultIterator()).andAnswer(new IAnswer<SeekingCurrentIterator>() {

				@Override
				public SeekingCurrentIterator answer() throws Throwable
				{
					int[] arry = a[index];
					Arrays.sort(arry);
					List<Result> resultsA = new ArrayList<Result>(arry.length);
					for (int rA : arry)	{
						resultsA.add(new Result(new CKeyValue(Bytes.toBytes(rA))));
					}
					
					return new DecoratingCurrentIterator(resultsA.iterator()); // set what this call will return
				}
			});
		}

		replay(table, keyValue);

		for (int i = 0; i < a.length; i++)
		{
			replay(myconstraints[i]);
		}

		Or myor = new Or(myconstraints);
		SeekingCurrentIterator mysci = myor.getResultIterator();
		int count = 0;
		while (mysci.hasNext())
		{
			mysci.next();
			count++;
		}

		verify(table);
		verify(keyValue);

		for (int i = 0; i < a.length; i++)
		{
			verify(myconstraints[i]);
		}

		return count;
	}

  @Test
  public void testReadWrite() throws Exception {
    Or or = new Or(new MockConstraint(), new MockConstraint());
    Utils.testReadWrite(or);
  }
}

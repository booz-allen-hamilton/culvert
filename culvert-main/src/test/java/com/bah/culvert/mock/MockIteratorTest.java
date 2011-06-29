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
package com.bah.culvert.mock;

import static org.junit.Assert.fail;

import org.junit.Test;

public class MockIteratorTest {

	@Test
	public void testMockIteratorInt() {
		new MockIterator(0);
		new MockIterator(1);
		new MockIterator(2);
	}

	@Test
	public void testMockIteratorIntBoolean() {
		new MockIterator(0,true);
		new MockIterator(1,true);
		new MockIterator(2,true);
	}

	@Test
	/**
	 * Simple test of the hasNext function
	 * 
	 * This test checks if it is true many calls to hasNext
	 * sequentially always gives same of true if the iterator is good.
	 * 
	 */
	public void testHasNext() {
		MockIterator mi = new MockIterator(2);
		
		if(!mi.hasNext()) {fail("hasNext Failure 1");}
		if(!mi.hasNext()) {fail("hasNext Failure 2");}
		if(!mi.hasNext()) {fail("hasNext Failure 3");}
		if(!mi.hasNext()) {fail("hasNext Failure 4");}
		
		mi = new MockIterator(2,true);
		
		if(!mi.hasNext()) {fail("hasNext Failure 1");}
		if(!mi.hasNext()) {fail("hasNext Failure 2");}
		if(!mi.hasNext()) {fail("hasNext Failure 3");}
		if(!mi.hasNext()) {fail("hasNext Failure 4");}
	}

	@Test
	/**
	 * Simple test of next function
	 * 
	 * checks that the object returns the correct number of 
	 * objects with the next function, returning null
	 * once no more objects can be found.
	 */
	public void testNext() {
		MockIterator mi = new MockIterator(2);
		
		if(mi.next() == null) {fail("next Failure 1");}
		if(mi.next() == null) {fail("next Failure 2");}
		if(mi.next() != null) {fail("next Failure 3");}
		
		mi = new MockIterator(2,true);
		
		if(mi.next() == null) {fail("next Failure 1");}
		if(mi.next() == null) {fail("next Failure 2");}
		if(mi.next() != null) {fail("next Failure 3");}
	}

	@Test
	public void testCurrent() {
		MockIterator mi = new MockIterator(2);

		mi.next();
		if(mi.current() == null) {fail("next Failure 1");}
		mi.next();
		if(mi.current() == null) {fail("next Failure 2");}
		mi.next();
		if(mi.current() != null) {fail("next Failure 3");}
		
		mi = new MockIterator(2,true);
		
		mi.next();
		if(mi.current() == null) {fail("next Failure 1");}
		mi.next();
		if(mi.current() == null) {fail("next Failure 2");}
		mi.next();
		if(mi.current() != null) {fail("next Failure 3");}
	}

	@Test
	public void testSeek() {
		MockIterator mi = new MockIterator(2,true);
		
		mi.seek(2);
		
		mi.next();
		if(mi.current() == null) {fail("next Failure 1");}
		mi.next();
		if(mi.current() != null) {fail("next Failure 2");}
		
		mi.seek(1);
		
		mi.next();
		if(mi.current() != null) {fail("next Failure 1");}
		
	}

	@Test
	public void testMarkDoneWith() {
		MockIterator mi = new MockIterator(2,true);
		mi.markDoneWith();
	}

	@Test
	public void testIsMarkedDoneWith() {
		MockIterator mi = new MockIterator(2,true);
		if(mi.isMarkedDoneWith()) {fail("Is not already marked done with");}
		mi.markDoneWith();
		if(!mi.isMarkedDoneWith()) {fail("Is already marked done with");}
	}

}

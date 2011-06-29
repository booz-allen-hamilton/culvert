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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

public class BytesTest
{

	/**
	 * Test conversion of bytes to a String 
	 *
	 */
	@Test
	public void testBytesToString()
	{
		String s1 = Bytes.stringFromBytes(new byte[]{41,64,43,54,85});
		Assert.assertEquals(")@+6U", s1);
	}
	
	/**
	 * Test conversion of String to bytes
	 *
	 */
	@Test
	public void testStringToBytes()
	{
		byte [] b = Bytes.toBytes(")@+6U");
		Assert.assertArrayEquals(new byte[]{41,64,43,54,85}, b);
	}
	
	/**
	 * Test conversion of bytes to Int
	 *
	 */
	@Test
	public void testBytesToInt()
	{
		try{
			Bytes.toInt(new byte []{54});
			Assert.fail();
		}catch (IllegalArgumentException iae){
			
		}
		byte bytes [] = {0, 0, 0, 6 };
		Integer i =  Bytes.toInt(bytes);
		Assert.assertEquals((Integer)6, i);
	}
	
	/**
	 * Test conversion of bytes to String
	 *
	 */
	@Test
	public void testBytesToString2()
	{
		
		String s1 = Bytes.toString(new byte[]{41,64,43,54,85});
		String s2 = Bytes.toString(null);
		String s3 = Bytes.toString(new byte[]{});
		Assert.assertEquals(")@+6U", s1);
		Assert.assertNull(s2);
		Assert.assertEquals("", s3);
	}
	
	/**
	 * Test comparison of bytes
	 *
	 */
	@Test
	public void testByteComparison()
	{
		byte [] b1 = new byte[] {41,64,43,54,85};
		byte [] b2 = new byte[] {64,43,54,85};
		byte [] b3 = new byte[] {41,64,43,54,85};
		byte [] b4 = new byte[] {32,64,43,54,85};

		Assert.assertEquals(-1, Bytes.compareTo(b1, b2));
		Assert.assertEquals(0, Bytes.compareTo(b1, b3));
		Assert.assertEquals(1, Bytes.compareTo(b1, b4));
		Assert.assertEquals(0, Bytes.compareTo(b1, b1));
	}
	
	/**
	 * Test equals of bytes
	 *
	 */
	@Test
	public void testByteEquals()
	{
		byte [] b1 = new byte[] {41,64,43,54,85};
		byte [] b2 = new byte[] {64,43,54,85};
		byte [] b3 = new byte[] {41,64,43,54,85};
		byte [] b4 = new byte[] {32,64,43,54,85};

		Assert.assertFalse(Bytes.equals(b1, b2));
		Assert.assertTrue(Bytes.equals(b1, b3));
		Assert.assertFalse(Bytes.equals(b1, b4));
		Assert.assertTrue(Bytes.equals(b1, b1));
	}
	
	/**
	 * Test addition of byte arrays
	 *
	 */
	@Test
	public void testAddBytes()
	{
		byte [] b1 = new byte[] {41,64,43,54,85};
		byte [] b2 = new byte[] {64,43,54,85};

		Assert.assertArrayEquals(new byte[]{41,64,43,54,85,64,43,54,85}, Bytes.add(b1, b2));
	}
	
	/**
	 * Test conversion from Integer to byte array
	 *
	 */
	@Test
	public void testIntToBytes()
	{		
		Assert.assertArrayEquals(new byte[]{0,0,0,6}, Bytes.toBytes(6));
	}							

	/**
	 * Test conversion from Character to byte array
	 *
	 */
	@Test
	public void testCharToBytes()
	{		
		Assert.assertArrayEquals(new byte[]{0,99}, Bytes.toBytes('c'));
	}							
	
	/**
	 * Test conversion from Long to byte array
	 *
	 */
	@Test
	public void testLongToBytes()
	{	
		Assert.assertArrayEquals(new byte[]{0,0,0,0,0,0,0,6}, Bytes.toBytes((long)6));
	}								
	
	/**
	 * Test concat of bytes
	 *
	 */
	@Test
	public void testByteConcat()
	{
		byte [] b1 = new byte[] {41,64,43,54,85};
		byte [] b2 = new byte[] {64,43,54,85};
		byte [] b3 = new byte[] {41,64,43,54,85};
		

		Assert.assertArrayEquals(new byte[]{41,64,43,54,85,64,43,54,85,41,64,43,54,85}, 
							Bytes.catBytes(b1, b2, b3));
	}
	
	/**
	 * Test increment of bytes
	 *
	 */
	@Test
	public void testByteIncrement()
	{
		byte [] b1 = new byte[] {41,64,43,54,85};
		Assert.assertArrayEquals(new byte[]{41,64,43,54,86}, Bytes.increment(b1));
		Assert.assertArrayEquals(new byte[]{41,64,43,54,86}, b1);
	}		
	
  /**
   * Test that lexicographically incrementing
   */
  @Test
	public void testLexIncrement()
	{
    LexicographicByteArrayComparator compare = LexicographicByteArrayComparator.INSTANCE;
	  byte[] bytes = new byte[]{1};
    assertTrue(compare.compare(bytes, Bytes.lexIncrement(bytes)) < 0);
    bytes = new byte[0];
    assertTrue(compare.compare(bytes, Bytes.lexIncrement(bytes)) < 0);
    bytes = new byte[] { 0 };
    assertTrue(compare.compare(bytes, Bytes.lexIncrement(bytes)) < 0);
    bytes = new byte[] { 0, 0 };
    assertTrue(compare.compare(bytes, Bytes.lexIncrement(bytes)) < 0);
	}

  /**
   * Test that correctly compare if two bytes are less than or greater than each
   * other
   */
  @Test
  public void testMinMax()
  {
    byte[] left = new byte[]{1};
    byte[] right = new byte[]{1};
    assertEquals(right, Bytes.min(left, right));
    assertEquals(right, Bytes.max(left, right));
    left = new byte[] { 0 };
    assertEquals(left, Bytes.min(left, right));
    assertEquals(right, Bytes.max(left, right));
    right = new byte[] { 0, 0 };
    assertEquals(left, Bytes.min(left, right));
    assertEquals(right, Bytes.max(left, right));
    assertEquals(right, Bytes.max(right, left));
  }
}

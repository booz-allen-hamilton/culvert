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

import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.util.LexicographicBytesComparator;

public class CulvertKeyValueTest {

	private final LexicographicBytesComparator byteComparator = LexicographicBytesComparator.INSTANCE;
	private static final byte ZERO_BYTES[] = new byte[0];
	private final String row = "row1";
	private final String fam = "fam1";
	private final String qual = "qual1";
	private final long time = Long.MIN_VALUE;
	private final String val = "val";
	
	@Before
	public void setup(){}

	@Test
	public void testCulvertKeyValue_Row(){
		CKeyValue kv = new CKeyValue(row.getBytes());
		assertTrue(byteComparator.compare(kv.getRowId(), row.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getFamily(), ZERO_BYTES) == 0);
		assertTrue(byteComparator.compare(kv.getQualifier(), ZERO_BYTES) == 0);
		assertTrue(kv.getTimestamp() == Long.MAX_VALUE);
		assertTrue(byteComparator.compare(kv.getValue(), ZERO_BYTES) == 0);
	}
	
	@Test
	public void testCulvertKeyValue_RowFamQual() {
		CKeyValue kv = new CKeyValue(row.getBytes(), fam.getBytes(), qual.getBytes());
		assertTrue(byteComparator.compare(kv.getRowId(), row.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getFamily(), fam.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getQualifier(), qual.getBytes()) == 0);
		assertTrue(kv.getTimestamp() == Long.MAX_VALUE);
		assertTrue(byteComparator.compare(kv.getValue(), ZERO_BYTES) == 0);
	}

	@Test
	public void testCulvertKeyValue_RowFamQualVal() {
		CKeyValue kv = new CKeyValue(row.getBytes(), fam.getBytes(), qual.getBytes(), val.getBytes());
		assertTrue(byteComparator.compare(kv.getRowId(), row.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getFamily(), fam.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getQualifier(), qual.getBytes()) == 0);
		assertTrue(kv.getTimestamp() == Long.MAX_VALUE);
		assertTrue(byteComparator.compare(kv.getValue(), val.getBytes()) == 0);
	}

	@Test
	public void testCulvertKeyValue_KeyVal() {
		CKeyValue kv = new CKeyValue(row.getBytes(), fam.getBytes(), qual.getBytes());
		CKeyValue kv2 = new CKeyValue(kv);
		
		assertTrue(kv2.getRowId().equals(kv.getRowId()));
		assertTrue(kv2.getFamily().equals(kv.getFamily()));
		assertTrue(kv2.getQualifier().equals(kv.getQualifier()));
		assertTrue(kv2.getTimestamp() == kv.getTimestamp());
		assertTrue(kv2.getValue().equals(kv.getValue()));
	}

	@Test
	public void testCulvertKeyValue_RowFamQualTimeVal() {
		CKeyValue kv = new CKeyValue(row.getBytes(), fam.getBytes(), qual.getBytes(), time, val.getBytes());
		assertTrue(byteComparator.compare(kv.getRowId(), row.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getFamily(), fam.getBytes()) == 0);
		assertTrue(byteComparator.compare(kv.getQualifier(), qual.getBytes()) == 0);
		assertTrue(kv.getTimestamp() == time);
		assertTrue(byteComparator.compare(kv.getValue(), val.getBytes()) == 0);
	}

	@Test
	public void testCompareTo() {
		CKeyValue kv1 = new CKeyValue("row1".getBytes(), "fam1".getBytes(), "qual1".getBytes(), 0, "val1".getBytes());
		CKeyValue kv2 = new CKeyValue("row1".getBytes(), "fam1".getBytes(), "qual1".getBytes(), 0, "val2".getBytes());
    assertEquals(-1, kv1.compareTo(kv2));
    assertEquals(1, kv2.compareTo(kv1));
		
		CKeyValue kv3 = new CKeyValue("row1".getBytes(), "fam1".getBytes(), "qual1".getBytes(), 10, "val2".getBytes());
		assertEquals(-1, kv1.compareTo(kv3));
		assertEquals(1, kv3.compareTo(kv1));
		
		CKeyValue kv4 = new CKeyValue("row1".getBytes(), "fam1".getBytes(), "qual2".getBytes(), 10, "val2".getBytes());
		assertEquals(-1, kv1.compareTo(kv4));
		assertEquals(1, kv4.compareTo(kv1));
		
		CKeyValue kv5 = new CKeyValue("row1".getBytes(), "fam2".getBytes(), "qual2".getBytes(), 10, "val2".getBytes());
		assertEquals(-1, kv1.compareTo(kv5));
		assertEquals(1, kv5.compareTo(kv1));
		
		CKeyValue kv6 = new CKeyValue("row2".getBytes(), "fam2".getBytes(), "qual2".getBytes(), 10, "val2".getBytes());
		assertEquals(-1, kv1.compareTo(kv6));
		assertEquals(1, kv6.compareTo(kv1));
		
		//Test equals
		CKeyValue kv7 = new CKeyValue("row1".getBytes(), "fam1".getBytes(), "qual1".getBytes(), 0, "val1".getBytes());
		assertEquals(0, kv1.compareTo(kv7));
	}

}

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
package com.bah.culvert.data.index;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.mock.MockIndex;
import com.bah.culvert.util.LexicographicBytesComparator;

@RunWith(JUnit4.class)
public class IndexTest {

	@Test
	public void testIndexPersistence() {
		MockIndex ix = new MockIndex("foo", 20, false, "bar".getBytes(),
				"baz".getBytes(), "p", "i");

		MockIndex ix2 = new MockIndex("a", 39, true, "b".getBytes(),
				"c".getBytes(), "a", "b");
		ix2.setConf(ix.getConf());
		assertEquals("foo", ix2.getName());
		assertEquals(0, LexicographicBytesComparator.INSTANCE.compare(
				"bar".getBytes(), ix2.getColumnFamily()));
		assertEquals(0, LexicographicBytesComparator.INSTANCE.compare(
				"baz".getBytes(), ix2.getColumnQualifier()));
		assertEquals("p", ix2.getPrimaryTableName());
		assertEquals("i", ix2.getIndexTableName());
	}

}

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
package com.bah.culvert;

import static junit.framework.Assert.assertEquals;
import static org.easymock.EasyMock.createMock;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.configuration.CConfiguration;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.mock.MockIndex;
import com.bah.culvert.transactions.Put;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class ClientTest {

	@Test
	public void testGetIndices() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");		
		Configuration conf = CConfiguration.getDefault();
		Client client = new Client(conf);
		client.addIndex(ix1);
		client.addIndex(ix2);
		Assert.assertNotNull(client.getIndices());		
	}

	@Test
	public void testGetIndicesForTable() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");		
		Configuration conf = CConfiguration.getDefault();
		Client client = new Client(conf);
		client.addIndex(ix1);
		client.addIndex(ix2);
		client.getIndicesForTable("c");
		//Assert.assertNotNull(client.getIndices());		
	}
	
	@Test
	public void testGetIndexByName() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");		
		Configuration conf = CConfiguration.getDefault();
		Client client = new Client(conf);
		client.addIndex(ix1);
		client.addIndex(ix2);
		Assert.assertNotNull(client.getIndexByName("foo"));
		Assert.assertNull(client.getIndexByName("coo"));	
	}
	
	@Test
	public void testGetIndicesForColumn() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");		
		Configuration conf = CConfiguration.getDefault();
		Client client = new Client(conf);
		client.addIndex(ix1);
		client.addIndex(ix2);
		Assert.assertNotNull(client.getIndicesForColumn("c", "a".getBytes(), "b".getBytes()));
	}			
	
	@Test
	public void testGetIndexMap() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");		
		Configuration conf = CConfiguration.getDefault();
		Client client = new Client(conf);
		client.addIndex(ix1);
		client.addIndex(ix2);
		client.getIndexMap();
		//Assert.assertNotNull(client.getIndicesForColumn("c", "a".getBytes(), "b".getBytes()));
	}	
	
	@Test
	public void testPut() throws Exception {
		TableAdapter primaryTable = createMock(TableAdapter.class);

		Client c = PowerMock.createPartialMock(Client.class, "getIndices",
				"getDatabaseAdapter");
		CKeyValue keyValue = new CKeyValue("rowid".getBytes(), "f".getBytes(),
				"q".getBytes(), "v".getBytes());
		ArrayList<CKeyValue> keyValueList = new ArrayList<CKeyValue>();
		keyValueList.add(keyValue);

		// used for main test and for setup
		Put put = new Put(keyValueList);
		Put put2 = new Put(keyValue);		
		put.hashCode();
		put2.equals("");		

    // MOCKING
    // mock out the index
		Index mockIndex = EasyMock.createMock(Index.class);
		EasyMock.expect(c.getIndices()).andReturn(new Index[] { mockIndex });
		EasyMock.expect(mockIndex.getColumnFamily()).andReturn("f".getBytes());
		EasyMock.expect(mockIndex.getColumnQualifier()).andReturn(
				"q".getBytes());
		mockIndex.handlePut(EasyMock.anyObject(Put.class));

    // mock out the db
		DatabaseAdapter databaseAdapter = EasyMock.createMock(DatabaseAdapter.class);
    EasyMock.expect(databaseAdapter.verify()).andReturn(true);
		PowerMock.expectPrivate(c, "getDatabaseAdapter").andReturn(databaseAdapter);
		EasyMock.expect(databaseAdapter.getTableAdapter("foo")).andReturn(primaryTable);

		primaryTable.put(put);

		PowerMock.replayAll(mockIndex, primaryTable, databaseAdapter);

		c.put("foo",put);
		// c.createRecord(recordID, sourceID, fieldValues);
		EasyMock.verify(primaryTable);
	}

	@Test
	public void testGetIndicies() throws Exception {
		MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(), "b".getBytes(), "c", "d");
		MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(), "f".getBytes(), "g", "h");
		
		Client c = new Client(new Configuration());
		c.addIndex(ix1);
		c.addIndex(ix2);
		
		Client c2 = new Client(c.getConf());
		Index[] ixs2 = c2.getIndices();
		assertEquals(ix1, ixs2[0]);
		assertEquals(ix2, ixs2[1]);
	}
}

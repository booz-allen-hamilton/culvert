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
package com.bah.culvert;

import static junit.framework.Assert.assertEquals;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.mock.MockDatabaseAdapter;
import com.bah.culvert.mock.MockIndex;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.google.common.collect.Iterators;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class ClientTest {

  @Test
  public void testGetIndices() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");
    Configuration conf = CConfiguration.getDefault();
    Client client = new Client(conf);
    client.addIndex(ix1);
    client.addIndex(ix2);
    Assert.assertNotNull(client.getIndices());
  }

  @Test
  public void testGetIndicesForTable() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");
    Configuration conf = CConfiguration.getDefault();
    Client client = new Client(conf);
    client.addIndex(ix1);
    client.addIndex(ix2);
    client.getIndicesForTable("c");
    // Assert.assertNotNull(client.getIndices());
  }

  @Test
  public void testGetIndexByName() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");
    Configuration conf = CConfiguration.getDefault();
    Client client = new Client(conf);
    client.addIndex(ix1);
    client.addIndex(ix2);
    Assert.assertNotNull(client.getIndexByName("foo"));
    Assert.assertNull(client.getIndexByName("coo"));
  }

  @Test
  public void testGetIndicesForColumn() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");
    Configuration conf = CConfiguration.getDefault();
    Client client = new Client(conf);
    client.addIndex(ix1);
    client.addIndex(ix2);
    Assert.assertNotNull(client.getIndicesForColumn("c", "a".getBytes(),
        "b".getBytes()));
  }

  @Test
  public void testGetIndexMap() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");
    Configuration conf = CConfiguration.getDefault();
    Client client = new Client(conf);
    client.addIndex(ix1);
    client.addIndex(ix2);
    client.getIndexMap();
    // Assert.assertNotNull(client.getIndicesForColumn("c", "a".getBytes(),
    // "b".getBytes()));
  }

  @Test
  public void testPut() throws Exception {

    // mock out the index
    Index mockIndex = EasyMock.createMock(Index.class);
    EasyMock.expect(mockIndex.getColumnFamily()).andReturn("f".getBytes());
    EasyMock.expect(mockIndex.getColumnQualifier()).andReturn("q".getBytes());
    mockIndex.handlePut(EasyMock.anyObject(Put.class));

    // mock out the client
    Configuration clientConf = new Configuration(false);
    Client c = PowerMock.createPartialMock(Client.class,
        new String[] { "getIndices" }, clientConf);
    // make sure we can get the configuration
    // c.setConf(clientConf);
    // return the mock index when asked
    expect(c.getIndices()).andReturn(new Index[] { mockIndex });

    // create the put for the table
    CKeyValue keyValue = new CKeyValue("rowid".getBytes(), "f".getBytes(),
        "q".getBytes(), "v".getBytes());
    ArrayList<CKeyValue> keyValueList = new ArrayList<CKeyValue>();
    keyValueList.add(keyValue);
    Put put = new Put(keyValueList);
    Put put2 = new Put(keyValue);
    put.hashCode();
    put2.equals("");

    // preping the db
    InMemoryDB databaseAdapter = new InMemoryDB();
    //create the db
    String primaryTable = "primary";
    TableAdapter pTable = databaseAdapter.getTableAdapter(primaryTable);

    // run the test
    PowerMock.replayAll(mockIndex, c);
    // then set the database for the primary tables
    c.setDatabase(databaseAdapter);
    // then do the put, expecting to write to the index
    c.put(primaryTable, put);

    // then read all the values the primary table
    Get get = new Get(CRange.FULL_TABLE_RANGE);
    assertEquals(1, Iterators.size(pTable.get(get)));


  }

  /**
   * Test that we don't allow broken databases to connect
   * @throws Exception
   */
  @Test
  public void testBrokenDatabaseConnection() throws Exception {
    DatabaseAdapter db = new MockDatabaseAdapter();
    Client c = new Client();
    try {
      c.setDatabase(db);
      fail("Client should not allow a broken database to be set");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testGetIndicies() throws Exception {
    MockIndex ix1 = new MockIndex("foo", 0, true, "a".getBytes(),
        "b".getBytes(), "c", "d");
    MockIndex ix2 = new MockIndex("bar", 0, true, "e".getBytes(),
        "f".getBytes(), "g", "h");

    Client c = new Client(new Configuration());
    c.addIndex(ix1);
    c.addIndex(ix2);

    Client c2 = new Client(c.getConf());
    Index[] ixs2 = c2.getIndices();
    assertEquals(ix1, ixs2[0]);
    assertEquals(ix2, ixs2[1]);
  }
}

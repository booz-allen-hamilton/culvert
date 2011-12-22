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
package com.bah.culvert.data.index;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;

/**
 * Tests the NaiveIndex implementation by creating a data table and using that
 * as the "index" table.
 */
public class NaiveIndexTest {

  private final String tableName = "dataTable";
  private NaiveIndex naiveIndex;

  // Row IDs
  private final byte[] aaaRowId = "AAA".getBytes();
  private final byte[] bbbRowId = "BBB".getBytes();
  private final byte[] cccRowId = "CCC".getBytes();

  private final ArrayList<byte[]> rowIdList = new ArrayList<byte[]>();

  private final byte[] colFam = "fam".getBytes();
  private final byte[] colQual = "qual".getBytes();
  private final byte[] value = "value".getBytes();

  @Before
  public void setup() {
    Configuration conf = new Configuration();
    DatabaseAdapter inMemDB = new InMemoryDB();

    // Set the database adapter conf
    Index.setDatabaseAdapter(conf, InMemoryDB.class);

    // Create the term based index
    naiveIndex = new NaiveIndex();
    naiveIndex.setConf(conf);
    Index.setIndexTable(tableName, conf);

    // Create some data
    ArrayList<CKeyValue> keyValueList = new ArrayList<CKeyValue>();
    keyValueList.add(new CKeyValue(aaaRowId, colFam, colQual, value));
    keyValueList.add(new CKeyValue(bbbRowId, colFam, colQual, value));
    keyValueList.add(new CKeyValue(cccRowId, colFam, colQual, value));
    Put put = new Put(keyValueList);

    // Add the row ids to the list
    rowIdList.add(aaaRowId);
    rowIdList.add(bbbRowId);
    rowIdList.add(cccRowId);

    // Add the data
    TableAdapter tableAdapter = inMemDB.getTableAdapter(tableName);
    tableAdapter.put(put);
  }

  /**
   * Tests whether the TermBasedIndex is able to retrieve all the data.
   * Implicitly tests putting the data.
   */
  @Test
  public void testGetAll() {
    // Get all the data back
    SeekingCurrentIterator indexIterator = naiveIndex.handleGet(new byte[0],
        new byte[0]);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(rowIdList.contains(result.getRecordId()));
    }

    // Test if we have the expected number of results
    assertTrue(count == 3);
  }

  /**
   * Tests whether the TermBasedIndex is able to retrieve a specific range.
   * Implicitly tests putting the data.
   */
  @Test
  public void testGetRange() {
    SeekingCurrentIterator indexIterator = naiveIndex.handleGet(cccRowId,
        cccRowId);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), cccRowId) == 0);
    }

    // Test if we have the expected number of results
    assertTrue(count == 1);
  }
}

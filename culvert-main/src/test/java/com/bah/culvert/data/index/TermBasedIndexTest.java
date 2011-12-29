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

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;

/**
 * Tests the TermBasedIndex class. Specifically, creating a term list and
 * parsing an index rowId. Implicitly it will test creating the rowId when
 * testing the term list creation.
 * 
 */
public class TermBasedIndexTest {

  private final String indexTableName = "termIndex";

  private final String rowId = "rowId";
  private final byte[] rowIdBytes = rowId.getBytes();

  private final String colFam1 = "colFam_1";
  private final String colQual1 = "colQual_1";
  private final String value1 = "Steve vituperated Rodney";

  private TermBasedIndex termIndex;
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
    DatabaseAdapter inMemDB = new InMemoryDB();

    // Create the term based index
    termIndex = new TermBasedIndex();
    termIndex.setConf(conf);

    // set the database
    termIndex.setDatabaseAdapater(inMemDB);

    // set the rest of the index properties
    Index.setIndexTable(indexTableName, conf);
    Index.setColumnFamily(colFam1, conf);
    Index.setColumnQualifier(colQual1, conf);

    // Create a new Put object with the value
    CKeyValue keyValue = new CKeyValue(rowId.getBytes(), colFam1.getBytes(),
        colQual1.getBytes(), value1.getBytes());
    Put put = new Put(keyValue);

    // Insert the data
    termIndex.handlePut(put);
  }

  /**
   * Tests whether the TermBasedIndex is able to retrieve all the data.
   * Implicitly tests putting the data.
   */
  @Test
  public void testGetAll() {
    // Get all the data back
    SeekingCurrentIterator indexIterator = termIndex.handleGet(new byte[0],
        new byte[0]);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), rowIdBytes) == 0);
    }

    // Test if we have the expected number of results
    assertTrue(count == 3);
  }

  /**
   * Tests setting the splitable configuration.
   */
  @Test
  public void testNotSplitable() {
    // Add a new value that we won't split
    CKeyValue keyValue = new CKeyValue(rowId.getBytes(), colFam1.getBytes(),
        colQual1.getBytes(), "broken glass everyone".getBytes());
    Put put = new Put(keyValue);

    // Set the splitable indicator to false
    TermBasedIndex.setSplitable(false, conf);

    // Insert the data
    termIndex.handlePut(put);

    // Get all the data back
    SeekingCurrentIterator indexIterator = termIndex.handleGet(new byte[0],
        new byte[0]);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), rowIdBytes) == 0);
    }

    // Test if we have the expected number of results
    // 3 for the original values in setup() and 1 for the new value
    assertTrue(count == 4);
  }

  /**
   * Tests setting the lower case configuration.
   */
  @Test
  public void testNotLowerCase() {
    // Add a new value that we won't lower case
    CKeyValue keyValue = new CKeyValue(rowId.getBytes(), colFam1.getBytes(),
        colQual1.getBytes(), "That was the old Fry. He's dead now.".getBytes());
    Put put = new Put(keyValue);

    // Set the splitable indicator to false
    TermBasedIndex.setToLower(false, conf);

    // Insert the data
    termIndex.handlePut(put);

    // Get Fry back
    byte[] fryBytes = "Fry".getBytes();

    // The end byte object should look like: steve\x01
    // This represents the next possible value which is allowable
    // since we pad the value with 6 null bytes.
    byte[] end = new byte[fryBytes.length + 1];
    System.arraycopy(fryBytes, 0, end, 0, fryBytes.length);
    end[end.length - 1] = 0x01;

    SeekingCurrentIterator indexIterator = termIndex.handleGet(fryBytes, end);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), rowIdBytes) == 0);
    }

    // Test if we have the expected number of results
    assertTrue(count == 1);
  }

  /**
   * Tests setting the token regex configuration.
   */
  @Test
  public void testTokenRegex() {
    // Add a new value that we won't split
    CKeyValue keyValue = new CKeyValue(rowId.getBytes(), colFam1.getBytes(),
        colQual1.getBytes(),
        "I'm not sure how one spells splitable. Is it even a word?".getBytes());
    Put put = new Put(keyValue);

    // Set the split regex to splittable.
    TermBasedIndex.setTokenRegex("splitable", conf);

    // Insert the data
    termIndex.handlePut(put);

    // Get Fry back
    byte[] partBytes = "i'm not sure how one spells ".getBytes();

    // The end byte object should look like: steve\x01
    // This represents the next possible value which is allowable
    // since we pad the value with 6 null bytes.
    byte[] end = new byte[partBytes.length + 1];
    System.arraycopy(partBytes, 0, end, 0, partBytes.length);
    end[end.length - 1] = 0x01;

    SeekingCurrentIterator indexIterator = termIndex.handleGet(partBytes, end);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), rowIdBytes) == 0);
    }

    // Test if we have the expected number of results
    assertTrue(count == 1);
  }

  /**
   * Tests whether the TermBasedIndex is able to retrieve a specific range.
   * Implicitly tests putting the data.
   */
  @Test
  public void testGetSteve() {
    // Get steve back
    byte[] steveBytes = "steve".getBytes();

    // The end byte object should look like: steve\x01
    // This represents the next possible value which is allowable
    // since we pad the value with 6 null bytes.
    byte[] end = new byte[steveBytes.length + 1];
    System.arraycopy(steveBytes, 0, end, 0, steveBytes.length);
    end[end.length - 1] = 0x01;

    SeekingCurrentIterator indexIterator = termIndex.handleGet(steveBytes, end);

    // Test the returned data
    int count = 0;
    while (indexIterator.hasNext()) {
      Result result = indexIterator.next();
      count++;

      assertTrue(Bytes.compareTo(result.getRecordId(), rowIdBytes) == 0);
    }

    // Test if we have the expected number of results
    assertTrue(count == 1);
  }

}

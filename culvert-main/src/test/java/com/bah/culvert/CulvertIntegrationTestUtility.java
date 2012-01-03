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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.configuration.CConfiguration;
import com.bah.culvert.constraints.And;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.IndexRangeConstraint;
import com.bah.culvert.constraints.RetrieveColumns;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.data.index.TermBasedIndex;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.google.common.collect.Lists;

/**
 * General utility class for creating integration tests between a database and
 * culvert.
 */
public final class CulvertIntegrationTestUtility {

  // private constructor so this can be created
  private CulvertIntegrationTestUtility() {
  }

  private static final String PRIMARY_TABLE = "primary";
  private static final String INDEX_TABLE = "index";
  private static final String INDEX_NAME = "termIndex";
  private static final byte[] INDEX_COLUMN = "test".getBytes();

  public static Client prepare(DatabaseAdapter database) {
    // first make sure that the database is connected
    assertTrue(database.verify());

    // then create the client
    Client client = new Client(CConfiguration.getDefault());

    // then we are going to create all the tables that we need in the database
    byte[] cf = "bar".getBytes();
    byte[] cq = "baz".getBytes();
    database.create(PRIMARY_TABLE, new byte[0][],
        Arrays.asList(new CColumn(cf, cq)));
    TableAdapter primaryTable = database.getTableAdapter(PRIMARY_TABLE);
    database.create(INDEX_TABLE, new byte[0][], null);
    TableAdapter indexTable = database.getTableAdapter(INDEX_TABLE);

    // create term-based index : index each of the words in the value, where the
    // row key is the word and the row id is stored in the rest of the key
    Index index = new TermBasedIndex(INDEX_NAME, database,
        primaryTable.getTableName(), indexTable.getTableName(), cf, cq);
    // other index definitions could also be loaded from the configuration

    // setup the client with the correct database
    client.setDatabase(database);
    client.addIndex(index);

    return client;
  }

  /**
   * Test doing a basic insertion in the table
   * @param client To use when putting
   * @throws Exception
   */
  public static void testInsertion(Client client) throws Exception {
    List<CKeyValue> valuesToPut = Lists.newArrayList(new CKeyValue("foo"
        .getBytes(), "bar".getBytes(), "baz".getBytes(), "value".getBytes()));
    Put put = new Put(valuesToPut);
    client.put(PRIMARY_TABLE, put);
  }

  /**
   * Query the client for the record that we put in, in
   * {@link #testInsertion(Client)}
   * @param client to use when reading
   * @throws Exception on failure
   */
  public static void testQuery(Client client) throws Exception {
    Index c1Index = client.getIndexByName(INDEX_NAME);
    Constraint c1Constraint = new RetrieveColumns(new IndexRangeConstraint(
        c1Index, new CRange("value".getBytes())), c1Index.getPrimaryTable());
    // check the first constraint
    SeekingCurrentIterator iter = c1Constraint.getResultIterator();
    assertTrue("Didn't get any results", iter.hasNext());
    Result r = iter.next();
    assertArrayEquals("value".getBytes(), r.getKeyValues().iterator().next()
        .getValue());
    assertFalse("Should not have more than 1 row in the table", iter.hasNext());

    // create a second constraint
    Index[] c2Indices = client.getIndicesForColumn(PRIMARY_TABLE,
        "bar".getBytes(), "baz".getBytes());
    Constraint c2Constraint = new RetrieveColumns(new IndexRangeConstraint(
        c2Indices[0], new CRange("value".getBytes(), "value".getBytes())),
        c2Indices[0].getPrimaryTable());
    // check the second constraint
    iter = c2Constraint.getResultIterator();
    assertTrue("Didn't get any results for second constraint", iter.hasNext());
    r = iter.next();
    assertArrayEquals("value".getBytes(), r.getKeyValues().iterator().next()
        .getValue());
    assertFalse("Should not have more than 1 row in the table", iter.hasNext());

    Constraint and = new And(c1Constraint, c2Constraint);
    iter = and.getResultIterator();
    assertTrue("Didn't get any results for AND constraint", iter.hasNext());
    r = iter.next();
    assertArrayEquals("value".getBytes(), r.getKeyValues().iterator().next()
        .getValue());
    assertFalse("Should not have more than 1 row in the table", iter.hasNext());

  }
}

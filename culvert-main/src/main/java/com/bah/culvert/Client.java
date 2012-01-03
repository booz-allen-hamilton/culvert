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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.BaseConfigurable;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.ConfUtils;
import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Main entry point for interacting with the indexed database
 */
public class Client extends BaseConfigurable {

  private static final String INDEXES_CONF_KEY = "culvert.indices.names";
  private DatabaseAdapter db;
  private ReentrantLock dbLock = new ReentrantLock(true);

  /**
   * Create a client with a specific configuration
   * @param conf to base the client on
   */
  public Client(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * Create a client with an empty configuration
   */
  public Client() {

  }

  /**
   * Create a record in the ClientAdapter for the information. Also
   * automatically indexes that {@link Put} for future use
   * @param put
   * @throws RuntimeException If an error occurs.
   */
  public void put(String tableName, Put put) {
    // Get the KeyValue list
    Iterable<CKeyValue> keyValueList = put.getKeyValueList();
    List<CKeyValue> indexValues = new ArrayList<CKeyValue>();
    // for each index, add only the keyvalues that should be indexed
    for (Index index : getIndices()) {
      indexValues.clear();
      for (CKeyValue keyValue : keyValueList) {
        if (Bytes.compareTo(index.getColumnFamily(), keyValue.getFamily()) == 0) {
          if (Bytes.compareTo(index.getColumnQualifier(),
              keyValue.getQualifier()) == 0) {
            indexValues.add(keyValue);
          }
        }
      }
      index.handlePut(new Put(indexValues));
    }

    DatabaseAdapter db = getDatabaseAdapter();
    db.getTableAdapter(tableName).put(put);
  }

  /**
   * Creates a map of the Index keyed by the index name.
   * @return map of [name, index]
   */
  public HashMap<String, Index> getIndexMap() {
    HashMap<String, Index> indexMap = new HashMap<String, Index>();

    for (Index index : getIndices()) {
      indexMap.put(index.getName(), index);
    }

    return indexMap;
  }

  /**
   * Query the ClientAdapter associated with <code>this</code>
   * @param query
   * @return an iterator to the list of results from the query
   */
  public Iterator<Result> query(Constraint query) {
    return query.getResultIterator();
  }

  private static String indexClassConfKey(String indexName) {
    return "culvert.indices.class." + indexName;
  }

  private static String indexConfPrefix(String indexName) {
    return "culvert.indices.conf." + indexName;
  }

  /**
   * Get the indices assigned to this client.
   * @return stored indicies
   */
  public Index[] getIndices() {
    String[] indexNames = this.getConf().getStrings(INDEXES_CONF_KEY);
    int arrayLength = indexNames == null ? 0 : indexNames.length;
    Index[] indices = new Index[arrayLength];
    for (int i = 0; i < arrayLength; i++) {
      String name = indexNames[i];
      Class<?> indexClass = this.getConf().getClass(indexClassConfKey(name),
          null);
      Configuration indexConf = ConfUtils.unpackConfigurationInPrefix(
          indexConfPrefix(name), this.getConf());
      Index index;
      try {
        index = Index.class.cast(indexClass.newInstance());
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      index.setConf(indexConf);
      indices[i] = index;
    }
    return indices;
  }

  /**
   * Get the indices assigned to this client.
   * @return The indices for this table.
   */
  public Index[] getIndicesForTable(String tableName) {
    String[] indexNames = this.getConf().getStrings(INDEXES_CONF_KEY);
    List<Index> indices = new ArrayList<Index>();
    for (int i = 0; i < indexNames.length; i++) {
      String name = indexNames[i];
      Class<?> indexClass = this.getConf().getClass(indexClassConfKey(name),
          null);
      Configuration indexConf = ConfUtils.unpackConfigurationInPrefix(
          indexConfPrefix(name), this.getConf());
      String primaryTableName = Index.getPrimaryTableName(indexConf);
      if (tableName.equals(primaryTableName)) {
        Index index;
        try {
          index = Index.class.cast(indexClass.newInstance());
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        index.setConf(indexConf);
        indices.add(index);
      }
    }
    return indices.toArray(new Index[indices.size()]);
  }

  /**
   * Get an index by name.
   * @param string The name of the index.
   * @return The index with the name, or null if no such index exists for this
   *         client.
   */
  public Index getIndexByName(String string) {
    Index[] indices = getIndices();
    for (Index index : indices) {
      if (index.getName().equals(string)) {
        return index;
      }
    }
    return null;
  }

  /**
   * Return any indices that index this column.
   * @param table primary table that index indexes
   * @param family The column family to search for.
   * @param qualifier The column Qualifier to search for.
   * @return The indices over the column.
   */
  public Index[] getIndicesForColumn(String table, byte[] family,
      byte[] qualifier) {
    Index[] indices = getIndices();
    List<Index> indicesForColumn = new ArrayList<Index>();
    for (Index index : indices) {
      if (table.equals(index.getPrimaryTableName())) {
        if (LexicographicBytesComparator.INSTANCE.compare(family,
            index.getColumnFamily()) == 0) {
          if (LexicographicBytesComparator.INSTANCE.compare(qualifier,
              index.getColumnQualifier()) == 0) {
            indicesForColumn.add(index);
          }
        }
      }
    }
    return indicesForColumn.toArray(new Index[indicesForColumn.size()]);
  }

  /**
   * Add an index to the primary table that this client us being used on.
   * @param index The index to add to this table.
   * @throws RuntimeException If the index name already exists.
   */
  public void addIndex(Index index) {
    String name = index.getName();
    String[] currentIndices = this.getConf().getStrings(INDEXES_CONF_KEY,
        new String[0]);
    for (String existingName : currentIndices) {
      if (existingName.equals(name)) {
        throw new RuntimeException("Index with name " + name
            + " already exists");
      }
    }

    String[] newNames = new String[currentIndices.length + 1];
    System.arraycopy(currentIndices, 0, newNames, 0, currentIndices.length);
    newNames[currentIndices.length] = name;
    ConfUtils.packConfigurationInPrefix(indexConfPrefix(name), index.getConf(),
        this.getConf());
    this.getConf().setStrings(INDEXES_CONF_KEY, newNames);
    this.getConf().set(indexClassConfKey(name), index.getClass().getName());
  }

  /**
   * Set the database the client is currently storing the data in for the
   * primary table.
   * <p>
   * This method updates the configuration, so the same client can be created by
   * just calling {@link #getConf()} on this object and setting the
   * configuration on the new client.
   * <p>
   * The database at this point must be reachable, or the set will be rejected
   * (the client will not store a database that is cannot reach).
   * @param db DatabaseAdapter to connect to the database
   */
  public void setDatabase(DatabaseAdapter db) {
    try {
      dbLock.lock();
      // ensure that the db can be connected to
      if (!db.verify())
        throw new IllegalArgumentException(
            "Database connection cannot be verified.");
      setDatabaseAdapter(this.getConf(), db);
      this.db = db;
    } finally {
      dbLock.unlock();
    }
  }

  /**
   * Set the database the client is currently storing the data in for the
   * primary table.
   * <p>
   * This method updates the configuration, so the same client can be created by
   * just calling {@link #getConf()} on this object and setting the
   * configuration on the new client
   * @param db {@link DatabaseAdapter} to connect to the database
   * @param conf {@link Configuration} to update with the database adapter for
   *        the client
   */
  public static void setDatabaseAdapter(Configuration conf, DatabaseAdapter db) {
    DatabaseAdapter.writeToConfiguration(db.getClass(), db.getConf(), conf);
  }

  /**
   * Set the database the client is currently storing the data in for the
   * primary table.
   * <p>
   * This method updates the configuration, so the same client can be created by
   * just calling {@link #getConf()} on this object and setting the
   * configuration on the new client
   * @param db {@link DatabaseAdapter} to connect to the database
   * @param dbConf {@link Configuration} for the database adapter to store; used
   *        when instantiating and configuring the database adapter on use
   * @param conf {@link Configuration} to update with the database adapter for
   *        the client
   */
  public static void setDatabaseAdpater(Configuration conf,
      Class<? extends DatabaseAdapter> db, Configuration dbConf) {
    DatabaseAdapter.writeToConfiguration(db, dbConf, conf);
  }

  private DatabaseAdapter getDatabaseAdapter() {
    try {
      dbLock.lock();
      // do a lazy initialization of the db
      if (db == null)
        db = DatabaseAdapter.readFromConfiguration(getConf());
      return db;
    } finally {
      dbLock.unlock();
    }
  }

  public boolean tableExists(String tableName) {
    DatabaseAdapter adapter = getDatabaseAdapter();
    return adapter.tableExists(tableName);
  }

  /**
   * Ensure that the client is can connect to the database
   * @return <tt>true</tt> if it can connect, <tt>false</tt> otherwise
   */
  public boolean verify() {
    return getDatabaseAdapter().verify();
  }
}

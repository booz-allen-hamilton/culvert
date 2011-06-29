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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.ConfUtils;
import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Main entry point for interacting with the indexed database
 */
public class Client {

  private static final String DATABASE_ADAPTER_CONF_KEY = "culvert.database.adapter";
  private static final String INDEXES_CONF_KEY = "culvert.indices.names";
  private static final String DATABASE_ADAPTER_CONF_PREFIX = "culvert.database.conf";

  private final Configuration configuration;

  public Client(Configuration conf) {
    this.configuration = conf;
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
    // TODO this is obviously not performant
    getDatabaseAdapter().getTableAdapter(tableName).put(put);
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
   * @return
   */
  public Iterator<Result> query(String tableName, Constraint query) {
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
    String[] indexNames = this.configuration.getStrings(INDEXES_CONF_KEY);
    int arrayLength = indexNames == null ? 0 : indexNames.length;
    Index[] indices = new Index[arrayLength];
    for (int i = 0; i < arrayLength; i++) {
      String name = indexNames[i];
      Class<?> indexClass = configuration.getClass(indexClassConfKey(name),
          null);
      Configuration indexConf = ConfUtils.unpackConfigurationInPrefix(
          indexConfPrefix(name), configuration);
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
    String[] indexNames = this.configuration.getStrings(INDEXES_CONF_KEY);
    List<Index> indices = new ArrayList<Index>();
    for (int i = 0; i < indexNames.length; i++) {
      String name = indexNames[i];
      Class<?> indexClass = configuration.getClass(indexClassConfKey(name),
          null);
      Configuration indexConf = ConfUtils.unpackConfigurationInPrefix(
          indexConfPrefix(name), configuration);
      String primaryTableName = indexConf.get(Index.PRIMARY_TABLE_CONF_KEY);
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
   * @param string
   *          The name of the index.
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Implementation RecordWriter and OutputFormat
   * @param family
   *          The column family to search for.
   * @param qualifier
   *          The column Qualifier to search for.
=======
   * @param table primary table that index indexes
   * @param family The column family to search for.
   * @param qualifier The column Qualifier to search for.
>>>>>>> CulvertHBaseIT implemenation and fixes
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
   * @param index
   *          The index to add to this table.
   * @throws RuntimeException
   *           If the index name already exists.
   */
  public void addIndex(Index index) {
    String name = index.getName();
    String[] currentIndices = configuration.getStrings(INDEXES_CONF_KEY,
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
        configuration);
    configuration.setStrings(INDEXES_CONF_KEY, newNames);
    configuration.set(indexClassConfKey(name), index.getClass().getName());
  }

  /**
   * Get the configuration for this client.
   * @return This client's configuration.
   */
  public Configuration getConf() {
    return configuration;
  }

  public static void setDatabase(DatabaseAdapter db, Configuration conf) {
    conf.set(DATABASE_ADAPTER_CONF_KEY, db.getClass().getName());
    ConfUtils.packConfigurationInPrefix(DATABASE_ADAPTER_CONF_PREFIX,
        db.getConf(), conf);
  }

  private DatabaseAdapter getDatabaseAdapter() {
    try {
      DatabaseAdapter adapter = DatabaseAdapter.class.cast(configuration
          .getClass(DATABASE_ADAPTER_CONF_KEY, null).newInstance());
      Configuration dbConf = ConfUtils.unpackConfigurationInPrefix(
          DATABASE_ADAPTER_CONF_PREFIX, configuration);
      adapter.setConf(dbConf);
      return adapter;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setDatabaseAdapter(Configuration conf,
      Class<? extends DatabaseAdapter> adapterClass) {
    conf.setClass(DATABASE_ADAPTER_CONF_KEY, adapterClass,
        DatabaseAdapter.class);
  }

  public boolean tableExists(String tableName) {
    DatabaseAdapter adapter = getDatabaseAdapter();
    return adapter.tableExists(tableName);
  }

  public boolean verify() {
    return getDatabaseAdapter().verify();
  }
}

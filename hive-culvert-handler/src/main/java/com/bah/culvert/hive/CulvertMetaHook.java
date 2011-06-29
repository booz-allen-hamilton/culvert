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
package com.bah.culvert.hive;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.util.BaseConfigurable;

/**
 * MetaHooks are used to help check that various metastore operations are valid,
 * for instance that the connection to an external table is properly configured.
 */
public class CulvertMetaHook extends BaseConfigurable implements HiveMetaHook {

  /* Property Strings */
  public static final String CULVERT_DATABASE_ADAPTER = "culvert.database.adapter";
  public static final String CULVERT_COLUMNS_MAPPING = "culvert.columns.mapping";
  public static final String CULVERT_TABLE_NAME = "culvert.table.name";
  public static final String CULVERT_INDEX_LIST = "culvert.index.list";

  /**
   * Contains the property strings, which are statically added to the set.
   */
  public static Set<String> requiredTableParams = new HashSet<String>();
  static {
    requiredTableParams.add(CULVERT_DATABASE_ADAPTER);
    requiredTableParams.add(CULVERT_COLUMNS_MAPPING);
    requiredTableParams.add(CULVERT_INDEX_LIST);
    requiredTableParams.add(CULVERT_TABLE_NAME);
  }

  @Override
  public void commitCreateTable(Table arg0) throws MetaException {

  }

  @Override
  public void commitDropTable(Table arg0, boolean arg1) throws MetaException {

  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Check if we have the required table properties
    Map<String, String> params = table.getParameters();
    for (String param : requiredTableParams) {
      if (!params.containsKey(param)) {
        throw new MetaException("Cannot create table. Missing parameter: "
            + param);
      }
    }

    // Discard attempts to create tables managed by Hive. We just want
    // the BigTable implementation to be the owner
    if (!MetaStoreUtils.isExternalTable(table)) {
      throw new MetaException(
          "Hive managed tables are not supported. The table must already exist in the datastore");
    }

    // Get the DatabaseAdapter (through reflection for now)
    String dbAdapterClassName = params.get(CULVERT_DATABASE_ADAPTER);
    Class<DatabaseAdapter> dbAdapterClass;
    DatabaseAdapter databaseAdapter;
    try {
      dbAdapterClass = (Class<DatabaseAdapter>) Class
          .forName(dbAdapterClassName);
      databaseAdapter = dbAdapterClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new MetaException("Unknown DatabaseAdapter: " + dbAdapterClassName);
    } catch (InstantiationException e) {
      throw new MetaException("Unable to instantiate DatabaseAdapter: "
          + dbAdapterClassName);
    } catch (IllegalAccessException e) {
      throw new MetaException("Unable to access DatabaseAdapter constructor: "
          + dbAdapterClassName
          + ". There should be a public no-arg constructor");
    }

    // Verify the table with the DatabaseAdapter
    boolean verified = databaseAdapter.verify();

    if (!verified) {
      throw new MetaException(
          "Unable to connect to the database table. Check the connection "
              + "parameters and if the table exists in the database.");
    }

    // No exceptions thrown means Hive is good to go in creating its
    // representation of the table. Method complete!
  }

  @Override
  public void preDropTable(Table arg0) throws MetaException {
  }

  @Override
  public void rollbackCreateTable(Table arg0) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table arg0) throws MetaException {
  }

}

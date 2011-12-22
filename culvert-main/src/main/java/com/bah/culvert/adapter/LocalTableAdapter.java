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
package com.bah.culvert.adapter;

import java.util.Iterator;

import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.util.BaseConfigurable;

/**
 * Access the local table to perform the specified operations.
 */
public abstract class LocalTableAdapter extends BaseConfigurable {

  public static final String TABLE_NAME_SETTING = "culvert.adapter.tablename";

  public String getTableName() {
    return this.getConf().get(TABLE_NAME_SETTING);
  }

  /**
   * Perform the specified get
   * 
   * @param get on the local table
   * @return an {@link Iterator} over the results of the get
   * @throws TableException on failure to use the table
   */
  public abstract Iterator<Result> get(Get get);

  /**
   * Gets the start key stored on a region
   * @return a byte[] representing the start key
   */
  public abstract byte[] getStartKey();

  /**
   * Gets the end key stored on a region
   * @return a byte[] representing the end key
   */
  public abstract byte[] getEndKey();
}

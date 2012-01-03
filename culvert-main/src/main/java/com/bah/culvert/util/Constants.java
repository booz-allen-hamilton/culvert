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
package com.bah.culvert.util;

/**
 * General constants for interacting with culvert
 */
public final class Constants {

  public static final byte[] EMPTY_COLUMN_QUALIFIER = new byte[0];

  /*
   * Database storage keys
   */
  public static final String DATABASE_ADAPTER_CLASS_KEY = "culvert.database.adapter.class";
  public static final String DATABASE_CONF_PREFIX = "culvert.database.conf";

}

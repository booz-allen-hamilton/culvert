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

/**
 * Simple class that represents culvert columns as hive columns.
 * <p>
 * This class uses three factory methods to present a constrained, fluent api
 * for creating mappings.
 * <ul>
 * <li>{@link #forColumnFamilyAndQualifier(byte[], byte[], String)}</li>
 * <li>{@link #forFamilyAsMap(byte[], String)}</li>
 * <li>{@link #forRowIdAsColumn(String)}</li>
 * </ul>
 */
public class CulvertIndexMapping {

  private final boolean rowId;
  private final boolean familyAsMap;
  private final byte[] columnFamily;
  private final byte[] columnQualifier;
  private final String hiveColumn;
  private final String type;

  /**
   * Create a mapping that represents a whole column family as a map.
   * @param columnFamily
   *          The bigtable column family to represent as a map.
   * @param hiveColumn
   *          The hive column to represent the map in.
   * @param type
   *          the type of the hive column (required).
   * @return A mapping of column family to a hive column as a map.
   */
  public static CulvertIndexMapping forFamilyAsMap(byte[] columnFamily,
      String hiveColumn, String type) {
    if (hiveColumn == null) {
      throw new NullPointerException("Hive column cannot be null");
    }
    if (columnFamily == null) {
      throw new NullPointerException("Column family cannot be null");
    }
    if (type == null) {
      throw new NullPointerException("Hive type cannot be null");
    }
    return new CulvertIndexMapping(false, true, columnFamily, null, hiveColumn,
        type);
  }

  /**
   * Create a mapping that represents the row id as a column.
   * @param hiveColumn
   *          The hive column name to map the row id to.
   * @param type
   *          the type of the hive column (required).
   * @return The mapping of row id to hive column.
   */
  public static CulvertIndexMapping forRowIdAsColumn(String hiveColumn,
      String type) {
    if (hiveColumn == null) {
      throw new NullPointerException("Hive column cannot be null");
    }
    if (type == null) {
      throw new NullPointerException("Hive type cannot be null.");
    }
    return new CulvertIndexMapping(true, false, null, null, hiveColumn, type);
  }

  /**
   * Get a culvert index mapping for a column family and column qualifier to a
   * hive column
   * @param columnFamily
   *          The bigtable column family.
   * @param columnQualifier
   *          The bigtable column qualifer.
   * @param hiveColumn
   *          The hive column to map to.
   * @param type
   *          the type of the hive column (required).
   * @return A mapping from column family and qualifier to a hive column
   */
  public static CulvertIndexMapping forColumnFamilyAndQualifier(
      byte[] columnFamily, byte[] columnQualifier, String hiveColumn,
      String type) {
    if (columnFamily == null) {
      throw new NullPointerException("Column family cannot be null");
    }
    if (columnQualifier == null) {
      throw new NullPointerException("Column qualifier cannot be null");
    }
    if (hiveColumn == null) {
      throw new NullPointerException("Hive column cannot be null");
    }
    if (type == null) {
      throw new NullPointerException("Hive type cannot be null.");
    }
    return new CulvertIndexMapping(false, false, columnFamily, columnQualifier,
        hiveColumn, type);
  }

  /**
   * Private constructor. We use this so that we can enforce logic about what
   * fields can be set in relationship to eachother, as well as create a more
   * fluent programming api with factory static methods.
   * @param rowId
   *          does this mapping represent a rowId
   * @param familyAsMap
   *          does this mapping represent a column family as a map
   * @param columnFamily
   *          the column family to use (optional)
   * @param columnQualifier
   *          the qualifier to use (optional)
   * @param hiveColumn
   *          the hive column to map to (required)
   * @param type
   *          the type of the hive column (required).
   */
  private CulvertIndexMapping(boolean rowId, boolean familyAsMap,
      byte[] columnFamily, byte[] columnQualifier, String hiveColumn,
      String type) {
    this.rowId = rowId;
    this.familyAsMap = familyAsMap;
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.hiveColumn = hiveColumn;
    this.type = type;
  }

  /**
   * Get the hive column.
   * @return The hive column to map to.
   */
  public String getHiveColumn() {
    return hiveColumn;
  }

  /**
   * Get the column family. May return null if this represents a rowID mapping.
   * @return The column family.
   * @see #isRowId()
   */
  public byte[] getColumnFamily() {
    return columnFamily;
  }

  /**
   * Get the column qualifier. May return null if this represents a row id
   * mapping or a column family as a map.
   * @return The column qualifier.
   * @see #isFamilyAsMap()
   * @see #isRowId()
   */
  public byte[] getColumnQualifier() {
    return columnQualifier;
  }

  /**
   * Does this mapping represent a family as a map?
   * @return weather or not this column family represents a map.
   */
  public boolean isFamilyAsMap() {
    return familyAsMap;
  }

  /**
   * Does this mapping represent the row id?
   * @return weather or not this mapping represents the row id
   */
  public boolean isRowId() {
    return rowId;
  }

  /**
   * Return the HIVE type stored in this column.
   * @return The HIVE type.
   */
  public String getType() {
    return type;
  }

}

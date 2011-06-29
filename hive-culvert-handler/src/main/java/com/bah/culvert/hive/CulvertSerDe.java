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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Put;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class CulvertSerDe implements SerDe {

  private static final class _Column {
    public _Column(byte[] colFamily, byte[] colQualifier, byte[] value) {
      this.colFamily = colFamily;
      this.colQualifier = colQualifier;
      this.value = value;
    }

    public final byte[] colFamily;
    public final byte[] colQualifier;
    public final byte[] value;
  }

  /** Key to lookup the current table used for storing data */
  public static String DATA_TABLE_KEY = "com.bah.culvert.table.data";

  /** Key to lookup the table name of the currently table used for indexing */
  public static String INDEX_TABLE_KEY = "com.bah.culvert.table.index";

  private CulvertIndexMapping[] mappings = null;

  private ObjectInspector culvertInspectorInstance;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    String colTypes = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    String cols = tbl.getProperty(Constants.LIST_COLUMNS);
    String mapping = CulvertHiveUtils.getCulvertMapping(tbl);
    if (conf == null) {
      // TODO: not sure if this is quite the right thing or not
      conf = new Configuration(false);
    }
    // set the fields we want for our jobs
    CulvertHiveUtils.setCulvertMapping(conf, mapping);
    /*
     * TODO, assuming we can just split the columns on a comma to get the types
     * for the columns
     */
    CulvertHiveUtils.setHiveColumnNamesInConf(conf, cols.split(","));
    /*
     * TODO assuming we can just split the column types on a colon to get the
     * names. There's code in the hbase adapter tests that suggests this might
     * not be right.
     */
    CulvertHiveUtils.setHiveColumnTypesInConf(conf, colTypes.split(":"));

    // set up our mappings
    this.mappings = CulvertHiveUtils.getIndexMappings(conf);
    // also go ahead and setup our object inspector
    this.culvertInspectorInstance = new CulvertResultInspector(mappings);
  }

  @Override
  public Object deserialize(Writable arg0) throws SerDeException {
    if (!(arg0 instanceof Result)) {
      throw new IllegalArgumentException(
          "Culvert SerDe can only handle loading culvert Result objects.");
    }
    return arg0;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return culvertInspectorInstance;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Put.class;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.hive.serde2.Serializer#serialize(java.lang.Object,
   * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector)
   */
  @Override
  public Put serialize(Object object, ObjectInspector inspector)
      throws SerDeException {
    Category cat = inspector.getCategory();
    if (cat != Category.STRUCT) {
      throw new SerDeException("Cannot process objects of type " + cat
          + ", only " + Category.STRUCT + " are allowed.");
    }
    StructObjectInspector structInspector = (StructObjectInspector) inspector;
    // we have to look for the primary key, save it as rowId
    byte[] rowId = null;
    /*
     * we can't create the CKeyValues at the same time since the rowId won't
     * always be first, so we save them in a temporary list and transform it
     * later.
     */
    List<_Column> columns = new ArrayList<_Column>();
    for (CulvertIndexMapping mapping : mappings) {
      StructField field = structInspector.getStructFieldRef(mapping
          .getHiveColumn());
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      Object data = structInspector.getStructFieldData(
          object, field);
      assert (data != null);
      if (fieldInspector.getCategory() != Category.PRIMITIVE) {
        throw new SerDeException(
            "Culvert's integration presently supports only primitives.");
      }
      Object primitive = ((PrimitiveObjectInspector) fieldInspector)
          .getPrimitiveJavaObject(data);
      byte[] value = CulvertHiveUtils.getBytesForPrimitiveObject(primitive,
          mapping.getType());
      if(value == null) continue;
      if (mapping.isRowId()) {
        rowId = value;
      } else if (mapping.isFamilyAsMap()) {
        throw new SerDeException("Culvert's integration presently does not "
            + "support storing families as maps.");
      } else {
        columns.add(new _Column(mapping.getColumnFamily(), mapping
            .getColumnQualifier(), value));
      }
    }

    // make sure we have a primary key
    if (rowId == null) {
      throw new SerDeException("In culvert you must provide a value for "
          + "the row id (primary key). This is specified "
          + "by the column mapping for :key.");
    }
    final byte[] finalId = rowId;

    // now that we have the primary key, return
    List<CKeyValue> keyValues = Lists.transform(columns,
        new Function<_Column, CKeyValue>() {
          @Override
          public CKeyValue apply(_Column input) {
            return new CKeyValue(finalId, input.colFamily, input.colQualifier,
                input.value);
          }
        });
    if (keyValues.size() == 0) {
      keyValues = Arrays.asList(new CKeyValue(rowId));
    }
    return new Put(keyValues);
  }
}

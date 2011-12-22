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
package com.bah.culvert.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;

/**
 * This is a StructObjectInspector to inspect the data from the CulvertResult
 * object. This does not utilize the Lazy methodology that is prevalent in the
 * Hive ObjectInspectors. Instead, it directly reads from the Result object and
 * returns data from a Result's CKeyValue list.
 * <p>
 * This means that there is no ObjectInspector for specific fields (e.g., as
 * seen in {@link LazyShortObjectInspector}). Data is returned as the Java
 * primitive type object value from the
 * {@link #getStructFieldData(Object, StructField)} method which will inspect
 * the data directly, rather than use a designated object inspector for the
 * field.
 * 
 */
public class CulvertResultInspector extends StructObjectInspector {

  /** CulvertIndexMapping will contain the hive column to Culvert column. */
  private final CulvertIndexMapping[] culvertMapping;

  /** StructField list used to indicate help retrieve data from a Result object. */
  private final List<CulvertField> fieldList = new ArrayList<CulvertField>();

  /**
   * Creates the CulvertField objects with PrimitiveObjectInspector as the field
   * ObjectInspector.
   * 
   * @param culvertMapping
   */
  public CulvertResultInspector(CulvertIndexMapping[] culvertMapping) {
    this.culvertMapping = culvertMapping;

    // Create the CulvertFields
    for (int i = 0; i < culvertMapping.length; i++) {
      CulvertIndexMapping fieldMap = culvertMapping[i];
      fieldList.add(new CulvertField(i, fieldMap.getHiveColumn(), fieldMap
          .getType()));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector#getTypeName()
   */
  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector#getCategory()
   */
  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector#
   * getAllStructFieldRefs()
   */
  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fieldList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector#
   * getStructFieldRef(java.lang.String)
   * 
   * Gets the CulvertField for the specified field.
   */
  @Override
  public StructField getStructFieldRef(String fieldName) {
    fieldName = fieldName.toLowerCase();

    for (CulvertField culvertField : fieldList) {
      if (culvertField.getFieldName().equals(fieldName)) {
        return culvertField;
      }
    }

    // Unable to find field
    throw new RuntimeException("Unable to find field " + fieldName
        + " in the field list.");
  }

  /**
   * Gets the specified field out of the Result data object.
   * 
   * @param data The {@link Result} object.
   * @return The Java primitive type object for the data in the field.
   */
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    // Check the data
    try {
      if (!(data instanceof Result)) {
        throw new RuntimeException(
            "Struct data is not an instance of the Result class.");
      }
    } catch (NullPointerException e) {
      throw new RuntimeException("Struct data is null.");
    }

    // Check the fieldRef
    try {
      if (!(fieldRef instanceof CulvertField)) {
        throw new RuntimeException(
            "fieldRef is not an instance of the CulvertField class.");
      }
    } catch (NullPointerException e) {
      throw new RuntimeException("fieldRef is null.");
    }

    // Get the data
    Result result = (Result) data;
    CulvertField field = (CulvertField) fieldRef;

    // Get the position of the data
    int pos = field.getFieldId();

    // Get the column family-qualifier tuple
    byte[] colFam;
    byte[] colQual;
    try {
      colFam = culvertMapping[pos].getColumnFamily();
      colQual = culvertMapping[pos].getColumnQualifier();
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException(
          "Index out of bounds in the Culvert-Hive mappings array.");
    }

    // Get the value from the Result object
    CKeyValue keyValue;
    try {
      keyValue = result.getValue(colFam, colQual);
    } catch (NullPointerException e) {
      throw new RuntimeException(
          "Trying to access result key value with null objects.");
    }

    // Attempt to convert the byte[] to a Java object
    byte[] value = keyValue.getValue();
    Object resultData = CulvertHiveUtils.getPrimitiveForBytes(value,
        field.getFieldType());

    return resultData;
  }

  /**
   * Gets all the fields from the data object. Uses
   * {@link CulvertResultInspector#getStructFieldData(Object, StructField)} to
   * retrieve the data.
   * 
   * @param data The {@link Result} object.
   * @return List of Java primitive type objects representing the data in the
   *         result object.
   */
  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    // Iterate through the fields
    List<Object> dataList = new ArrayList<Object>();

    for (CulvertField field : fieldList) {
      Object fieldData = getStructFieldData(data, field);
      dataList.add(fieldData);
    }

    return dataList;
  }

}
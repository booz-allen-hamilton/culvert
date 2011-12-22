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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.hive.CulvertField;
import com.bah.culvert.hive.CulvertIndexMapping;
import com.bah.culvert.hive.CulvertResultInspector;
import com.bah.culvert.util.Bytes;

/**
 * Tests the CulvertResultInspector by testing single inspections and a multiple
 * inspection test.
 */
public class CulvertResultInspectorTest {

  private final String stringValue = "value";
  private final int intValue = 1;
  private final short shortValue = (short) 1;
  private final long longValue = 100;
  private final double doubleValue = 4.35;
  private final boolean boolValue = true;

  private final byte[] rowId = "rowId".getBytes();
  private final byte[] fam1 = "fam1".getBytes();
  private final byte[] fam2 = "fam2".getBytes();
  private final byte[] col1 = "col1".getBytes();
  private final byte[] col2 = "col2".getBytes();
  private final byte[] valString = stringValue.getBytes();
  private final byte[] valInt = Bytes.toBytes(intValue);
  private final byte[] valShort = Bytes.toBytes(shortValue);
  private final byte[] valLong = Bytes.toBytes(longValue);
  private final byte[] valDouble = Bytes.toBytes(doubleValue);
  private final byte[] valBool = Bytes.toBytes(boolValue);

  private List<CKeyValue> keyValueList;

  @Before
  public void setUp() throws Exception {
    keyValueList = new ArrayList<CKeyValue>();
  }

  /**
   * Test getting all the data in the form of a list.
   */
  @Test
  public void testMultipleKeyValues() {
    // Create the result
    keyValueList.add(new CKeyValue(rowId, fam1, col1, valString));
    keyValueList.add(new CKeyValue(rowId, fam1, col2, valInt));
    keyValueList.add(new CKeyValue(rowId, fam2, col1, valShort));
    keyValueList.add(new CKeyValue(rowId, fam2, col2, valLong));
    Result result = new Result(keyValueList);

    // Create the mapping
    CulvertIndexMapping[] mappingArray = {
        CulvertIndexMapping.forColumnFamilyAndQualifier(fam1, col1,
            "StringValue", "STRING"),
        CulvertIndexMapping.forColumnFamilyAndQualifier(fam1, col2, "IntValue",
            "INT"),
        CulvertIndexMapping.forColumnFamilyAndQualifier(fam2, col1,
            "ShortValue", "SMALLINT"),
        CulvertIndexMapping.forColumnFamilyAndQualifier(fam2, col2,
            "LongValue", "BIGINT") };
    CulvertResultInspector resultInspector = new CulvertResultInspector(
        mappingArray);

    // Test the amount of field refs
    assertTrue(resultInspector.getAllStructFieldRefs().size() == 4);

    // Test getting all the data (this is somewhat redundant with the single
    // inspector tests below).
    List<Object> dataList = resultInspector.getStructFieldsDataAsList(result);
    assertTrue(dataList.size() == 4);

    // Check each data
    assertTrue(dataList.get(0).equals(stringValue));
    assertTrue(dataList.get(1).equals(intValue));
    assertTrue(dataList.get(2).equals(shortValue));
    assertTrue(dataList.get(3).equals(longValue));
  }

  /**
   * Tests inspecting a single String value.
   */
  @Test
  public void testStringInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valString;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "StringValue";
    String type = "STRING";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof String);
    assertTrue(data.equals(stringValue));
  }

  @Test
  public void testIntegerInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valInt;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "IntValue";
    String type = "INT";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof Integer);
    assertTrue(data.equals(intValue));
  }

  /**
   * Tests inspecting a single Short value.
   */
  @Test
  public void testShortInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valShort;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "ShortValue";
    String type = "SMALLINT";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof Short);
    assertTrue(data.equals(shortValue));
  }

  /**
   * Tests inspecting a single Long value.
   */
  @Test
  public void testLongInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valLong;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "LongValue";
    String type = "BIGINT";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof Long);
    assertTrue(data.equals(longValue));
  }

  /**
   * Tests inspecting a single String value.
   */
  // @Ignore("Skipped until Bytes.toBytes(double) is implemented.")
  @Test
  public void testDoubleInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valDouble;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "DoubleValue";
    String type = "DOUBLE";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof Double);
    assertTrue(data.equals(doubleValue));
  }

  /**
   * Tests inspecting a single Boolean value.
   */
  // @Ignore("Skipped until Bytes.toBytes(boolean) is implemented.")
  @Test
  public void testBoolInspector() {
    // Create the result
    byte[] family = fam1;
    byte[] qualifier = col1;
    byte[] value = valBool;
    keyValueList.add(new CKeyValue(rowId, family, qualifier, value));
    Result result = new Result(keyValueList);
    String hiveColumn = "BoolValue";
    String type = "BOOLEAN";

    Object data = getData(result, family, qualifier, hiveColumn, type);

    assertTrue(data instanceof Boolean);
    assertTrue(data.equals(boolValue));
  }

  /**
   * Gets the data using the ObjectInspector.
   * 
   * @param result
   * @param family
   * @param qualifier
   * @param hiveColumn
   * @param type
   * @return
   */
  public Object getData(Result result, byte[] family, byte[] qualifier,
      String hiveColumn, String type) {
    // Create the culvert mapping and the result inspector
    CulvertIndexMapping culvertMapping = CulvertIndexMapping
        .forColumnFamilyAndQualifier(family, qualifier, hiveColumn, type);
    CulvertIndexMapping[] mappingArray = { culvertMapping };
    CulvertResultInspector resultInspector = new CulvertResultInspector(
        mappingArray);

    // Get the fields
    List<? extends StructField> fields = resultInspector
        .getAllStructFieldRefs();

    // There should only be one field
    assertTrue(fields.size() == 1);
    CulvertField field = (CulvertField) fields.get(0);

    // Get the data
    Object data = resultInspector.getStructFieldData(result, field);

    return data;
  }

}
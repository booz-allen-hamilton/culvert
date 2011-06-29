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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 *  
 *
 */
public class CulvertField implements StructField {

  private final int fieldId;
  private final String fieldName;
  private final String fieldType;

  public CulvertField(int fieldId, String fieldName, String fieldType) {
    this.fieldId = fieldId;
    this.fieldName = fieldName.toLowerCase();
    this.fieldType = fieldType;
  }

  public String getFieldType() {
    return fieldType;
  }

  public int getFieldId() {
    return fieldId;
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  /**
   * This does not use object inspectors to check the data. Data inspection is
   * done in
   * {@link CulvertResultInspector#getStructFieldData(Object, StructField).
   */
  @Override
  public ObjectInspector getFieldObjectInspector() {
    throw new RuntimeException(
        "FieldObjectInspectors in CulvertFields are unused. Use the CulvertResultInspector directly.");
  }
}

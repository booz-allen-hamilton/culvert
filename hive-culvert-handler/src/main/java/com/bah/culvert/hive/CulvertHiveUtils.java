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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import com.bah.culvert.util.Bytes;

/**
 * This class contains various static utilities for working with Culvert's hive
 * integration, particularly utilities for manipulating the configurating and
 * saving/serializing/deserializing relevant information to the job
 * configuration.
 */
public class CulvertHiveUtils {

  /**
   * Map of hive types to java primitive types, used for processing hive types
   * into byte arrays.
   */
  private static final Map<String, Class<?>> HIVE_TO_JAVA_MAP = new HashMap<String, Class<?>>();
  static {
    HIVE_TO_JAVA_MAP.put(Constants.TINYINT_TYPE_NAME, Byte.class);
    HIVE_TO_JAVA_MAP.put(Constants.SMALLINT_TYPE_NAME, Short.class);
    HIVE_TO_JAVA_MAP.put(Constants.INT_TYPE_NAME, Integer.class);
    HIVE_TO_JAVA_MAP.put(Constants.BIGINT_TYPE_NAME, Long.class);
    HIVE_TO_JAVA_MAP.put(Constants.DOUBLE_TYPE_NAME, Double.class);
    HIVE_TO_JAVA_MAP.put(Constants.BOOLEAN_TYPE_NAME, Boolean.class);
    HIVE_TO_JAVA_MAP.put(Constants.STRING_TYPE_NAME, String.class);
    HIVE_TO_JAVA_MAP.put(Constants.FLOAT_TYPE_NAME, Float.class);
  }
  /**
   * Setting for storing the column mapping for hive to bigtable-style columns.
   */
  private static final String CULVERT_HIVE_MAPPING_CONF_KEY = "culvert.hive.column.mapping";
  /**
   * Setting for storing the hive column names. This should correlate 1-1 to the
   * column mappings.
   */
  private static final String CULVERT_HIVE_COLUMN_NAMES_CONF_KEY = "culvert.hive.column.names";
  /**
   * Setting for storing the hive column types. This should correlate 1-1 to the
   * column mappings.
   */
  private static final String CULVERT_HIVE_COLUMN_TYPES_CONF_KEY = "culvert.hive.column.types";
  /**
   * Setting for storing the table being accessed by hive.
   */
  private static final String CULVERT_HIVE_CTABLE_NAME_CONF_KEY = "culvert.hive.culvert.table.name";
  /**
   * Setting for storing weather or not to use the hive properties as the
   * culvert configuration.
   */
  private static final String CULVERT_HIVE_CONF_IS_EMBEDDED_CONF_KEY = "culvert.hive.conf.is.embedded";
  /**
   * Setting for storing the culvert configuration as an external file. Can
   * accept HDFS paths as well.
   */
  private static final String CULVERT_HIVE_EXTERNAL_CONF_FILE_CONF_KEY = "culvert.hive.conf.file.external";

  private CulvertHiveUtils() {
  }

  /**
   * Get the index mappings. Since information about the hive columns is not
   * kept in the job conf, the configuration first needs to be setup using:
   * <ul>
   * <li>{@link #setHiveColumnNamesInConf(Configuration, String[])}</li>
   * <li>{@link #setHiveColumnTypesInConf(Configuration, String[])}</li>
   * <ul>
   * These values can be set in the
   * {@link CulvertSerDe#initialize(Configuration, java.util.Properties)}, since
   * the serde get access to all the table properties at that point. You can
   * also get access to these settings from the table properties in
   * {@link CulvertMetaHook}.
   * @param conf
   * @return
   */
  public static CulvertIndexMapping[] getIndexMappings(Configuration conf) {
    String culvertMapping = getCulvertMapping(conf);
    String[] hiveTypes = getHiveColumnTypesInConf(conf);
    String[] hiveNames = getHiveColumnNamesFromConf(conf);
    String[] mapElementStrings = culvertMapping.split(",");
    int elements = hiveNames.length;
    if (hiveTypes.length != elements || mapElementStrings.length != elements) {
      throw new IllegalArgumentException(
          "Hive types, names, and mapping should all be"
              + "the same length. Instead got hive types: " + hiveTypes.length
              + " hive names: " + hiveNames.length + " map elements: "
              + mapElementStrings.length);
    }
    CulvertIndexMapping[] mappings = new CulvertIndexMapping[elements];
    for (int i = 0; i < elements; i++) {
      String mapElementString = mapElementStrings[i];
      String hiveColumnType = hiveTypes[i];
      String hiveColumnName = hiveNames[i];
      // row id mapping
      if (mapElementString.equals(":key")) {
        mappings[i] = CulvertIndexMapping.forRowIdAsColumn(hiveColumnName,
            hiveColumnType);
        // column family as map
      } else if (mapElementString.matches("[a-zA-Z0-9]+:")) {
        String familyName = mapElementString.substring(0,
            mapElementString.length() - 1);
        mappings[i] = CulvertIndexMapping.forFamilyAsMap(
            Bytes.toBytes(familyName), hiveColumnName, hiveColumnType);
        // column family + qualifier
      } else if (mapElementString.matches("[a-zA-Z0-9]+:[a-zA-Z0-9]+")) {
        String familyName = mapElementString.substring(0,
            mapElementString.indexOf(':'));
        String qualifierName = mapElementString.substring(
            mapElementString.indexOf(':') + 1, mapElementString.length());
        mappings[i] = CulvertIndexMapping.forColumnFamilyAndQualifier(
            Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            hiveColumnName, hiveColumnType);
      } else {
        throw new RuntimeException("Unsupported mapping string: "
            + mapElementString + " must match one of\n :key\n"
            + " [a-zA-Z0-9]+:\n " + "[a-zA-Z0-9]+:[a-zA-Z0-9]+\n");
      }
    }
    return mappings;
  }

  /**
   * Get the culvert mapping string. See
   * {@link #getIndexMappings(Configuration)} for the format.
   * @param conf
   *          The configuration to get the mapping string from.
   * @return The unparsed mappings string.
   */
  public static String getCulvertMapping(Configuration conf) {
    return conf.get(CULVERT_HIVE_MAPPING_CONF_KEY);
  }

  /**
   * Get the culvert mapping string from a properties object. See
   * {@link #getIndexMappings(Configuration)} for the format.
   * @param conf
   *          The properties to pull the string from.
   * @return The unparsed mapping string.
   */
  public static String getCulvertMapping(Properties conf) {
    return (String) conf.get(CULVERT_HIVE_MAPPING_CONF_KEY);
  }

  /**
   * Set the culvert mapping string on a configuration object. See
   * {@link #getIndexMappings(Configuration)} for the format.
   * @param conf
   *          The configuration to set the mapping in.
   * @param mapping
   *          The mapping string.
   */
  public static void setCulvertMapping(Configuration conf, String mapping) {
    conf.set(CULVERT_HIVE_MAPPING_CONF_KEY, mapping);
  }

  /**
   * Set the culvert mapping string on a configuration object. See
   * {@link #getIndexMappings(Configuration)} for the format.
   * @param conf
   *          The configuration to set the mapping in.
   * @param mapping
   *          The mapping string.
   */
  public static void setCulvertMapping(Properties conf, String mapping) {
    conf.setProperty(CULVERT_HIVE_MAPPING_CONF_KEY, mapping);
  }

  /**
   * Set the hive column names in the conf. Corresponds 1-1 to the mappings.
   * @param conf
   *          The conf to set the hive columns in.
   * @param names
   *          The column names.
   */
  public static void setHiveColumnNamesInConf(Configuration conf, String[] names) {
    conf.setStrings(CULVERT_HIVE_COLUMN_NAMES_CONF_KEY, names);
  }

  /**
   * Get the hive column names in the conf. Corresponds 1-1 to the mappings.
   * @param conf
   *          The configuration to get the the names out of.
   * @return The hive column names.
   */
  public static String[] getHiveColumnNamesFromConf(Configuration conf) {
    return conf.getStrings(CULVERT_HIVE_COLUMN_NAMES_CONF_KEY);
  }

  /**
   * Set the hive column types in the conf. Corresponds 1-1 to the mappings.
   * @param conf
   *          The configuration to set the types in.
   * @param types
   *          The types to set.
   */
  public static void setHiveColumnTypesInConf(Configuration conf, String[] types) {
    conf.setStrings(CULVERT_HIVE_COLUMN_TYPES_CONF_KEY, types);
  }

  /**
   * Get the hive column types from the conf. Corresponds 1-1 to the mappings.
   * @param conf
   *          The configuration to get the hive column types from.
   * @return The column types.
   */
  public static String[] getHiveColumnTypesInConf(Configuration conf) {
    return conf.getStrings(CULVERT_HIVE_COLUMN_TYPES_CONF_KEY);
  }

  /**
   * 
   * @param data
   * @param storageType
   * @return
   */
  public static Object getPrimitiveForBytes(byte[] data, String storageType) {
    Object primitive;
    Class<?> storageClass = HIVE_TO_JAVA_MAP.get(storageType.toLowerCase());
    
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    
    if (Byte.class.isAssignableFrom(storageClass)) {
      primitive = data;
    } else if (Short.class.isAssignableFrom(storageClass)) {
      primitive = byteBuffer.getShort();
    } else if (Integer.class.isAssignableFrom(storageClass)) {
      primitive = byteBuffer.getInt();
    } else if (Long.class.isAssignableFrom(storageClass)) {
      primitive = byteBuffer.getLong();
    } else if (Double.class.isAssignableFrom(storageClass)) {
      primitive = byteBuffer.getDouble();
    } else if (String.class.isAssignableFrom(storageClass)) {
      primitive = Bytes.toString(data);
    } else if (Boolean.class.isAssignableFrom(storageClass)) {
      //TODO need to check if this is correct
      boolean value = true;
      if(data[0] == 0x0){
        value = false;
      }
      primitive = new Boolean(value);
    } else {
      throw new IllegalArgumentException("Unsupported storage type "
          + storageType);
    }
    
    return primitive;
  }
  
  /**
   * Get the byte representation of an object returned by
   * {@link PrimitiveObjectInspector#getPrimitiveJavaObject(Object)} as it
   * should be stored in a column with a particular type.
   * <p>
   * Column types can be one of tinyint, smallint, int, bigint, double, boolean,
   * float, or string. These types are stored in {@link Constants}.
   * @param primitive
   * @param storageType
   *          The storage type.
   * @return null if the primitive passed is null.
   * @see Constants#TINYINT_TYPE_NAME
   * @see Constants#SMALLINT_TYPE_NAME
   * @see Constants#INT_TYPE_NAME
   * @see Constants#BIGINT_TYPE_NAME
   * @see Constants#DOUBLE_TYPE_NAME
   * @see Constants#FLOAT_TYPE_NAME
   * @see Constants#BOOLEAN_TYPE_NAME
   * @see Constants#STRING_TYPE_NAME
   */
  public static byte[] getBytesForPrimitiveObject(Object primitive,
      String storageType) {
    if (primitive == null)
      return null;
    byte[] byteValue;
    Class<?> storageClass = HIVE_TO_JAVA_MAP.get(storageType.toLowerCase());
    /* if its a number, number has a nice method for returning the casted value */
    if (primitive instanceof Number) {
      Number num = (Number) primitive;
      if (Byte.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.byteValue());
      } else if (Short.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.shortValue());
      } else if (Integer.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.intValue());
      } else if (Long.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.longValue());
      } else if (Double.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.doubleValue());
      } else if (Float.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.floatValue());
      } else if (String.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(num.toString());
      } else if (Boolean.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(!num.equals(0));
      } else {
        throw new IllegalArgumentException("Unsupported storage type "
            + storageType);
      }
      /* if its a string, try to parse it */
    } else if (primitive instanceof String) {
      String str = (String) primitive;
      if (Byte.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Byte.parseByte(str));
      } else if (Short.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Short.parseShort(str));
      } else if (Integer.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Integer.parseInt(str));
      } else if (Long.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Long.parseLong(str));
      } else if (Double.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Double.parseDouble(str));
      } else if (Float.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Float.parseFloat(str));
      } else if (String.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(str);
      } else if (Boolean.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(Boolean.parseBoolean(str));
      } else {
        throw new IllegalArgumentException("Unsupported storage type "
            + storageType);
      }
      /* if its a boolean use 1=true 0=false */
    } else if (primitive instanceof Boolean) {
      Boolean tf = (Boolean) primitive;
      byte val = (byte) (tf ? 1 : 0);
      if (Byte.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(val);
      } else if (Short.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(val);
      } else if (Integer.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes((int) val);
      } else if (Long.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes((long) val);
      } else if (Double.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes((double) val);
      } else if (Float.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes((float) val);
      } else if (String.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(tf ? "true" : "false");
      } else if (Boolean.class.isAssignableFrom(storageClass)) {
        byteValue = Bytes.toBytes(tf);
      } else {
        throw new IllegalArgumentException("Unsupported storage type "
            + storageType);
      }
    } else {
      throw new IllegalArgumentException(
          "Primitive must be a number, boolean, or string.");
    }
    return byteValue;
  }

  /**
   * Get the culvert configuration. Checks to see if we should just treat the
   * properties as the configuration, or if we should load an external file.
   * Finally, returns the configuration based off of the properties.
   * @param props
   *          The properties to examine and possibly turn into a configuration.
   * @return The new configuration.
   * @see CulvertHiveUtils#isCulvertConfigurationEmbedded(Properties)
   * @see #propsToConf(Properties)
   * @see #setCulvertConfiguration(Properties, Configuration)
   * @see #setCulvertConfiguration(Properties, String)
   */
  public static Configuration getCulvertConfiguration(Properties props) {
    if (isCulvertConfigurationEmbedded(props)) {

      return propsToConf(props);
    } else {
      String fileName = props
          .getProperty(CULVERT_HIVE_EXTERNAL_CONF_FILE_CONF_KEY);
      Path fileLocation = new Path(fileName);
      FSDataInputStream input = null;
      try {
        FileSystem fs = fileLocation.getFileSystem(new Configuration());
        Configuration conf = new Configuration(false);
        input = fs.open(fileLocation);
        conf.readFields(input);
        return conf;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (input != null)
          try {
            input.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
      }
    }
  }

  /**
   * Turn a properties object into a configuration object.
   * @param props
   *          The properties to transform.
   * @return A new configuration populated with the properties settings.
   */
  public static Configuration propsToConf(Properties props) {
    Configuration conf = new Configuration(false);
    for (Entry<Object, Object> entry : props.entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }

  /**
   * Set a confiugration as an embedded conf inside of a properties. Used
   * primarily for interaction with components during testing; in a real
   * scenario this would be set by the user in the hive properties.
   * <p>
   * Retrieval of the culvert configuration is handled in
   * {@link #getCulvertConfiguration(Properties)}
   * @param props
   *          The properties to turn into a configuration.
   * @param conf
   *          The conf to insert key, value pairs into.
   * @see #isCulvertConfigurationEmbedded(Properties)
   * @see #setCulvertConfiguration(Properties, String)
   * @see #getCulvertConfiguration(Properties)
   */
  public static void setCulvertConfiguration(Properties props,
      Configuration conf) {
    setCulvertConfigurationIsEmbedded(props, true);
    for (Entry<String, String> entry : conf) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Set a configuration file to use as an external culvert configuration.
   * @param props
   *          The culvert configuration file to use.
   * @param filename
   *          The path expression to where the file is stored. Supports
   *          hdfs-style and regular paths.
   * @see CulvertHiveUtils#isCulvertConfigurationEmbedded(Properties)
   * @see #setCulvertConfiguration(Properties, Configuration)
   * @see #getCulvertConfiguration(Properties)
   */
  public static void setCulvertConfiguration(Properties props, String filename) {
    setCulvertConfigurationIsEmbedded(props, false);
    props.setProperty(CULVERT_HIVE_EXTERNAL_CONF_FILE_CONF_KEY, filename);
  }

  /**
   * Return true if the culvert configuration is embedded in this properties
   * object. This basically means the properties are the culvert configuration.
   * @param props
   *          The properties to check.
   * @return true if the configuration is embedded in the properties.
   */
  public static boolean isCulvertConfigurationEmbedded(Properties props) {
    String val = props.getProperty(CULVERT_HIVE_CONF_IS_EMBEDDED_CONF_KEY,
        "false");
    return val.equals("true");
  }

  /**
   * Set weather or not this properties object can be translated into a culvert
   * configuration.
   * @param props
   *          The properties to change.
   * @param embedded
   *          Weather or not the culvert configuration is embedded in these
   *          properties.
   */
  public static void setCulvertConfigurationIsEmbedded(Properties props,
      boolean embedded) {
    props.setProperty(CULVERT_HIVE_CONF_IS_EMBEDDED_CONF_KEY, embedded ? "true"
        : "false");
  }

  /**
   * Get the culvert table to use as input.
   * @param conf
   *          The configuration to get the table name from.
   * @return The culvert table name.
   */
  public static String getCulvertTable(Configuration conf) {
    return conf.get(CULVERT_HIVE_CTABLE_NAME_CONF_KEY);
  }

  /**
   * Get the culvert table to use.
   * @param conf
   *          The properties to get the table name from.
   * @return The culvert table name.
   */
  public static String getCulvertTable(Properties props) {
    return props.getProperty(CULVERT_HIVE_CTABLE_NAME_CONF_KEY);
  }

  /**
   * Set the culvert table to use as input.
   * @param conf
   *          The configuration to set the table name on.
   * @return The culvert table name.
   */
  public static void setCulvertTable(Configuration conf, String table) {
    conf.set(CULVERT_HIVE_CTABLE_NAME_CONF_KEY, table);
  }

  /**
   * Set the culvert table to use as input.
   * @param props
   *          The configuration to set the table name on.
   * @return The culvert table name.
   */
  public static void setCulvertInputTable(Properties props, String table) {
    props.setProperty(CULVERT_HIVE_CTABLE_NAME_CONF_KEY, table);
  }

  /**
   * Turn a configuration into properties
   * @param conf
   *          The configuration to turn into properties.
   * @return The properties version of the configuration.
   */
  public static Properties confToProps(Configuration conf) {
    Properties props = new Properties();
    for (Entry<String, String> entry : conf) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
    return props;
  }

  public static void setCulvertTable(Properties tblProps, String table) {
    tblProps.setProperty(CULVERT_HIVE_CTABLE_NAME_CONF_KEY, table);
  }
}

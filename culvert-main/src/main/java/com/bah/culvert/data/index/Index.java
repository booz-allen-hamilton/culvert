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
package com.bah.culvert.data.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.Client;
import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CRange;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.BaseConfigurable;
import com.bah.culvert.util.LexicographicBytesComparator;
import com.google.common.base.Objects;

//import static com.bah.culvert.util.Constants.*;

/**
 * An index on a table. Index implementations may be instantiated multiple times
 * over a table or even a single column in a table. This promotes code reuse.
 * <p>
 * Indices are uniquely identified by their name. This promotes a functional
 * programming style so that multiple instantiations can logically refer to the
 * same index.
 */
public abstract class Index extends BaseConfigurable implements Writable {

  public static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final String NAME_CONF_KEY = "culvert.index.name";
  private static final String PRIMARY_TABLE_CONF_KEY = "culvert.index.table.primary";
  private static final String INDEX_TABLE_CONF_KEY = "culvert.index.table.index";
  private static final String COL_FAM_CONF_KEY = "culvert.index.family";
  private static final String FAM_BASE64_ENCODED_CONF_KEY = "culvert.index.family.base64";
  private static final String COL_QUAL_CONF_KEY = "culvert.index.qualifier";
  private static final String QUAL_BASE64_ENCODED_CONF_KEY = "culvert.index.qual.base64";


  /**
   * For use with {@link #readFields(DataInput)}
   */
  public Index() {

  }

  /**
   * Create an index around the specified values
   * @param name of the index
   * @param columnFamily that this index indexes
   * @param columnQualifier that this index indexes
   * @param database that this database can access to
   * @param primaryTable
   * @param indexTable
   */
  public Index(String name, byte[] columnFamily, byte[] columnQualifier,
      DatabaseAdapter database, String primaryTable, String indexTable) {
    super();
    Configuration conf = new Configuration();
    super.setConf(conf);

    // Set the configuration
    Index.setIndexName(name, conf);
    Index.setColumnFamily(columnFamily, conf);
    Index.setColumnQualifier(columnQualifier, conf);
    Index.setPrimaryTable(primaryTable, conf);
    Index.setIndexTable(indexTable, conf);

    // Store the database
    this.setDatabaseAdapater(database);
  }

  /**
   * Set the index name.
   * @param name The name of the index.
   * @param conf The configuration to set.
   */
  public static void setIndexName(String name, Configuration conf) {
    conf.set(NAME_CONF_KEY, name);
  }

  /**
   * Set the name of the data table containing the indexed row tuple.
   * @param table The name of the data table.
   * @param conf The configuration to set.
   */
  public static void setPrimaryTable(String table, Configuration conf) {
    conf.set(PRIMARY_TABLE_CONF_KEY, table);
  }

  /**
   * Set the name of the index table. This should refer to the actual name of
   * the index table. This is can be different than the name of the index.
   * @param table The name of the index table.
   * @param conf The configuration to set.
   */
  public static void setIndexTable(String table, Configuration conf) {
    conf.set(INDEX_TABLE_CONF_KEY, table);
  }

  /**
   * Set the column family of the column tuple that is being indexed.
   * @param colFam The column family.
   * @param conf The configuration to set.
   */
  public static void setColumnFamily(String colFam, Configuration conf) {
    conf.set(COL_FAM_CONF_KEY, colFam);
  }

  /**
   * Set the column qualifier of the column tuple that is being indexed.
   * @param colQual The column qualifier.
   * @param conf The configuration to set.
   */
  public static void setColumnQualifier(String colQual, Configuration conf) {
    conf.set(COL_QUAL_CONF_KEY, colQual);
  }

  /**
   * Set the column family (in bytes) of the column tuple that is being indexed.
   * @param colFam The column family.
   * @param conf The configuration to set.
   */
  public static void setColumnFamily(byte[] colFam, Configuration conf) {
    setBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY, COL_FAM_CONF_KEY, colFam,
        conf);
  }

  /**
   * Set the column qualifier (in bytes) of the column tuple that is being
   * indexed.
   * @param colQual The column qualifier.
   * @param conf The configuration to set.
   */
  public static void setColumnQualifier(byte[] colQual, Configuration conf) {
    setBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY, COL_QUAL_CONF_KEY,
        colQual, conf);
  }

  /**
   * Used to set a key indicating if the string value held by another
   * configuration key is a base64 encoded binary or not.
   * @param isValueBinaryEncodedSetting The key telling weather or not the other
   *        key (setting) is base64.
   * @param potentiallyEncodedSetting The actual key that might be base64
   *        encoded.
   * @param data The data to set as base64.
   * @param conf The configuration to do the setting on.
   */
  private static void setBinaryConfSetting(String isValueBinaryEncodedSetting,
      String potentiallyEncodedSetting, byte[] data, Configuration conf) {
    CharsetDecoder decoder = UTF_8.newDecoder();
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    try {
      CharBuffer colFamString = decoder.decode(ByteBuffer.wrap(data));
      conf.setBoolean(isValueBinaryEncodedSetting, false);
      conf.set(potentiallyEncodedSetting, colFamString.toString());
    } catch (CharacterCodingException e) {
      conf.setBoolean(isValueBinaryEncodedSetting, true);
      conf.set(potentiallyEncodedSetting, new String(Base64.encodeBase64(data),
          UTF_8));
    }
  }

  /**
   * Get the contents of a key that might be binary.
   * @param isBinarySettingKey Tells us weather or not the field is binary.
   * @param potentiallyBinaryEncodedSetting The actual field name that might
   *        contain binary data.
   * @param conf The configuration to retrieve from
   * @return The decoded value to return.
   */
  private static byte[] getBinaryConfSetting(String isBinarySettingKey,
      String potentiallyBinaryEncodedSetting, Configuration conf) {
    String value = conf.get(potentiallyBinaryEncodedSetting);
    boolean isBase64 = conf.getBoolean(isBinarySettingKey, false);
    if (isBase64) {
      return Base64.decodeBase64(value.getBytes());
    } else {
      return value.getBytes();
    }
  }

  public static String getPrimaryTableName(Configuration conf) {
    return conf.get(PRIMARY_TABLE_CONF_KEY);
  }

  public void setDatabaseAdapater(DatabaseAdapter database) {
    DatabaseAdapter.writeToConfiguration(database.getClass(),
        database.getConf(), getConf());
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), getColumnFamily(), getColumnQualifier(),
        getIndexTable());
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String s = ("Name:" + getName());
    return s;
  }

  /**
   * Get the column family that this index is configured to index.
   * @return The column family that this index is configured to index.
   */
  public byte[] getColumnFamily() {
    return getBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY, COL_FAM_CONF_KEY,
        getConf());
  }

  /**
   * Get the column family that this index is configured to index.
   * @return The column family that this index is configured to index.
   */
  public byte[] getColumnQualifier() {
    return getBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY,
        COL_QUAL_CONF_KEY, getConf());
  }

  /**
   * Get the name of this index. The name is used to determine what
   * configuration should be applied when thawing indicies from a client
   * configuration.
   * @return The index name.
   */
  public String getName() {
    return getConf().get(NAME_CONF_KEY);
  }

  /**
   * Get the index table for this index. The index is assumed to have complete
   * control over all data encoded in the index table, so that its contents
   * aren't clobbered by other table users.
   * @return The Index table used by this index.
   */
  public TableAdapter getIndexTable() {
    return getTableAdapter(getConf(), INDEX_TABLE_CONF_KEY);
  }

  /**
   * Just get the name of the index table (don't create an adapter).
   * @return The index table used by this index.
   */
  public String getIndexTableName() {
    return getConf().get(INDEX_TABLE_CONF_KEY);
  }

  /**
   * Get the primary table used for this index. This is the table that the index
   * indexes.
   * @return The primary table for this index.
   */
  public TableAdapter getPrimaryTable() {
    return getTableAdapter(getConf(), PRIMARY_TABLE_CONF_KEY);
  }

  /**
   * Just get the name of the primary table for this index, don't create an
   * adapter.
   * @return The primary table name for this index.
   */
  public String getPrimaryTableName() {
    return getConf().get(PRIMARY_TABLE_CONF_KEY);
  }

  /**
   * Gets a table adapter from a configuration.
   * @param conf
   * @param adapterSetting
   * @return
   */
  private static TableAdapter getTableAdapter(Configuration conf,
      String adapterSetting) {
    DatabaseAdapter db = DatabaseAdapter.readFromConfiguration(conf);
    String tableName = conf.get(adapterSetting);
    return db.getTableAdapter(tableName);
  }

  @Override
  public boolean equals(Object o) {
    LexicographicBytesComparator bc = LexicographicBytesComparator.INSTANCE;
    if (o instanceof Index) {
      Index oi = (Index) o;
      return oi.getName().equals(getName())
          && bc.compare(oi.getColumnFamily(), getColumnFamily()) == 0
          && bc.compare(oi.getColumnQualifier(), getColumnQualifier()) == 0
          && oi.getIndexTableName().equals(getIndexTableName())
          && oi.getPrimaryTableName().equals(getPrimaryTableName());
    }
    return false;
  }

  /**
   * Perform any operations necessary to index this put. The passed put is the
   * put for the primary table, not the put to use for the index table.
   * <p>
   * The Index will handle putting into the index table, leaving the
   * {@link Client} to handle assuring the put ends up in the primary table. The
   * index assumes that the column family and column qualifier of values in the
   * {@link Put} already meet the criteria for this index, before being called.
   * 
   * @param put The put to handle.
   */
  public abstract void handlePut(Put put);

  /**
   * Return rowid's between a particular range on the index.
   * @param indexRangeStart The range to start on. An empty array signals to
   *        begin at the beginning of the table.
   * @param indexRangeEnd The range to end on. An empty array signals to end at
   *        the end of the table.
   * @return An iterator of results containing the rowIds of records indexed in
   *         the requested range.
   */
  public abstract SeekingCurrentIterator handleGet(byte[] indexRangeStart,
      byte[] indexRangeEnd);

  /**
   * Primarily for use with MapReduce.
   * @return the range splits associated with this index table as the table is
   *         sharded over the distributed database
   */
  public List<CRange> getSplits() {
    TableAdapter indexTable = getIndexTable();
    byte[][] startkeys = indexTable.getStartKeys();
    byte[][] endKeys = indexTable.getEndKeys();
    List<CRange> range = new ArrayList<CRange>(startkeys.length);
    for (int i = 0; i < startkeys.length; i++) {
      range.add(new CRange(startkeys[i], endKeys[i]));
    }
    return range;
  }

  /**
   * Primarily for use with MapReduce.
   * @return the hosts that this hosting the index table.
   */
  public Collection<String> getPreferredHosts() {
    TableAdapter index = getIndexTable();
    return index.getHosts();
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(arg0);
    setConf(conf);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    getConf().write(arg0);
  }

}

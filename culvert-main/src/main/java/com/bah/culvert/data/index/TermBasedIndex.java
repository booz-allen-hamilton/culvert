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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;

/**
 * Creates an index based on tokenizing the terms within a string. Each index
 * value is stored in the row ID based on
 * <tt>&lt;term>&lt;6_nulls>&lt;data_row_id>&lt;data_row_id_length_in_4_bytes></tt>
 * .
 * <p>
 * Example: rowId: ABCD value:
 * "Steve vituperated Rodney and his pusillanimous nature" <br>
 * <ul>
 * <li>and\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>his\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>nature\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>pusillanimous\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>rodney\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>steve\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * <li>vituperated\x00\x00\x00\x00\x00\x00ABCD\x00\x00\x00\x04</li>
 * </ul>
 * 
 */
public class TermBasedIndex extends Index {

  private static final Log LOG = LogFactory.getLog(TermBasedIndex.class);

  // Configuration settings
  private static final String TOKEN_REGEX_KEY = "culvert.index.term.token";
  private static final String SPLITABLE_TERM_KEY = "culvert.index.term.splitable";
  private static final String TO_LOWER_KEY = "culvert.index.term.lower";

  /**
   * Regex used to tokenize the term string.
   * <p>
   * Currently set to: [^A-Za-z0-9]+
   */
  public static final String DEFAULT_TOKEN_REGEX = "[^A-Za-z0-9]+";

  /**
   * Default setting for splitting values.
   * <p>
   * Currently set to: true
   */
  private static final boolean DEFAULT_IS_SPLITABLE = true;

  /**
   * Default setting for indexing in lower case.
   * <p>
   * Currently set to: true
   */
  private static final boolean DEFAULT_IS_LOWER_CASED = true;

  /**
   * The size of the byte array to store the record row id length
   */
  public static final int NUM_RECORD_ID_LENGTH_BYTES = 4;

  /**
   * The number of null bytes for padding
   */
  public static final int NUM_NULL_BYTES = 6;

  /**
   * The null bytes padding
   */
  public static final byte[] NULL_BYTES_PADDING = new byte[NUM_NULL_BYTES];

  /**
   * For use with {@link #readFields(DataInput)} or
   * {@link #setConf(Configuration)}.
   */
  public TermBasedIndex() {

  }

  /**
   * Create a term based index. All the specified values are stored in a
   * configuration for use later. Defaults to splittable with the default token
   * regex.
   * @see Index
   * 
   * @param name of the index
   * @param columnFamily that this index indexes
   * @param columnQualifier that this index indexes
   * @param database that this database can access to
   * @param primaryTable
   * @param indexTable
   */
  public TermBasedIndex(String name, DatabaseAdapter database,
      String primaryTable, String indexTable, byte[] columnFamily,
      byte[] columnQualifier) {
    super(name, columnFamily, columnQualifier, database, primaryTable,
        indexTable);
  }

  /**
   * Create a term based index. All the specified values are stored in a
   * configuration for use later. Defaults to splittable based on the
   * @see Index
   * 
   * @param name
   * @param database
   * @param primaryTable
   * @param indexTable
   * @param columnFamily
   * @param columnQualifier
   * @param isSplittable if the terms are splittable based on the specified
   *        regex
   * @param regex to use. To use the default regex, either supply
   *        {@link #DEFAULT_TOKEN_REGEX}
   */
  public TermBasedIndex(String name, DatabaseAdapter database,
      String primaryTable, String indexTable, byte[] columnFamily,
      byte[] columnQualifier, boolean isSplittable, String regex) {
    super(name, columnFamily, columnQualifier, database, primaryTable,
        indexTable);
  }

  /**
   * Sets the regex used to split the value into tokens. The regex will only be
   * used if the configuration allows for splitting (
   * {@link #setSplitable(boolean, Configuration)}).
   * <p>
   * If no value is set, then the index will use the default regex (
   * {@link #DEFAULT_TOKEN_REGEX}).
   * 
   * @param regex Regex to split the value.
   * @param conf The configuration to set.
   */
  public static void setTokenRegex(String regex, Configuration conf) {
    conf.set(TOKEN_REGEX_KEY, regex);
  }

  /**
   * Sets the value splitting functionality. If this is set to <tt>false</tt>
   * then splitting will not occur. To set the regex use
   * {@link #setTokenRegex(String, Configuration)}
   * <p>
   * If not value is set, then the index will use the default value (
   * {@link #DEFAULT_IS_SPLITABLE}).
   * 
   * @param isSplitable If splitting should occur.
   * @param conf The configuration to set.
   */
  public static void setSplitable(boolean isSplitable, Configuration conf) {
    conf.setBoolean(SPLITABLE_TERM_KEY, isSplitable);
  }

  /**
   * Sets whether the index should lower case the terms.
   * <p>
   * If not value is set, then the index will use the default value (
   * {@link #DEFAULT_IS_LOWER_CASED}).
   * 
   * @param isLowerable If terms should be converted to lower case.
   * @param conf The configuration to set.
   */
  public static void setToLower(boolean isLowerable, Configuration conf) {
    conf.setBoolean(TO_LOWER_KEY, isLowerable);
  }

  /**
   * Gets the regex used to split the value. Uses the {@link #TOKEN_REGEX_KEY}
   * key and defaults to {@link #DEFAULT_TOKEN_REGEX}.
   * 
   * @return The regex string used to split values.
   */
  public String getTokenRegex() {
    return getConf().get(TOKEN_REGEX_KEY, DEFAULT_TOKEN_REGEX);
  }

  /**
   * Gets the boolean value that indicates if the value should be split. Uses
   * the {@link #SPLITABLE_TERM_KEY} key and defaults to
   * {@link #DEFAULT_IS_SPLITABLE}.
   * 
   * @return Splitable indicator.
   */
  public boolean getSplitable() {
    return getConf().getBoolean(SPLITABLE_TERM_KEY, DEFAULT_IS_SPLITABLE);
  }

  /**
   * Gets the boolean value that indicates if the value should be converted to
   * lower case. Uses the {@link #DEFAULT_IS_LOWER_CASED} key and defaults to
   * {@link #DEFAULT_IS_LOWER_CASED}.
   * 
   * @return Lower case indicator.
   */
  public boolean getToLower() {
    return getConf().getBoolean(TO_LOWER_KEY, DEFAULT_IS_LOWER_CASED);
  }

  @Override
  public void handlePut(Put put) {
    // Get the table adapter for this index
    TableAdapter indexTable = getIndexTable();

    /*
     * Create a new Put object by creating a new CKeyValue list containing the
     * terms from the values of the original key value list.
     */
    Iterable<CKeyValue> keyValues = put.getKeyValueList();
    List<CKeyValue> indexKeyValues = new ArrayList<CKeyValue>();
    for (CKeyValue keyValue : keyValues) {
      try {
        indexKeyValues.addAll(createTermList(keyValue));
      } catch (RuntimeException e) {
        // Log the error and return an empty list so that nothing is inserted.
        // Also, this implies that the length of the row ID is greater than
        // INT_MAX, which is a really really long row ID.
        LOG.error("Record row id length is too long to fit in a 4 byte array.");
        indexKeyValues = new ArrayList<CKeyValue>();
        break;
      }
    }

    // Create a new Put object with the indexed data
    Put indexPut = new Put(indexKeyValues);
    indexTable.put(indexPut);
  }

  @Override
  public SeekingCurrentIterator handleGet(byte[] indexRangeStart,
      byte[] indexRangeEnd) {

    // Get the table adapter for this index
    TableAdapter indexTable = getIndexTable();

    // update the end key to lookup the term properly (and inclusively)
    byte[] end;
    if (indexRangeEnd.length != 0) {
      // need to increment the end so it is inclusive of the end of the range
      end = Arrays.copyOf(indexRangeEnd, indexRangeEnd.length + 1);
      end[end.length - 1] = 1;
    }
    // if it covers the entire range, then we don't need to do anything
    else
      end = indexRangeEnd;

    // do the get
    Get get = new Get(new CRange(indexRangeStart, end));
    return new TermBasedIndexIterator(indexTable.get(get));
  }

  /**
   * This will construct the necessary row IDs to be inserted into the index
   * table. Creates terms based on the configuration settings.
   * 
   * @param keyValue The CKeyValue to index.
   * @return List of CKeyValues corresponding to the different terms.
   * @throws RuntimeException If the record row ID length takes up more than 4
   *         bytes
   */
  private List<CKeyValue> createTermList(CKeyValue keyValue)
      throws RuntimeException {
    // Get the configuration values
    boolean toSplit = getSplitable();
    boolean toLower = getToLower();
    String tokenRegex = getTokenRegex();

    // Get the value and lower case if necessary.
    String value = new String(keyValue.getValue(), Charset.forName("UTF-8"));
    if (toLower) {
      value = value.toLowerCase();
    }

    // Create the KeyValue list for the Index
    ArrayList<CKeyValue> indexKeyValueList = new ArrayList<CKeyValue>();

    // Get the record row id
    byte[] recordTableRowId = keyValue.getRowId();

    // Split the terms and create the index.
    if (toSplit) {
      try {
        String[] words = new String(value).split(tokenRegex);

        // Create the KeyValue for the new index value
        for (String word : words) {
          byte[] indexRowId;
          indexRowId = createIndexRowId(Bytes.toBytes(word), recordTableRowId);
          indexKeyValueList.add(new CKeyValue(indexRowId));
        }
      } catch (PatternSyntaxException e) {
        throw new RuntimeException("Invalid token regex for splitting terms.",
            e);
      }
    } else {
      byte[] indexRowId;
      indexRowId = createIndexRowId(Bytes.toBytes(value), recordTableRowId);
      indexKeyValueList.add(new CKeyValue(indexRowId));
    }

    return indexKeyValueList;
  }

  /**
   * Creates an index row id based on the provided term and row ID.
   * 
   * @param term The term to index.
   * @param recordRowId The row ID that corresponds to the term.
   * @return byte[] The index row ID.
   * @throws RuntimeException If the record row ID length takes up more than 4
   *         bytes
   */
  private static byte[] createIndexRowId(byte[] term, byte[] recordRowId)
      throws RuntimeException {

    // Initialize variables
    byte[] indexRowId = new byte[term.length + NUM_NULL_BYTES
        + recordRowId.length + NUM_RECORD_ID_LENGTH_BYTES];
    int destPos = 0;

    // How many null bytes we need to pad the length byte array
    byte[] rowIdLength = Bytes.toBytes(recordRowId.length);
    int lengthPadding = NUM_RECORD_ID_LENGTH_BYTES - rowIdLength.length;
    if (lengthPadding < 0) {
      throw new RuntimeException(
          "Record row id length is too long to fit in a 4 byte array");
    }

    // Copy to the index byte array
    System.arraycopy(term, 0, indexRowId, destPos, term.length);
    destPos += term.length;
    System
        .arraycopy(NULL_BYTES_PADDING, 0, indexRowId, destPos, NUM_NULL_BYTES);
    destPos += NUM_NULL_BYTES;
    System.arraycopy(recordRowId, 0, indexRowId, destPos, recordRowId.length);
    destPos += recordRowId.length;
    System.arraycopy(new byte[lengthPadding], 0, indexRowId, destPos,
        lengthPadding);
    destPos += lengthPadding;
    System.arraycopy(rowIdLength, 0, indexRowId, destPos, rowIdLength.length);

    return indexRowId;
  }

  /**
   * Parses an index row ID and returns the record row id. Follows the schema
   * provided in createTermList.
   * 
   * @param indexRowId The row ID for a term based index.
   * @return The row ID for the data table.
   * @throws NullPointerException if the indexRowId is null.
   * @throws RuntimeException if it wasn't able to parse the byte array.
   */
  private static byte[] parseIndexRowGetId(byte[] indexRowId)
      throws RuntimeException {
    if (indexRowId == null)
      throw new RuntimeException();

    // Storage for the length of the record row id
    byte[] recordRowIdLengthBytes = new byte[NUM_RECORD_ID_LENGTH_BYTES];

    // Get the starting position for extracting the row id length
    int lengthStartPos = indexRowId.length - NUM_RECORD_ID_LENGTH_BYTES;

    // Get the length of the value
    System.arraycopy(indexRowId, lengthStartPos, recordRowIdLengthBytes, 0,
        NUM_RECORD_ID_LENGTH_BYTES);
    int rowIdLength = Bytes.toInt(recordRowIdLengthBytes);

    // Get the starting position for the row id
    int rowIdStartPos = indexRowId.length
        - (NUM_RECORD_ID_LENGTH_BYTES + rowIdLength);

    // Extract the row id
    byte[] recordRowId = new byte[rowIdLength];
    try {
      System.arraycopy(indexRowId, rowIdStartPos, recordRowId, 0, rowIdLength);
    } catch (Exception e) {
      throw new RuntimeException("Error parsing the index row ID");
    }
    return recordRowId;
  }

  /**
   * This iterator wraps a SeekingCurrentIterator returned from a TableAdapter's
   * get method. It takes the the row ID from a Result object and parses out the
   * row ID for the data table.
   * 
   */
  public static class TermBasedIndexIterator extends DecoratingCurrentIterator {

    public TermBasedIndexIterator(SeekingCurrentIterator iterator) {
      super(iterator);
    }

    /**
     * Parses the Result from the wrapped iterator in order to extract the row
     * ID for the data table.
     * 
     * @return Result with the data row ID;
     */
    @Override
    public Result next() {
      // Get the result
      Result indexResult = super.next();
      byte[] indexRowId = indexResult.getRecordId();

      // Create the Result containing the data row ID
      byte[] dataRowId = TermBasedIndex.parseIndexRowGetId(indexRowId);
      Result dataResult = new Result(dataRowId);
      current = dataResult;

      return dataResult;
    }
  }
}

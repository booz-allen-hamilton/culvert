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
package com.bah.culvert.inmemory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.LexicographicByteArrayComparator;
import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Provides a thread safe implementation of an in-memory table.
 */
public class InMemoryTable extends TableAdapter {

  private final SortedMap<Bytes, InMemoryFamily> table = Collections
      .synchronizedSortedMap(new TreeMap<Bytes, InMemoryFamily>());

  public InMemoryTable() {
    super(new Configuration());
  }

  /**
   * Do the actual put
   * 
   * @param rowId to put
   * @param cf to put
   * @param cq to put
   * @param value to put
   */
  private void put(byte[] rowId, byte[] cf, byte[] cq, long timestamp,
      byte[] value) {
    InMemoryFamily family = this.table.get(new Bytes(rowId));

    // if there is no family stored, create one
    if (family == null) {
      family = new InMemoryFamily();
      this.table.put(new Bytes(rowId), family);
    }
    // and then store values in it
    family.put(cf, cq, timestamp, value);
  }

  @Override
  public void put(Put put) {
    Iterable<CKeyValue> keyValues = put.getKeyValueList();
    for (CKeyValue kv : keyValues) {
      kv = checkValues(kv);
      this.put(kv.getRowId(), kv.getFamily(), kv.getQualifier(),
          kv.getTimestamp(), kv.getValue());
    }
  }

  private CKeyValue checkValues(CKeyValue kv) {

    byte[] cf = kv.getFamily();
    byte[] cq = kv.getQualifier();
    byte[] value = kv.getValue();
    long timestamp = kv.getTimestamp();
    // make sure all the byte arrays have a value before we add them
    if (cf == null)
      cf = new byte[0];
    if (cq == null)
      cq = new byte[0];
    if (timestamp == CKeyValue.DEFAULT_TIME_STAMP)
      timestamp = System.currentTimeMillis();
    if (value == null)
      value = new byte[0];
    return new CKeyValue(kv.getRowId(), cf, cq, timestamp, value);
  }

  @Override
  public SeekingCurrentIterator get(Get get) {

    if (this.table.size() == 0)
      return new DecoratingCurrentIterator(new ArrayList<Result>(0).iterator());

    CRange range = get.getRange();

    // if the start is at the start of the table, store it
    Bytes start = range.getStart().length == 0 ? this.table.firstKey()
        : new Bytes(range.getStart());

    // if the end is at the end of the table, store it
    Bytes end = range.getEnd().length == 0 ? this.table.lastKey() : new Bytes(
        range.getEnd());

    List<Result> results = new ArrayList<Result>();
    SortedMap<Bytes, InMemoryFamily> submap;

    // if just getting a single row
    if (LexicographicBytesComparator.INSTANCE.compare(start.getBytes(),
        end.getBytes()) == 0) {
      InMemoryFamily family = this.table.get(start);
      // if the family is not present, return an empty iterator (short circuit)
      if (family == null)
        return new DecoratingCurrentIterator(
            ((List<Result>) Collections.EMPTY_LIST).iterator());

      // otherwise it is present, and set the submap
      submap = new TreeMap<Bytes, InMemoryFamily>();
      submap.put(start, family);
    }

    // getting more than a single row
    else {
      submap = new TreeMap<Bytes, InMemoryFamily>(this.table.subMap(start, end));

      // remove the start key if the range is not start inclusive
      if (!range.isStartInclusive())
        submap.remove(start);

      // since subMap is end EXCLUSIVE we need to check the last value too
      if (range.isEndInclusive()) {
        // if the end key is valid, add it to the submap
        InMemoryFamily endFamily = this.table.get(end);
        if (endFamily != null) {
          submap.put(end, endFamily);
        }
      }
    }
    return new DecoratingCurrentIterator(filteredLookup(submap, get).iterator());
  }

  /**
   * Filter the contents of the sub-table based on the columns in the get
   * @param table to filter
   * @param get to filter by
   * @return results gotten from the table
   */
  private static Collection<Result> filteredLookup(
      SortedMap<Bytes, InMemoryFamily> table, Get get) {

    List<Result> results = new ArrayList<Result>();
    // now the submap is setup with the potential rows
    // so we do the lookup
    List<CColumn> columns = get.getColumns();

    // if there are no columns, just get all
    if (columns.size() == 0) {
      for (Entry<Bytes, InMemoryFamily> row : table.entrySet()) {
        List<CKeyValue> values = new ArrayList<CKeyValue>();
        for (Entry<Bytes, InMemoryQualifier> family : row.getValue().map
            .entrySet()) {
          for (Entry<Bytes, List<InMemoryTimestampedValue>> qualifier : family
              .getValue().map.entrySet()) {
            InMemoryTimestampedValue min = null;
            // get the most recent value
            for (InMemoryTimestampedValue value : qualifier.getValue()) {
              if (min == null || value.getTimestamp() > min.getTimestamp())
                min = value;
            }
            if (min != null)
              values.add(new CKeyValue(row.getKey().getBytes(), family.getKey()
                  .getBytes(), qualifier.getKey().getBytes(), min
                  .getTimestamp(), min.getBytes()));
          }
        }
        if (values.size() > 0)
          results.add(new Result(values));
      }
      return results;
    }

    // there are CFs specified, so we need to do a filtered lookup
    for (Entry<Bytes, InMemoryFamily> row : table.entrySet()) {
      List<CKeyValue> values = new ArrayList<CKeyValue>();
      // loop through all the families
      for (Entry<Bytes, InMemoryQualifier> family : row.getValue().map
          .entrySet()) {
        // if the family is valid
        List<CColumn> matches = matchingColumnsWithFamily(columns, family
            .getKey().getBytes());
        if (matches != null)
          // iterate through its qualifiers
          for (Entry<Bytes, List<InMemoryTimestampedValue>> qualifier : family
              .getValue().map.entrySet()) {
            // if the CQ is valid
            if (columnHasQualifier(matches, qualifier.getKey().getBytes())) {
              InMemoryTimestampedValue min = null;
              // get the most recent value
              for (InMemoryTimestampedValue value : qualifier.getValue()) {
                if (min == null || value.getTimestamp() > min.getTimestamp())
                  min = value;
              }
              if (min != null)
                values.add(new CKeyValue(row.getKey().getBytes(), family
                    .getKey().getBytes(), qualifier.getKey().getBytes(), min
                    .getTimestamp(), min.getBytes()));
            }
          }
      }
      if (values.size() > 0)
        results.add(new Result(values));
    }
    return results;
  }

  /**
   * Determine if any of the specified columns has the given qualifier.
   * <p>
   * Zero-length qualifiers in the columns will match any qualifier.
   * @param columns
   * @param qualifier
   * @return <tt>true</tt> if the qualifier is present in any of the columns.
   *         <tt>false</tt> otherwise
   */
  private static boolean columnHasQualifier(List<CColumn> columns,
      byte[] qualifier) {
    LexicographicByteArrayComparator comparator = LexicographicByteArrayComparator.INSTANCE;
    for (CColumn column : columns) {
      byte[] qual = column.getColumnQualifier();
      if (qual.length == 0 || comparator.compare(qual, qualifier) == 0)
        return true;
    }
    return false;
  }

  /**
   * Get the list of columns that have a matching column family
   * @param bytes
   * @return the matching columns
   */
  private static List<CColumn> matchingColumnsWithFamily(List<CColumn> columns,
      byte[] bytes) {
    List<CColumn> matches = new ArrayList<CColumn>();
    LexicographicByteArrayComparator comparator = LexicographicByteArrayComparator.INSTANCE;
    for (CColumn column : columns) {
      if (comparator.compare(column.getColumnFamily(), bytes) == 0)
        matches.add(column);
    }
    return matches;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> List<T> remoteExec(byte[] startKey, byte[] endKey,
      Class<? extends RemoteOp<T>> remoteCallable, Object... array) {
    try {
      RemoteOp<T> op = remoteCallable.newInstance();
      LocalTableAdapter adapter = new InMemoryLocalTableAdapter(this);
      op.setConf(getConf());
      op.setLocalTableAdapter(adapter);
      return Arrays.asList(op.call(array));
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return get the underlying map for this table
   */
  SortedMap<Bytes, InMemoryFamily> getTable() {
    return this.table;
  }

  @Override
  public byte[][] getStartKeys() {
    if (this.table.size() == 0)
      return new byte[0][];
    byte[][] start = new byte[1][];
    start[0] = this.table.firstKey().getBytes();
    return start;
  }

  @Override
  public byte[][] getEndKeys() {
    if (this.table.size() == 0)
      return new byte[0][];
    byte[][] end = new byte[1][];
    end[0] = this.table.lastKey().getBytes();
    return end;
  }

  @Override
  public List<String> getHosts() {
    return Arrays.asList("localhost");
  }
}

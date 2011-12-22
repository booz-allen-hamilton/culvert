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
package com.bah.culvert.constraints;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.write.Handler;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;
import com.google.common.collect.Lists;

/**
 * Constraints are used to implement various types of relational algebra.
 * <p>
 * They may be used to describe set operations, joins, selection, and
 * projection, or any other operations one wants to implement in the Culvert
 * framework.
 */
public abstract class Constraint implements Writable {

  private static final byte[] EMPTY_START_ROW = new byte[0];

  /**
   * Get an {@link Iterator} over the result sets by applying <tt>this</tt> to
   * the table
   * 
   * @return a {@link SeekingCurrentIterator} over the result set
   */
  public abstract SeekingCurrentIterator getResultIterator();

  /**
   * Progress the iterator until the specified key is found.
   * <p>
   * If the specified key is not present, seeks to the key after the specified
   * key.
   * 
   * @param key to search for
   * @param resultIterator iterator from the specified key.
   * @return <tt>true</tt> if the key is found, <tt>false</tt> otherwise.
   */
  protected static boolean progressUntil(byte[] key,
      SeekingCurrentIterator resultIterator) {
    if (!resultIterator.isMarkedDoneWith()) {
      // load up the current result from the iterator
      Result current = resultIterator.current();
      byte[] currentKey = current == null ? EMPTY_START_ROW : current
          .getRecordId();

      int comparison = Bytes.compareTo(currentKey, key);
      // first check if we're already there.
      if (comparison == 0) {
        return true;
      } else if (comparison < 0)
      // if not and we're less, then seek and check again
      {
        resultIterator.seek(key);
        Result seeked = resultIterator.current();
        // if we went to the end, we now get a null
        if (seeked == null) {
          return false;
        }
        // if the current key is greater, we overshot
        else if ((comparison = Bytes.compareTo(seeked.getRecordId(), key)) > 0) {
          return false;
        }
        // if its equal we're good (we assume seek works)
        else {
          assert (comparison == 0);
          return true;
        }
      } else
      // otherwise we've overshot it and its not here.
      {
        return false;
      }
    }

    return false;
  }

  /**
   * Dump the results of this constraint to a table. Provides an opportunity to
   * implement parallel dumps if the operation can be represented in parallel.
   * <p>
   * Blocks until the dump is complete. If you are dumping to a table that
   * you're watching, you should use asynchronous handlers to execute the dump
   * concurrently with whatever other process you're coordinating.
   * <p>
   * The default implementation of this simply dumps the results of the
   * iterators to a table.
   * 
   * @param outputTable The table that this constraint should dump to.
   */
  public void writeToTable(TableAdapter outputTable) {
    SeekingCurrentIterator iterator = getResultIterator();
    while (iterator.hasNext() == true) {
      Result res = iterator.next();
      List<CKeyValue> values = Lists.newArrayList(res.getKeyValues());
      if (values.size() != 0) {
        Put put = new Put(values);
        outputTable.put(put);
      }
    }
  }

  /**
   * Dump the results of this constraint to a table. Provides an opportunity to
   * implement parallel dumps if the operation can be represented in parallel.
   * <p>
   * Blocks until the dump is complete. If you are dumping to a table that
   * you're watching, you should use asynchronous handlers to execute the dump
   * concurrently with whatever other process you're coordinating.
   * <p>
   * The default implementation of this simply dumps the results of the
   * iterators to a table.
   * 
   * @param outputTable The table that this constraint should dump to.
   * @param filter To handle the transformation from the primary table to the
   *        output table
   */
  public void writeToTable(TableAdapter outputTable, Handler filter) {
    SeekingCurrentIterator iterator = getResultIterator();
    while (iterator.hasNext() == true) {
      Result res = iterator.next();
      List<CKeyValue> values = new ArrayList<CKeyValue>();

      // apply the filter to the result
      values.addAll(filter.apply(res));

      // write the put to the table
      if (values.size() != 0) {
        Put put = new Put(values);
        outputTable.put(put);
      }

    }
  }

  /**
   * Read the constraint from the stream
   * @param in to read the constraint from
   * @return specified {@link Constraint}
   * @throws IOException if the constraint could not be created or read
   */
  public static Constraint readFromStream(DataInput in) throws IOException {
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);
    ow.readFields(in);
    return (Constraint) ow.get();
  }

  /**
   * Write a given constraint to the output stream
   * @param constraint to write
   * @param out to write to
   * @throws IOException on failure to write
   */
  public static void write(Constraint constraint, DataOutput out)
      throws IOException {
    new ObjectWritable(constraint).write(out);
  }
}

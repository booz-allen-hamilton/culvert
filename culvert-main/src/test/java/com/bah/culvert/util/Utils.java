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
package com.bah.culvert.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;

/**
 * Culvert testing utilities
 */
public final class Utils {

  private Utils() {
  }

  /**
   * Test that the specified iterator has the expected number of results and
   * values
   * @param iter
   * @param expectedRows expected total number of results
   * @param expectedValues expected TOTAL number of rows for ALL results
   */
  public static void testResultIterator(Iterator<Result> iter,
      int expectedRows, int expectedValues) {
    testResultIterator(iter, expectedRows, expectedValues, false);
  }

  /**
   * Test that the specified iterator has the expected number of results and
   * values
   * @param iter
   * @param expectedRows expected total number of results
   * @param expectedValues expected TOTAL number of rows for ALL results
   * @param print <tt>true</tt> to print out all the received rows. Useful for
   *        debugging
   */
  public static void testResultIterator(Iterator<Result> iter,
      int expectedRows, int expectedValues, boolean print) {
    if (expectedRows > 0)
      assertTrue(iter.hasNext());
    int results = 0;
    int values = 0;
    while (iter.hasNext()) {
      Result r = iter.next();
      results++;
      for (CKeyValue kv : r.getKeyValues()) {
        if (print)
          System.out.println(kv);

        values++;
      }
    }
    assertEquals("Number of rows differed from expected number of rows",
        expectedRows, results);
    assertEquals("Number of values differed from expected number of values",
        expectedValues, values);
  }

  /**
   * Checks if the writable will successfully read and write using
   * {@link Writable}'s methods.
   * @param writeable
   * @return The instance read in using
   *         {@link Writable#readFields(java.io.DataInput)}. Can be used for
   *         checking equality.
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   */
  public static Writable testReadWrite(Writable writeable)
      throws InstantiationException, IllegalAccessException, IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
      writeable.write(dos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Writable inst = writeable.getClass().newInstance();
    inst.readFields(dis);
    return inst;
  }

  /**
   * Test that the writable both serializes successfully and that the read
   * version will compare equivalently to the original.
   * <p>
   * NOTE: requires that the sent object is comparable to itself
   * @param <T> Class of the writable
   * @param writable to test
   * @return the read in version of the original value
   * @throws IOException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @SuppressWarnings("unchecked")
  public static <T> WritableComparable<T> testReadWrite(
      WritableComparable<T> writable) throws InstantiationException,
      IllegalAccessException, IOException {
    WritableComparable<T> inst = (WritableComparable<T>) testReadWrite((Writable) writable);
    assertEquals(0, writable.compareTo((T) inst));
    return inst;
  }
}

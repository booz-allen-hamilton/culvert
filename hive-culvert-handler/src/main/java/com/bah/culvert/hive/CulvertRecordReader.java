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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

import com.bah.culvert.data.Result;

/**
 * Read a row from a culvert data table using the specified value in the index
 * table.
 */
public class CulvertRecordReader implements RecordReader<NullWritable, Result> {

  private static final Log LOG = LogFactory.getLog(CulvertRecordReader.class);
  private final Iterator<Result> indexIterator;
  private long currentKeyCount = 0;

  /**
   * Create a Culvert record reader that uses the indexes in the split to read
   * records from the specified data table
   * 
   * @param split to read index information
   * @throws IOException on failure to read from the index table
   */
  public CulvertRecordReader(CulvertInputSplit split) throws IOException {
    // actually do the get on the indicies
    this.indexIterator = split.getQueryConstraint().getResultIterator();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#close()
   */
  @Override
  public void close() throws IOException {
    // NOOP
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  @Override
  public Result createValue() {
    return new Result();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#getPos()
   */
  @Override
  public long getPos() throws IOException {
    return currentKeyCount;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#getProgress()
   */
  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object,
   * java.lang.Object)
   */
  @Override
  public boolean next(NullWritable key, Result row) throws IOException {
    // if there are more indexed rows, we want to get them
    if (indexIterator.hasNext()) {
      // update the number of keys gotten
      this.currentKeyCount++;

      // don't think that we need to actually handle getting the value - this
      // should be taken care of by the input splits
      Result newResult = indexIterator.next();
      // set the data row id
      row.copy(newResult);
      return indexIterator.hasNext();
    } else {
      throw new IOException("Out of results.");
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }
}
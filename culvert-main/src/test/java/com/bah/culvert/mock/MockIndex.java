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
package com.bah.culvert.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.data.index.Index;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;

public class MockIndex extends Index {

  public MockIndex() {
    // no-arg constructor.
  }

  public MockIndex(String name, int id, boolean isSplitable,
      byte[] columnFamily, byte[] columnQualifier, String primaryTable,
      String indexTable) {
    super();
    Configuration conf = new Configuration();
    super.setConf(conf);

    // Set the configuration
    Index.setIndexName(name, conf);
    Index.setColumnFamily(columnFamily, conf);
    Index.setColumnQualifier(columnQualifier, conf);
    Index.setPrimaryTable(primaryTable, conf);
    Index.setIndexTable(indexTable, conf);

  }

  @Override
  public void handlePut(Put put) {
    // right now, nooop.
  }

  @Override
  public SeekingCurrentIterator handleGet(byte[] indexRangeStart,
      byte[] indexRangeEnd) {
    // right now, noop.
    return null;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // noop

  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // noop

  }

}

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

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.bah.culvert.Client;
import com.bah.culvert.transactions.Put;

/**
 * Hive record writer that outputs data to a culvert table.
 */
public class CulvertRecordWriter implements RecordWriter,
    org.apache.hadoop.mapred.RecordWriter<BytesWritable, Put> {

  /**
   * The client to use to store puts.
   */
  private final Client client;
  /**
   * The table to output data to.
   */
  private final String table;
  private final Progressable progress;

  /**
   * Create a writer to output information to a culvert table.
   * 
   * @param client The client to use.
   * @param table The table to output records to using the client.
   * @param progress
   */
  public CulvertRecordWriter(Client client, String table, Progressable progress) {
    this.client = client;
    this.table = table;
    this.progress = progress;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter#write(org.
   * apache.hadoop.io.Writable)
   */
  @Override
  public void write(final Writable w) throws IOException {
    if (!(w instanceof Put)) {
      throw new RuntimeException(
          "CulverRecordWriters only support culvert Put "
              + "operations. Instead got instance of " + w.getClass());
    }
    client.put(table, (Put) w);
    progress.progress();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter#close(boolean)
   */
  @Override
  public void close(boolean abort) throws IOException {
    /*
     * noop... perhaps we should try to support some sort of snapshotting at
     * some point via bulk operations?
     */
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordWriter#write(java.lang.Object,
   * java.lang.Object)
   */
  @Override
  public void write(BytesWritable key, Put value) throws IOException {
    this.write(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.RecordWriter#close(org.apache.hadoop.mapred.Reporter
   * )
   */
  @Override
  public void close(Reporter reporter) throws IOException {
    // currently a no-op.
  }

}
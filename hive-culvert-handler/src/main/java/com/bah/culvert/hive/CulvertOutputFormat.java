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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.bah.culvert.Client;
import com.bah.culvert.transactions.Put;

/**
 * Output format for writing values to a culvert table.
 * <p>
 * This implementation is primarily oriented towards HIVE interaction.
 */
@SuppressWarnings("deprecation")
public class CulvertOutputFormat implements OutputFormat<BytesWritable, Put>,
    HiveOutputFormat<BytesWritable, Put> {

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hadoop.hive.ql.io.HiveOutputFormat#getHiveRecordWriter(org.apache
   * .hadoop.mapred.JobConf, org.apache.hadoop.fs.Path, java.lang.Class,
   * boolean, java.util.Properties, org.apache.hadoop.util.Progressable)
   */
  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
      boolean isCompressed, Properties tableProperties, Progressable progress)
      throws IOException {
    if (!Put.class.isAssignableFrom(valueClass)) {
      throw new IllegalArgumentException(
          "Culvert output formats can only handle culvert "
              + "Puts, instead got " + valueClass.getName());
    }
    /*
     * get the client from the table props... the client thats configured in the
     * jobconf is either for input or doesn't exist (if the input for the job
     * isn't culvert).
     */
    Configuration clientConf = CulvertHiveUtils
        .getCulvertConfiguration(tableProperties);
    Client culvertClient = new Client(clientConf);
    String culvertTable = CulvertHiveUtils.getCulvertTable(tableProperties);
    return new CulvertRecordWriter(culvertClient, culvertTable, progress);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.hadoop
   * .fs.FileSystem, org.apache.hadoop.mapred.JobConf, java.lang.String,
   * org.apache.hadoop.util.Progressable)
   */
  @Override
  public RecordWriter<BytesWritable, Put> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    /*
     * This is part of the hadoop output format, which we don't really use.
     */
    Client culvertClient = new Client(job);
    String table = CulvertHiveUtils.getCulvertTable(job);
    return new CulvertRecordWriter(culvertClient, table, progress);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache.hadoop
   * .fs.FileSystem, org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws IOException {
    /*
     * This is part of the hadoop output format, which we don't really use.
     */
    Client client = new Client(job);
    if (!client.verify()) {
      throw new IOException("Unable to verify culvert client: " + client);
    }
  }

}

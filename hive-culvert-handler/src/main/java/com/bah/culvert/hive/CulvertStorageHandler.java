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

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * Handles Culvert storage operations for HIVE.
 */
public class CulvertStorageHandler extends DefaultStorageHandler implements
    HiveStoragePredicateHandler {

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler#getInputFormatClass
   * ()
   */
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return CulvertInputFormat.class;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler#getOutputFormatClass
   * ()
   */
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return CulvertOutputFormat.class;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler#getSerDeClass()
   */
  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return CulvertSerDe.class;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler#
   * decomposePredicate(org.apache.hadoop.mapred.JobConf,
   * org.apache.hadoop.hive.serde2.Deserializer,
   * org.apache.hadoop.hive.ql.plan.ExprNodeDesc)
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
      Deserializer deserializer, ExprNodeDesc predicate) {
    CulvertPredicateHandlerDelegate delegate = new CulvertPredicateHandlerDelegate();
    return delegate.decomposePredicate(jobConf, deserializer, predicate);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler#getMetaHook()
   */
  @Override
  public HiveMetaHook getMetaHook() {
    return new CulvertMetaHook();
  }

}
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
package com.bah.culvert.tableadapters;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import com.bah.culvert.adapter.RemoteOp;

public class HBaseCulvertCoprocessorEndpoint extends BaseEndpointCoprocessor
    implements HBaseCulvertCoprocessorProtocol {

  @Override
  public <T> T call(Class<? extends RemoteOp<T>> remoteCallable,
      Configuration configuration, List<Object> args) {
    HBaseLocalTableAdapter tableAdapter = new HBaseLocalTableAdapter(this);
    RemoteOp<T> op = null;
    try {
      op = remoteCallable.newInstance();
      op.setConf(configuration);
      op.setLocalTableAdapter(tableAdapter);
      return op.call(args.toArray());
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
  }
}
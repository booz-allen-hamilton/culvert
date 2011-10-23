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
package com.bah.culvert.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;

public class MockTableAdapter extends TableAdapter {

  public MockTableAdapter() {
    super(new Configuration());
  }

  @Override
  public void put(Put put) {
    // NOOP
  }

  @Override
  public <T> List<T> remoteExec(byte[] startKey, byte[] endKey,
      Class<? extends RemoteOp<T>> remoteCallable,
          Object... array) {
    // currently a no-op.
    return null;
  }

  @Override
  public SeekingCurrentIterator get(Get get) {
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

  @Override
  public byte[][] getStartKeys() {
    return new byte[0][];
  }

  @Override
  public byte[][] getEndKeys() {
    return new byte[0][];
  }

  @Override
  public List<String> getHosts() {
    return Arrays.asList("localhost");
  }

}

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
package com.bah.culvert.transactions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.bah.culvert.data.CKeyValue;

/**
 * Test the Put transaction
 */
public class TestPut {
  @Test
  public void testPutWrite() {

    CKeyValue ckv = new CKeyValue(new byte[] { 0, 0, 0, 8 });
    List<CKeyValue> list = new ArrayList<CKeyValue>();
    list.add(ckv);
    Put p1 = new Put(list);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      p1.write(dos);
    } catch (IOException e) {
      Assert.fail();
    }

    Assert.assertArrayEquals(new byte[] { 0, 0, 0, 1, 4, 0, 0, 0, 0, 0, 0, 8,
        -120, 127, -1, -1, -1, -1, -1, -1, -1 }, baos.toByteArray());
  }

  @Test
  public void testPutRead() {
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] { 0, 0, 0,
        1, 4, 0, 0, 0, 0, 0, 0, 8, -120, 127, -1, -1, -1, -1, -1, -1, -1 });
    DataInputStream dis = new DataInputStream(bais);
    Put p1 = new Put();
    try {
      p1.readFields(dis);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Assert.assertArrayEquals(new byte[] { 0, 0, 0, 8 }, p1.getKeyValueList()
        .iterator().next().getRowId());

  }
}

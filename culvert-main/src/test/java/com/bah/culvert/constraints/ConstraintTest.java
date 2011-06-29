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
package com.bah.culvert.constraints;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Constraint.class)
public class ConstraintTest {

  @Test
  public void testNaiveDump() {
    Constraint constraint = PowerMock.createPartialMock(Constraint.class,
        "getResultIterator");
    List<Result> resList = new ArrayList<Result>();
    for (char c = 'a'; c <= 'z'; c++) {
      List<CKeyValue> ckv = new ArrayList<CKeyValue>();
      for (int i = 1; i < 10; i++) {
        for (char C = 'A'; C <= 'Z'; C++) {
          ckv.add(new CKeyValue(Bytes.toBytes(String.format("%c", c)), Bytes
              .toBytes(String.format("%d", i)), Bytes.toBytes(String.format(
              "%c", C))));
        }
      }
      resList.add(new Result(ckv));
    }
    EasyMock.expect(constraint.getResultIterator()).andReturn(
        new DecoratingCurrentIterator(resList.iterator()));
    TableAdapter dumpTable = EasyMock.createMock(TableAdapter.class);
    for(Result res : resList){
      dumpTable.put(new Put(res.getKeyValues()));
    }
    PowerMock.replayAll(dumpTable);
    constraint.dumpToTable(dumpTable, null);
    PowerMock.verifyAll();
  }
  
  private static class ExposingConstraint extends Constraint{

    @Override
    public SeekingCurrentIterator getResultIterator() {
      return null;
    }
    
    public static boolean progressUntil(byte[] key, SeekingCurrentIterator resultIterator){
      return Constraint.progressUntil(key, resultIterator);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      
    }

  }

  @Test
  public void testCanProgressExists(){
    List<Result> resList = new ArrayList<Result>();
    for (char c = 'a'; c <= 'z'; c++) {
      List<CKeyValue> ckv = new ArrayList<CKeyValue>();
      for (int i = 1; i < 10; i++) {
        for (char C = 'A'; C <= 'Z'; C++) {
          ckv.add(new CKeyValue(Bytes.toBytes(String.format("%c", c)), Bytes
              .toBytes(String.format("%d", i)), Bytes.toBytes(String.format(
              "%c", C))));
        }
      }
      resList.add(new Result(ckv));
    }
    SeekingCurrentIterator ci = new DecoratingCurrentIterator(resList.iterator());
    Assert.assertTrue(ExposingConstraint.progressUntil(Bytes.toBytes("c"), ci));
  }
  
  @Test
  public void testCanProgressDoesntExist(){
    List<Result> resList = new ArrayList<Result>();
    for (char c = 'a'; c <= 'z'; c++) {
      List<CKeyValue> ckv = new ArrayList<CKeyValue>();
      for (int i = 1; i < 10; i++) {
        for (char C = 'A'; C <= 'Z'; C++) {
          ckv.add(new CKeyValue(Bytes.toBytes(String.format("%c", c)), Bytes
              .toBytes(String.format("%d", i)), Bytes.toBytes(String.format(
              "%c", C))));
        }
      }
      resList.add(new Result(ckv));
    }
    SeekingCurrentIterator ci = new DecoratingCurrentIterator(resList.iterator());
    Assert.assertFalse(ExposingConstraint.progressUntil(Bytes.toBytes("~"), ci));
  }

  
}

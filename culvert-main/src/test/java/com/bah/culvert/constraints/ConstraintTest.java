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
package com.bah.culvert.constraints;

import static org.junit.Assert.assertFalse;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.write.Handler;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.test.Utils;
import com.bah.culvert.transactions.Get;
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
    TableAdapter dumpTable = new InMemoryTable();
    PowerMock.replayAll();
    constraint.writeToTable(dumpTable);
    PowerMock.verifyAll();
  }

  @Test
  public void testComplexDump() {
    Constraint constraint = PowerMock.createPartialMock(Constraint.class,
        "getResultIterator");
    List<Result> resList = new ArrayList<Result>();
    List<CKeyValue> ckv = new ArrayList<CKeyValue>();
    for (int i = 0; i < 10; i++) {
      ckv.add(new CKeyValue(Bytes.toBytes(String.format("%d", i)), Bytes
          .toBytes("cf"), "cq".getBytes()));
    }
    resList.add(new Result(ckv));

    EasyMock.expect(constraint.getResultIterator()).andReturn(
        new DecoratingCurrentIterator(resList.iterator()));
    TableAdapter dumpTable = new InMemoryTable();
    class DropFilter extends Handler {

      @Override
      public List<CKeyValue> apply(Result write) {
        return Collections.EMPTY_LIST;
      }

    }

    // test an empty dump
    PowerMock.replayAll();
    constraint.writeToTable(dumpTable, new DropFilter());
    PowerMock.verifyAll();
    Iterator<Result> results = dumpTable.get(new Get(new CRange(new byte[0])));
    assertFalse(results.hasNext());
    PowerMock.resetAll();

    // test a dump that generates multiple keys
    EasyMock.expect(constraint.getResultIterator()).andReturn(
        new DecoratingCurrentIterator(resList.iterator()));
    dumpTable = new InMemoryTable();

    // this is a fairly contrived example, but does the work
    class MultiplicateHandler extends Handler {

      @Override
      public List<CKeyValue> apply(Result write) {
        if (!write.getKeyValues().iterator().hasNext())
          return Collections.EMPTY_LIST;
        List<CKeyValue> newKvs = new ArrayList<CKeyValue>();

        for (CKeyValue kv : write.getKeyValues())
          newKvs.add(new CKeyValue(kv.getRowId(), Bytes.lexIncrement(kv
              .getFamily()), "add".getBytes()));

        newKvs.addAll(write.getKeyValues());
        return newKvs;
      }
    }
    replayAll();
    constraint.writeToTable(dumpTable, new MultiplicateHandler());
    results = dumpTable.get(new Get(new CRange(new byte[0])));
    Utils.testResultIterator(results, 10, 20, false);
    verifyAll();
  }

  private static class ExposingConstraint extends Constraint {

    @Override
    public SeekingCurrentIterator getResultIterator() {
      return null;
    }

    public static boolean progressUntil(byte[] key,
        SeekingCurrentIterator resultIterator) {
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
  public void testCanProgressExists() {
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
    SeekingCurrentIterator ci = new DecoratingCurrentIterator(
        resList.iterator());
    Assert.assertTrue(ExposingConstraint.progressUntil(Bytes.toBytes("c"), ci));
  }

  @Test
  public void testCanProgressDoesntExist() {
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
    SeekingCurrentIterator ci = new DecoratingCurrentIterator(
        resList.iterator());
    Assert
        .assertFalse(ExposingConstraint.progressUntil(Bytes.toBytes("~"), ci));
  }

}

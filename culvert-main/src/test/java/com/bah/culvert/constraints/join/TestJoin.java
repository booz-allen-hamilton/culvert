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
package com.bah.culvert.constraints.join;

import org.junit.Test;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.Join;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.inmemory.InMemoryTable;
import com.bah.culvert.mock.MockConstraint;
import com.bah.culvert.util.Utils;


public class TestJoin {

  @Test
  public void testReadWrite() throws Exception
  {
    MockJoin mj = new MockJoin(new InMemoryDB(), new InMemoryTable(),
        new MockConstraint(), new CColumn(new byte[] { 1 }), "right table");
    Utils.testReadWrite(mj);
  }
  
  
  public static class MockJoin extends Join {
    public MockJoin()
    {
      
    }
    
    public MockJoin(DatabaseAdapter db, TableAdapter leftTable,
        Constraint left,
      CColumn leftColumn, String rightTable) {
      super(db, leftTable, left, leftColumn,rightTable); 
    }
 
  @Override
  protected void doRemoteOperation(TableAdapter outputTable,
      byte[] rightOutputColumn) {
    // NOOP

  }
  }

}

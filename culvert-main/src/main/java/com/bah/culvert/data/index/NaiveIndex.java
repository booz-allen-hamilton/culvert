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
package com.bah.culvert.data.index;

import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.filter.KeyOnlyFilter;
import com.bah.culvert.data.CRange;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;

/**
 * The NaiveIndex does nothing but get the row IDs for the data table. It should
 * take an empty byte array for both the start and end ranges. Additionally, it
 * assumes that the index table is actually a TableAdapter to the data table.
 * 
 */
public class NaiveIndex extends Index {

  @Override
  public void handlePut(Put put) {
    // The NaiveIndex should not be inserting data
  }

  @Override
  public SeekingCurrentIterator handleGet(byte[] indexRangeStart,
      byte[] indexRangeEnd) {

    TableAdapter dataTable = getIndexTable();
    
    //Just need to return a SelectRowId on the rowId and use its getResultsIterator
    //to return the row IDs
    KeyOnlyFilter selectRowId = new KeyOnlyFilter(dataTable, new CRange(indexRangeStart, indexRangeEnd));
    return selectRowId.getResultIterator();
  }

}

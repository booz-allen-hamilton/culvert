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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Get;

/**
 * Adapter for the local hbase table when doing a {@link RemoteOp}
 */
public class HBaseLocalTableAdapter extends LocalTableAdapter {
  /**
   * Reference to the BaseEndpointCoprocessor so that we have access to the
   * environment.
   */
  private final BaseEndpointCoprocessor ENDPOINT;

  public HBaseLocalTableAdapter(BaseEndpointCoprocessor endpoint) {
    this.ENDPOINT = endpoint;
  }

  @Override
  public Iterator<Result> get(Get get) {
    // Get the range list
    List<CRange> culvertRangeList = get.getRanges();

    // Only scan the tables if we have a range
    if (culvertRangeList.size() > 0) {

      List<Result> results = new ArrayList<Result>();

      // Get the reference to the HRegion
      HRegion hregion = null;
      try {
        hregion = ((RegionCoprocessorEnvironment) this.ENDPOINT
            .getEnvironment()).getRegion();
      } catch (Exception e) {
        throw new RuntimeException(this.getTableName()
            + " Unable to get HRegion", e);
      }
      try {
        // Loop through the ranges
        for (CRange cr : culvertRangeList) {
          Scan scan = new Scan(cr.getStart(), cr.getEnd());
          // add columns to the scanner
          for (CColumn cc : get.getColumns()) {
            scan.addColumn(cc.getColumnFamily(), cc.getColumnQualifier());
          }
          InternalScanner scanner = hregion.getScanner(scan);
          try {
            List<KeyValue> curVals = new ArrayList<KeyValue>();
            boolean hasMore = false;
            do {
              curVals.clear();
              hasMore = scanner.next(curVals);
              KeyValue kv = curVals.get(0);
              results.add(new Result(kv.getRow(), new CKeyValue(kv.getKey(), kv
                  .getFamily(), kv.getQualifier(), kv.getTimestamp(), kv
                  .getValue())));
            } while (hasMore);
          } finally {
            scanner.close();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(this.getTableName()
            + " Failed to iterate through table", e);
      }

      return results.iterator();
    }
    // There were no ranges in the list, so return an empty iterator
    return EmptyIterator.INSTANCE;
  }

  @Override
  public byte[] getStartKey() {
    // Get the reference to the HRegion
    HRegion hregion = null;
    try {
      hregion = ((RegionCoprocessorEnvironment) ENDPOINT.getEnvironment())
          .getRegion();
    } catch (Exception e) {
      throw new RuntimeException(getTableName() + " Unable to get HRegion", e);
    }

    return hregion.getStartKey();
  }

  @Override
  public byte[] getEndKey() {
    // Get the reference to the HRegion
    HRegion hregion = null;
    try {
      hregion = ((RegionCoprocessorEnvironment) ENDPOINT.getEnvironment())
          .getRegion();
    } catch (Exception e) {
      throw new RuntimeException(getTableName() + " Unable to get HRegion", e);
    }

    return hregion.getEndKey();
  }
}
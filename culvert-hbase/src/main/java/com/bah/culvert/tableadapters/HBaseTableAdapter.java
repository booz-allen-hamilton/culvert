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
package com.bah.culvert.tableadapters;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.bah.culvert.adapter.RemoteOp;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.DecoratingCurrentIterator;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Interact with an HBase table through Culvert
 */
public class HBaseTableAdapter extends TableAdapter {

  @SuppressWarnings("synthetic-access")
  private static final HResultConverter resultConverter = new HResultConverter();

  private HTable table;

  /**
   * Default column to use when a table has not been created with an columns
   */
  public static byte[] DEFAULT_COLUMN = "_DC".getBytes();

  /**
   * Create a connection to an hbase table.
   * <p>
   * It is recommended to reuse the same configuration for all table adapters
   * connecting the the same HBase instance to ensure that they all use the same
   * {@link HConnection} instance.
   * @param conf for culvert specific information and hbase connection
   *        information. This configuration is cloned to resuse is not an issue.
   */
  public HBaseTableAdapter(Configuration conf) {
    super(conf);
    // Check to see if the Table is available
    try {
      this.table = new HTable(conf, getTableName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Put put) {

    // Iterator through put's KeyValue list and create HBase puts
    Iterable<CKeyValue> keyValueList = put.getKeyValueList();
    List<org.apache.hadoop.hbase.client.Put> puts = new ArrayList<org.apache.hadoop.hbase.client.Put>();
    for (CKeyValue keyValue : keyValueList) {
      // XXX hack to make sure that we put into a valid CF
      byte[] cf = keyValue.getFamily();
      if (cf == null || cf.length == 0)
        cf = HBaseTableAdapter.DEFAULT_COLUMN;
      org.apache.hadoop.hbase.client.Put p = new org.apache.hadoop.hbase.client.Put(
          keyValue.getRowId()).add(cf,
          keyValue.getQualifier(), keyValue.getValue());
      puts.add(p);
    }

    try {
      this.table.put(puts);
    } catch (IOException e) {
      throw new RuntimeException("Failed to add puts to HBase table", e);
    }

  }

  @Override
  public <T> List<T> remoteExec(byte[] startKey, 
		                        byte[] endKey,
                                final Class<? extends RemoteOp<T>> remoteCallable, 
                                final Object... array) {

    // checking keys to make sure that we span the full key range
    if (startKey != null && startKey.length == 0)
      startKey = null;
    if (endKey != null && endKey.length == 0)
      endKey = null;

    final Configuration conf = getConf();
    Map<byte[], T> results = null;
    try {
/*
      results = this.table.coprocessorExec(
          HBaseCulvertCoprocessorProtocol.class, startKey, endKey,
          new Batch.Call<HBaseCulvertCoprocessorProtocol, T>() {
            @Override
            public T call(HBaseCulvertCoprocessorProtocol instance)
                throws IOException {
              return instance.call(remoteCallable, conf, Arrays.asList(array));
            }
*/
    	/*Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable)
      */
    	
    	Batch.Call<HBaseCulvertCoprocessorProtocol, T>  batch = new Batch.Call<HBaseCulvertCoprocessorProtocol, T>() {
            @Override
            public T call(HBaseCulvertCoprocessorProtocol instance)
                throws IOException {
              return instance.call(remoteCallable, conf, Arrays.asList(array));
            }
          };
          
        results = this.table.coprocessorExec(
                HBaseCulvertCoprocessorProtocol.class, startKey, endKey, batch);
        
      List<T> tResults = new ArrayList<T>();
      for (Map.Entry<byte[], T> e : results.entrySet()) {
        T res = e.getValue();
        tResults.add(res);

      }

      return tResults;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to execute remote call", e);
    }

  }

  @Override
  public SeekingCurrentIterator get(Get get) {
    CRange range = get.getRange();
    byte[] start = range.getStart();
    byte[] end = range.getEnd();
    // if we are just getting a single row
    if (Bytes.compareTo(start, end) == 0) {
      // if it is not inclusive, we have no results
      if (!range.isEndInclusive() || !range.isStartInclusive())
        return new DecoratingCurrentIterator(
            new ArrayList<Result>(0).iterator());
      // setup the get
      org.apache.hadoop.hbase.client.Get hGet = new org.apache.hadoop.hbase.client.Get(
          start);

      //add the columns to get
      for(CColumn column: get.getColumns())
      {
        if(column.getColumnQualifier().length == 0)
 {
          // XXX hack to make sure that we don't get from an empty column
          if (column.getColumnFamily().length == 0)
            hGet.addFamily(DEFAULT_COLUMN);
          else
          hGet.addFamily(column.getColumnFamily());
        }
        else
        {
          // XXX hack to make sure that we don't get from an empty column
          if (column.getColumnFamily().length == 0)
            hGet.addColumn(DEFAULT_COLUMN, column.getColumnQualifier());
          else
          hGet.addColumn(column.getColumnFamily(), column.getColumnQualifier());
        }
      }

      // do the get
      try {
        org.apache.hadoop.hbase.client.Result r = this.table.get(hGet);
        Result rr = resultConverter.apply(r);
        if(rr == null){
        	System.out.println("Empy Result. Return null");
        	return null;
        }
        else{
          Iterator<Result> iter = Arrays.asList(rr).iterator();
          return new DecoratingCurrentIterator(iter);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to get from HBase", e);
      }
    }
    // make sure that we get the right ranges
    // if we don't include the start
    if (!range.isStartInclusive())
      start = Bytes.increment(start);

    // if we do include the end
    if (range.isEndInclusive())
      end = Bytes.increment(end);

    // create a scanner [start, end)
    Scan scan = new Scan(start, end);
    // add the columns to scan
    for (CColumn column : get.getColumns()) {
      if (column.getColumnQualifier().length == 0)
        scan.addFamily(column.getColumnFamily());
      else
        scan.addColumn(column.getColumnFamily(), column.getColumnQualifier());
    }

    // do the scan
    try {
      ResultScanner scanner = this.table.getScanner(scan);
      return new DecoratingCurrentIterator(Iterators.transform(
          scanner.iterator(), resultConverter));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.table = new HTable(getConf(), getTableName());
  }

  @Override
  public byte[][] getStartKeys() {
    try {
      return this.table.getStartKeys();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[][] getEndKeys() {
    try {
      return this.table.getEndKeys();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<String> getHosts() {
    try {
      NavigableMap<HRegionInfo, ServerName> regions = this.table
          .getRegionLocations();
      Set<String> hosts = new TreeSet<String>();
      for (Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
        hosts.add(entry.getValue().getHostAndPort());
      }
      return hosts;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert from an HBase result into a Culvert result
   */
  private static class HResultConverter implements
      Function<org.apache.hadoop.hbase.client.Result, Result> {

    @Override
    public Result apply(org.apache.hadoop.hbase.client.Result hresult) {
      if (hresult.isEmpty()){
    	//If there are no values return a null result
    	return null;
      }

      // if there are values, build up a list of CKeyValue
      List<KeyValue> hvalues = hresult.list();
      List<CKeyValue> values = new ArrayList<CKeyValue>(hvalues.size());
      for (KeyValue kv : hvalues) {
        // XXX hack to make sure that we return correct results from the default
        // column
        byte[] cf = kv.getFamily();
        if (Arrays.equals(cf, DEFAULT_COLUMN))
          cf = new byte[0];
        values.add(new CKeyValue(kv.getRow(), kv.getFamily(),
            kv.getQualifier(), kv.getTimestamp(), kv.getValue()));
      }
      
      return new Result(values);
    }

  }
}

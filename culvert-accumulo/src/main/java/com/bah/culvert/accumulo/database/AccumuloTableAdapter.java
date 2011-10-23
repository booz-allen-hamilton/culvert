package com.bah.culvert.accumulo.database;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.accumulo.AccumuloGet;
import com.bah.culvert.accumulo.AccumuloKeyValue;
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
import com.bah.culvert.util.Exceptions;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * Connect, write and read to/from an Accumulo table. If 'special' features of
 * Accumulo are desired, use the special accumulo versions of the desired
 * classes.
 * @see AccumuloGet
 * @see AccumuloKeyValue
 */
public class AccumuloTableAdapter extends TableAdapter {

  private final Connector conn;
  private final BatchWriter writer;
  private static final boolean DEBUG = false;

  public AccumuloTableAdapter(Connector conn, String tableName, long maxMemory,
      long maxLatency, int maxWriteThreads) {
    super(tableName);
    this.conn = conn;
    try {
      writer = conn.createBatchWriter(tableName, maxMemory, maxLatency,
          maxWriteThreads);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Put put) {
    try {
      writer.addMutations(Iterables.transform(put.getKeyValueList(),
          CKeyValuetoMutation));
      writer.flush();
    } catch (Exception e) {
      throw Exceptions.asRuntime(e);
    }

  }

  @Override
  public SeekingCurrentIterator get(Get get) {
    if (DEBUG)
      try {
        printCurrentTable();
      } catch (Throwable e1) {
        e1.printStackTrace();
      }

    CRange range = get.getRange();
    byte[] start = range.getStart();
    byte[] end = range.getEnd();
    // if we are just getting a single row
    if (Bytes.compareTo(start, end) == 0) {
      // if it is not inclusive, we have no results
      if (!range.isEndInclusive() || !range.isStartInclusive())
        return new DecoratingCurrentIterator(
            new ArrayList<Result>(0).iterator());
    }

    List<CColumn> columns = get.getColumns();

    Authorizations auths;

    if (get instanceof AccumuloGet)
      auths = ((AccumuloGet) get).getAuthorizations();
    else
      auths = Constants.NO_AUTHS;

    try {
      // create the scanner on this table with the specified authorizations
      Scanner scan = conn.createScanner(this.getTableName(), auths);
      // add the columns to the scan
      for (CColumn column : columns) {
        byte[] qual = column.getColumnQualifier();
        if (Arrays.equals(qual,
            com.bah.culvert.util.Constants.EMPTY_COLUMN_QUALIFIER))
          scan.fetchColumnFamily(new Text(column.getColumnFamily()));
        else
          scan.fetchColumn(new Text(column.getColumnFamily()), new Text(qual));
      }
      // set the range
      scan.setRange(new Range(start.length == 0 ? null : new Text(start), range
          .isStartInclusive(), end.length == 0 ? null : new Text(end), range
          .isEndInclusive()));

      return new DecoratingCurrentIterator(new CombiningIterator(
          scan.iterator()));
    } catch (Exception e) {
      throw Exceptions.asRuntime(e);
    }

  }

  @Override
  public <T> List<T> remoteExec(byte[] startKey, byte[] endKey,
      Class<? extends RemoteOp<T>> remoteCallable, Object... array) {
    // TODO port this up to actually using the tablet servers - requires
    // patching
    // accumulo
    // when accumulo is patched, remove the next block
    for (Object o : array)
      if (!(o instanceof Writable || o instanceof Serializable))
        throw new IllegalArgumentException("Argument " + o
            + " is not serializable or writable!");

    try {
      RemoteOp operation = remoteCallable.newInstance();
      operation.setConf(this.getConf());
      operation.setLocalTableAdapter(new AccumuloLocalTableAdapter(this));
      return (List<T>) Arrays.asList(operation.call(array));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[][] getStartKeys() {
    try {
      Collection<Text> splits = conn.tableOperations().getSplits(
          this.getTableName());
      // splits only define intertable boundaries - so we need to add the zero
      // for the start
      byte[] start = new byte[0];

      byte[][] byteSplits = new byte[splits.size() + 1][];
      byteSplits[0] = start;
      int i = 1;
      for (Text t : splits) {
        byteSplits[i++] = t.getBytes();
      }
      return byteSplits;
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[][] getEndKeys() {
    try {

      Collection<Text> splits = conn.tableOperations().getSplits(
          this.getTableName());
      byte[][] byteSplits = new byte[splits.size() + 1][];
      int i = 0;
      for (Text t : splits) {
        byteSplits[i++] = t.getBytes();
      }

      // specify the end as null since Accumulo says that null - go to end of
      // range
      byteSplits[i] = null;
      return byteSplits;
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<String> getHosts() {
    return conn.instanceOperations().getTabletServers();
  }

  private final Function<CKeyValue, Mutation> CKeyValuetoMutation = new Function<CKeyValue, Mutation>() {

    @Override
    public Mutation apply(CKeyValue input) {
      Mutation m = new Mutation(new Text(input.getRowId()));
      // if it is an accumulo key value, pull out the interesting info
      if (input instanceof AccumuloKeyValue) {
        AccumuloKeyValue kv = (AccumuloKeyValue) input;
        m.put(new Text(kv.getFamily()), new Text(kv.getQualifier()),
            new ColumnVisibility(kv.getVisibility()), new Value(kv.getValue()));
      }
      else
        m.put(new Text(input.getFamily()), new Text(input.getQualifier()),
            new Value(input.getValue()));

      return m;
    }
  };

  /**
   * Iterator to combine results from a multiple Accumulo rows into a single
   * result
   */
  private final class CombiningIterator implements Iterator<Result> {

    private final PeekingIterator<Entry<Key, Value>> backingIterator;
    private Result nextResult;

    public CombiningIterator(Iterator<Entry<Key, Value>> accumuloIterator) {
      boolean hasNext = accumuloIterator.hasNext();
      backingIterator = Iterators.peekingIterator(accumuloIterator);
      nextResult = getNextResult();
    }

    private Result getNextResult() {
      Result result = null;
      // if there are more values, get the next one
      if (backingIterator.hasNext()) {
        result = new Result(EntrytoResult.apply(backingIterator.next()));
        byte[] key = result.getRecordId();
        // if there are still more values
        while (backingIterator.hasNext()) {
          // peek into the next value
          CKeyValue kv = EntrytoResult.apply(backingIterator.peek());
          // check to see if it matches
          if (Arrays.equals(kv.getRowId(), key)) {
            // if it matches, add it to the current result
            result.addKeyValues(kv);
            // and then pull the next value out to make sure we don't peek it
            // next time
            backingIterator.next();
          }
          else
            break;

        }
        return result;
      }
      else
        return result;
    }

    @Override
    public boolean hasNext() {
      return nextResult != null;
    }

    @Override
    public Result next() {
      Result current = nextResult;
      nextResult = getNextResult();
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Cannot remove values from a scan of Accumulo");

    }

    private final Function<Entry<Key, Value>, CKeyValue> EntrytoResult = new Function<Entry<Key, Value>, CKeyValue>() {

      @Override
      public CKeyValue apply(Entry<Key, Value> input) {
        Key key = input.getKey();
        return new AccumuloKeyValue(new AccumuloKeyValue(key.getRow()
            .getBytes(), key.getColumnFamily().getBytes(), key
            .getColumnQualifier().getBytes(), key.getColumnVisibility()
            .toString(), key.getTimestamp(), input.getValue().get()));
      }

    };
  }

  public void printCurrentTable() throws Throwable {
    Scanner scan = conn.createScanner(this.getTableName(), Constants.NO_AUTHS);
    for (Entry<Key, Value> e : scan) {
      System.out.println("Row:  |" + e.getKey().getRow());
      System.out.println("CF:   |" + e.getKey().getColumnFamily());
      System.out.println("CQ:   |" + e.getKey().getColumnQualifier());
      System.out.println("Value:|" + new String(e.getValue().get()));
    }
  }

}

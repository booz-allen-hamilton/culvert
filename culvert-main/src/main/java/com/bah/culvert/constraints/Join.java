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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;

import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.constraints.filter.Filter;
import com.bah.culvert.constraints.filter.FilteredConstraint;
import com.bah.culvert.constraints.filter.ResultFilter;
import com.bah.culvert.constraints.write.Handler;
import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.NullResult;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.util.Bytes;

/**
 * SQL-style join on a single column.
 * <p>
 * This implements the inner join semantics: returns row when there is a match
 * in all the result sets.
 * <p>
 * Currently the implementation takes the results set from the 'left' constraint
 * applied to the sent table and then does a select on the specified columns
 * from those results. The output is then dumped into a temporary table. The
 * temporary table then runs the Remote JoinOp on the server to pull in matching
 * values from the "other" table. The results of the join on each remote server
 * are then dumped into an 'output' table and iterated over.
 */
public abstract class Join extends Constraint {

  private DatabaseAdapter database;
  private TableAdapter leftTable;
  private Constraint left;
  private CColumn leftColumn;
  private String rightTable;

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public Join() {

  }

  /**
   * Join the specified constraints based on the specified column.
   * <p>
   * The left constraint is first applied to the table in
   * {@link #getResultIterator()}, and the the specified column in
   * {@link #leftColumn} are used to extract the desired columns from the 'left'
   * side of the join. The right table constraint is then applied and the
   * resulting values are then used for the 'right' side of the join. Where
   * values match for the left and right side of the join, a value is returned
   * in the output table.
   * @param db Database to use when creating the temporary table
   * @param leftTable from which to retrieve necessary columns
   * @param left Constraint to apply on the table before running the join.
   * @param leftColumn column to select from the table and join the results of
   *        the constraint on
   * @param rightTable to operate on
   */
  public Join(DatabaseAdapter db, TableAdapter leftTable, Constraint left,
      CColumn leftColumn, String rightTable) {
    this.database = db;
    this.leftTable = leftTable;
    this.left = left;
    this.leftColumn = leftColumn;
    this.rightTable = rightTable;
  }

  /*
   * Sent table will be the one from which we pull the first result set into the
   * temporary table. All subsequent tables will be queried using a remoteOp.
   */
  @Override
  public SeekingCurrentIterator getResultIterator() {

    // then create the output table to iterate over the whole result set
    TableAdapter output = Join.createOutputTable(this.database,
        getAllTableNames(this.leftTable, this.rightTable));

    // apply the left side of the join and dump it to an output table
    this.left.writeToTable(output, new JoinWriteHandler(this.leftTable,
        this.leftColumn));

    doRemoteOperation(output,
        Join.getOutputColumn(this.rightTable));

    // do a select * from the table
    return new FilteredConstraint(new ResultFilter(output,
        CRange.FULL_TABLE_RANGE), new OnlyMultipleRowsFilter())
        .getResultIterator();
    // TODO find a way to clean up this output table to ensure we don't create
    // extra tables that we don't have to.
  }

  /**
   * Do the remote call on the output table. This should involve a call to
   * {@link TableAdapter#remoteExec(byte[], byte[], Class, Object...)}. By the
   * end of the method, the output table should be populated with the complete
   * output data.
   * <p>
   * The output table will already be populated with data from the left side of
   * the join - rowid: value joining on | CF: tableName | CQ: original table row
   * id | value: empty
   * @param outputTable
   * @param rightColumn
   * @param rightOutputColumn
   */
  protected abstract void doRemoteOperation(TableAdapter outputTable,
      byte[] rightOutputColumn);

  /**
   * Get all the table names
   * @param table main table
   * @param subConstraints to use
   * @return array of names of tables
   */
  private final static String[] getAllTableNames(TableAdapter leftTable,
      String... rightTables) {
    Set<String> names = new TreeSet<String>();

    names.add(leftTable.getTableName());
    for (String table : rightTables) {
      names.add(table);
    }
    return names.toArray(new String[0]);
  }

  /**
   * Create the output table based on the sent table names.
   * <p>   
   * The resulting table has the schema:
   * <p>
   * Row | Column Family | Column Qualifier <br>
   * original value | "[original table name].rowID" as bytes | original row id
   * @param table that is being filtered
   * @param constraints that will be applied
   * @return a table to dump all the results
   */
  private final static TableAdapter createOutputTable(DatabaseAdapter database,
      String... tables) {
    // create the output column families
    List<CColumn> columns = new ArrayList<CColumn>(tables.length);
    for (String name : tables)
      columns.add(new CColumn(getOutputColumn(name)));

    // create the table
    String outputTable = createOutputTableName();
    database.create(outputTable, null, columns);
    return database.getTableAdapter(outputTable);
  }

  private final static String createOutputTableName() {
    return "join-output-" + UUID.randomUUID().toString();
  }

  protected final static byte[] getOutputColumn(String tableName) {
    return (tableName + ".rowID").getBytes();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);

    // read in left table
    ow.readFields(in);
    this.leftTable = (TableAdapter) ow.get();

    // read in left constraint
    ow.readFields(in);
    this.left = (Constraint) ow.get();

    // read in left column
    this.leftColumn = new CColumn();
    this.leftColumn.readFields(in);

    // read in right table
    this.rightTable = Text.readString(in);

    ow.readFields(in);
    this.database = (DatabaseAdapter) ow.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write the left table
    ObjectWritable ow = new ObjectWritable(this.leftTable);
    ow.write(out);

    // write left constraint
    ow.set(this.left);
    ow.write(out);

    this.leftColumn.write(out);

    Text.writeString(out, this.rightTable);

    // write out the database
    ow.set(this.database);
    ow.write(out);
  }
  
  /**
   * Only return results that have more than 1 keyvalue
   */
  private static class OnlyMultipleRowsFilter extends Filter
  {
    public OnlyMultipleRowsFilter() {
      // NOOP
    }

    @Override
    public Result apply(Result toFilter) {
      int count = 0;
      for (@SuppressWarnings("unused")
      CKeyValue kv : toFilter.getKeyValues()) {
        if (count >= 1)
          return toFilter;
        count++;
      }
      return NullResult.INSTANCE;
    }
    
  }

  /**
   * Handle writing the left side of the join into the output table.
   * <p>
   * NOTE: this is currently not meant to be serialized
   */
  public static class JoinWriteHandler extends Handler {

    private final TableAdapter sourceTable;
    private final CColumn column;

    public JoinWriteHandler(TableAdapter primaryTable, CColumn column) {
      this.sourceTable = primaryTable;
      this.column = column;
    }

    @Override
    public List<CKeyValue> apply(Result row) {
      // TODO Implement apply
      List<CKeyValue> results = new ArrayList<CKeyValue>();
      String tableName = this.sourceTable.getTableName();

      // put the values in the row (rowid, cf, cq, value) -> (value,
      // "[tableName].rowID", rowid) as |row|CF|CQ||
      for (CKeyValue value : filterKeyValues(this.sourceTable, this.column,
          row.getRecordId(),
          row.getKeyValues()))
        results.add(new CKeyValue(value.getValue(), getOutputColumn(tableName),
            value.getRowId()));
      return results;
    }

    /**
     * Filter the specified key values from the SAME row. If the specified
     * column is not present in the set of key values, it is attempted to be
     * retrieved from the sent table
     * @param table to do lookup
     * @param column to filter for
     * @param rowid of the keyValues
     * @param keyValues to filter
     * @return {@link CKeyValue}s from the specified table matching the filter
     */
    private static Iterable<CKeyValue> filterKeyValues(TableAdapter table,
        CColumn column, byte[] rowid, Iterable<CKeyValue> keyValues) {
      // if there are no columns or a default
      if (column == null || column.compareTo(CColumn.ALL_COLUMNS) == 0)
        return keyValues;

      // otherwise, filter the specified columns
      List<CKeyValue> matching = getMatchingKeyValues(keyValues, column);
      // if no columns match, do a select on that row
      if (matching.size() == 0) {
        Get get = new Get(new CRange(rowid));
        get.addColumn(column);
        SeekingCurrentIterator iter = table.get(get);
        // if those columns exist in the table, return the match
        // since we are only getting 1 row, only going to get 1 result
        if (iter.hasNext())
          matching = getMatchingKeyValues(iter.next().getKeyValues(), column);
      }
      // return the list of matching columns
      return matching;
    }

    /**
     * Get the {@link KeyValue}s that match the specified columns
     * @param kvs to match
     * @param columns to match against
     * @return the matching {@link KeyValue}s. An emtpy list if none match.
     */
    private static List<CKeyValue> getMatchingKeyValues(
        Iterable<CKeyValue> kvs, CColumn columns) {
      List<CKeyValue> matching = new ArrayList<CKeyValue>();
      for (CKeyValue kv : kvs)
        if (columnsMatch(kv, columns))
          matching.add(kv);
      return matching;
    }

    /**
     * Check to see if the column matches the sent keyValue.
     * <p>
     * If column.CF == null || 0 length || match, then match.. If column.CQ ==
     * null || 0 length || match, then match. If Column.timestamp == DEFAULT or
     * < kv.timestamp, it is a match
     * @param kv to check
     * @param column to check against
     * @return <tt>true</tt> if it matches exactly, <tt>false</tt> otherwise.
     */
    private static boolean columnsMatch(CKeyValue kv, CColumn column) {
      byte[] cf = column.getColumnFamily();
      // if CF matches
      if (cf == null || cf.length == 0 || Bytes.equals(cf, kv.getFamily())) {
        // if the CQ matches
        byte[] cq = column.getColumnQualifier();
        if (cq == null || cq.length == 0 || Bytes.equals(cq, kv.getQualifier())) {
          // TODO add timestamp checking
          return true;
        }
      }
      return false;
    }

  }
  

}

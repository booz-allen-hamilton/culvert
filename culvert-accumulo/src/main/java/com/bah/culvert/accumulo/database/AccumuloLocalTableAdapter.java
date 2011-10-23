package com.bah.culvert.accumulo.database;

import java.util.Iterator;

import com.bah.culvert.adapter.LocalTableAdapter;
import com.bah.culvert.data.Result;
import com.bah.culvert.transactions.Get;

/**
 * Local table adapter for server-side accumulo table access Note: Until
 * accumulo has the ability to pass objects to iterators, this does not actually
 * remote table, and hence, is shortcutted here.
 */
public class AccumuloLocalTableAdapter extends LocalTableAdapter {

  private final AccumuloTableAdapter table;

  public AccumuloLocalTableAdapter(AccumuloTableAdapter table) {
    this.table = table;
  }

  @Override
  public Iterator<Result> get(Get get) {
    return table.get(get);
  }

  @Override
  public byte[] getStartKey() {
    return table.getStartKeys()[0];
  }

  @Override
  public byte[] getEndKey() {
    return table.getEndKeys()[0];
  }

}

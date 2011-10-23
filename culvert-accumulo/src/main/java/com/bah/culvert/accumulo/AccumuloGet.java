package com.bah.culvert.accumulo;

import static org.apache.accumulo.core.Constants.NO_AUTHS;

import org.apache.accumulo.core.security.Authorizations;

import com.bah.culvert.data.CRange;
import com.bah.culvert.transactions.Get;

/**
 * A Get via Culvert that has {@link Authorizations} into an Accumulo table
 * 
 * @see Get
 */
public class AccumuloGet extends Get {

  private final Authorizations auths;

  public AccumuloGet(CRange range, Authorizations auths) {
    super(range);
    this.auths = auths;
  }

  public AccumuloGet(CRange range) {
    this(range, NO_AUTHS);
  }

  public AccumuloGet(CRange range, byte[] columnFamily) {
    this(range, columnFamily, NO_AUTHS);
  }

  public AccumuloGet(CRange range, byte[] columnFamily, Authorizations auths) {
    super(range, columnFamily);
    this.auths = auths;
  }

  public AccumuloGet(CRange range, byte[] columnFamily, byte[] columnQualifier) {
    this(range, columnFamily, columnQualifier, NO_AUTHS);
  }

  public AccumuloGet(CRange range, byte[] columnFamily, byte[] columnQualifier,
      Authorizations auths) {
    super(range, columnFamily, columnQualifier);
    this.auths = auths;
  }

  public Authorizations getAuthorizations() {
    return this.auths;
  }

}

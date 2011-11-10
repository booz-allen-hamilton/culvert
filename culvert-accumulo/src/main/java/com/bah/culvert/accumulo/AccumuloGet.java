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

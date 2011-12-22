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
package com.bah.culvert.examples;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.Client;
import com.bah.culvert.configuration.CConfiguration;
import com.bah.culvert.constraints.And;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.IndexRangeConstraint;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;

public class Query {

  public static void main(String args) {
    Configuration culvertConf = CConfiguration.getDefault();
    // index definitions are loaded implicitly from the configuration
    Client client = new Client(culvertConf);
    Index c1Index = client.getIndexByName("index1");
    Constraint c1Constraint = new IndexRangeConstraint(c1Index, new CRange(
        "abba".getBytes(), "cadabra".getBytes()));
    Index[] c2Indices = client.getIndicesForColumn("tablename",
        "rabbit".getBytes(), "hat".getBytes());
    Constraint c2Constraint = new IndexRangeConstraint(c2Indices[0],
        new CRange("bar".getBytes(), "foo".getBytes()));
    Constraint and = new And(c1Constraint, c2Constraint);
    Iterator<Result> results = client.query(and);
  }
}
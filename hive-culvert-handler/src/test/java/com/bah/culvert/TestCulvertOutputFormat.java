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
package com.bah.culvert;

import java.util.Arrays;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.data.CColumn;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.hive.CulvertHiveUtils;
import com.bah.culvert.hive.CulvertOutputFormat;
import com.bah.culvert.inmemory.InMemoryDB;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Get;
import com.bah.culvert.transactions.Put;

@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public class TestCulvertOutputFormat {

  @Ignore(value = "Filed as bug. #375")
  @Test
  public void testBasicOperation() throws Throwable {
    CulvertOutputFormat format = new CulvertOutputFormat();
    // the client only works with configurations
    JobConf conf = new JobConf();
    InMemoryDB db = new InMemoryDB();
    Client.setDatabaseAdapter(conf, db);
    /*
     * most of the stuff we set in the table properties because we use the
     * jobconf earlier for input stuff
     */
    Properties tblProps = CulvertHiveUtils.confToProps(conf);

    CColumn col = new CColumn("foo".getBytes(), "bar".getBytes());
    db.create("baz", Arrays.asList(col));
    CulvertHiveUtils.setCulvertConfigurationIsEmbedded(tblProps, true);
    CulvertHiveUtils.setCulvertTable(tblProps, "baz");
    final int[] i = { 0 };
    Progressable progress = new Progressable() {

      @Override
      public void progress() {
        i[0]++;
      }
    };
    RecordWriter writer = format.getHiveRecordWriter(conf, null, Put.class,
        true, tblProps, progress);
    writer.write(new Put(new CKeyValue("a".getBytes(), "b".getBytes(), "c"
        .getBytes(), "d".getBytes())));
    Assert.assertEquals(1, i[0]);

    SeekingCurrentIterator it = db.getTableAdapter("baz").get(
        new Get(new CRange("a".getBytes())));
    // this is failing - looks like stuff has been put but isn't coming out of
    // the get
    Assert.assertTrue("Iterator should have a next value", it.hasNext());
    Result next = it.next();
    Assert.assertTrue("Result row should be 'a' byte equivalent",
        Arrays.equals("a".getBytes(), next.getRecordId()));
    Assert.assertTrue(
        "Result should be ",
        Arrays.equals("d".getBytes(),
            next.getValue("b".getBytes(), "c".getBytes()).getValue()));
  }

}
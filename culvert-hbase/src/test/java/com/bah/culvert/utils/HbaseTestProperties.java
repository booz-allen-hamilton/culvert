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
package com.bah.culvert.utils;

import org.apache.hadoop.conf.Configuration;

public class HbaseTestProperties {
  public static void addStandardHBaseProperties(Configuration conf) {
    // The following properties are from HBase's
    // src/test/resources/hbase-site.xml
    conf.set("hbase.regionserver.msginterval", "1000");
    conf.set("hbase.client.pause", "5000");
    conf.set("hbase.client.retries.number", "4");
    conf.set("hbase.master.meta.thread.rescanfrequency", "10000");
    conf.set("hbase.server.thread.wakefrequency", "1000");
    conf.set("hbase.regionserver.handler.count", "5");
    conf.set("hbase.master.info.port", "-1");
    conf.set("hbase.regionserver.info.port", "-1");
    conf.set("hbase.regionserver.info.port.auto", "true");
    conf.set("hbase.master.lease.thread.wakefrequency", "3000");
    conf.set("hbase.regionserver.optionalcacheflushinterval", "1000");
    conf.set("hbase.regionserver.safemode", "false");
  }
}

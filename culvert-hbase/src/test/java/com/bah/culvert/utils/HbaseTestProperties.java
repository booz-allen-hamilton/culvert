package com.bah.culvert.utils;

import org.apache.hadoop.conf.Configuration;

public class HbaseTestProperties {
    public static void addStandardHBaseProperties(Configuration conf) {
      // The following properties are from HBase's src/test/resources/hbase-site.xml
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

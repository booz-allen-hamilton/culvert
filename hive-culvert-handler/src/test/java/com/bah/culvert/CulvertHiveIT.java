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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Ignore(value = "Filed as #374")
@RunWith(Parameterized.class)
public class CulvertHiveIT {

  private QTestUtil util;
  private String tempLog;
  private String tempTestDir;
  private final String scriptName;
  private String tempHadoopLogs;
  private final String scriptLocation;

  @Parameters
  public static List<String[]> params() {
    String[] queries = { "culvert_pushdown.q" };
    List<String[]> paramList = new ArrayList<String[]>(queries.length);
    for (String query : queries) {
      try {
        URL fileUrl = CulvertHiveIT.class
            .getResource("/clientqueries/" + query);
        String path = new File(fileUrl.toURI()).getAbsolutePath();
        paramList.add(new String[] { path, query });
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    return paramList;
  }

  public CulvertHiveIT(String scriptLocation, String scriptName) {
    this.scriptLocation = scriptLocation;
    this.scriptName = scriptName;
  }

  @Before
  public void prepareEnvironment() throws Throwable {
    Configuration.addDefaultResource("hive-site.xml");
    tempTestDir = mkDirP("target/hive-test/");
    tempLog = mkDirP("target/hive-log/");
    tempHadoopLogs = mkDirP("target/hive-hadoop-log/");
    System.setProperty("hadoop.log.dir", tempHadoopLogs);
    System.setProperty("test.tmp.dir", tempTestDir);
    System.setProperty("test.output.overwrite", "true");
    // System.setProperty("javax.jdo.PersistenceManagerFactoryClass", );
    util = new QTestUtil("src/test/resources/clientresults/", tempLog, true,
        "0.20");
  }

  @Test
  public void testHiveScript() throws Throwable {
    util.addFile(scriptLocation);
    util.clearTestSideEffects();
    util.cliInit(scriptLocation);
    int parseRes = util.executeClient(scriptName);
    TestCase.assertEquals(0, parseRes);
    int verifyRes = util.checkCliDriverResults(scriptName);
    TestCase.assertEquals(0, verifyRes);
  }

  @After
  public void cleanup() throws Throwable {
    util.cleanUp();
    // recursiveDelete(tempHadoopLogs);
    // recursiveDelete(tempTestDir);
    // recursiveDelete(tempLog);
  }

  private static String mkDirP(String dir) {
    new File(dir).mkdirs();
    return dir;
  }

  private static String createTempFolder(String string) {
    try {
      File temp = File.createTempFile("culvert-hive-test", string);
      temp.delete();
      temp.mkdirs();
      return temp.getAbsolutePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void recursiveDelete(String filename) {
    File file = new File(filename);
    if (file.isFile()) {
      file.delete();
    } else {
      for (File child : file.listFiles()) {
        recursiveDelete(child.getAbsolutePath());
      }
      file.delete();
    }
  }
}

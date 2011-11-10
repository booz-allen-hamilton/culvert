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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;

public class AccumuloConstants {

  /* Configuration keys */
  public static final String ZOOKEEPER_SERVERS_KEY = "culvert.accumulo.zookeepers";
  public static final String INSTANCE_CLASS_KEY = "culvert.accumulo.instance.class";
  public static final String INSTANCE_NAME_KEY = "culvert.accumulo.instance.name";
  public static final String USERNAME_KEY = "culvert.accumulo.user.name";
  public static final String PASSWORD_KEY = "culvert.accumulo.user.password";
  public static final String AUTHORIZATIONS_KEY = "culvert.accumulo.user.authorizations";
  public static final String MAX_THREADS_KEY = "culvert.accumulo.writer.maxThreads";
  public static final String MAX_MEMORY_KEY = "culvert.accumulo.writer.maxMemory";
  public static final String MAX_LATENCY_KEY = "culvert.accumulo.writer.latency";

  /* Connection constants */
  public static final String DEFAULT_INSTANCE_NAME = "default-instance";
  public static final Class<? extends Instance> DEFAULT_INSTANCE_CLASS = ZooKeeperInstance.class;
  public static final long DEFAULT_MAX_MEMORY = 10000;
  public static final long DEFAULT_MAX_LATENCY = 6000;
  public static final int DEFAULT_MAX_THREADS = 1;
}

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
package com.bah.culvert.examples;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.Client;
import com.bah.culvert.configuration.CConfiguration;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.transactions.Put;
import com.google.common.collect.Lists;

public class Insertion {

	public static void main(String[] args) throws Throwable {
		Configuration culvertConf = CConfiguration.getDefault();
		// index definitions are loaded implicitly from the configuration
		Client client = new Client(culvertConf);
		List<CKeyValue> valuesToPut = Lists.newArrayList();
		valuesToPut.add(new CKeyValue("foo".getBytes(), "bar".getBytes(), "baz"
				.getBytes()));
		Put put = new Put(valuesToPut);
		client.put("tablename", put);
	}

}

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
package com.bah.culvert.util;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class ConfUtils {

	/**
	 * Pack a configuration into a namespace in another one.
	 * 
	 * @param prefix
	 *            The prefix to pack into. A '.' character is appended to the
	 *            prefix.
	 * @param toPack
	 *            The configuration to pack.
	 * @param toPackInto
	 *            The configuration to pack <tt>toPack</tt> into.
	 * @return <tt>toPackInto</tt>, with <tt>toPack</tt> packed in the prefix.
	 */
	public static Configuration packConfigurationInPrefix(String prefix,
			Configuration toPack, Configuration toPackInto) {
		for (Entry<String, String> entry : toPack) {
			toPackInto.set(prefix + "." + entry.getKey(), entry.getValue());
		}
		return toPackInto;
	}

	/**
	 * Unpack a configuration that has been prefixed. A '.' character must be
	 * used to separate the prefixed keys from the prefix.
	 * 
	 * @param prefix
	 *            The prefix to unpack.
	 * @param toUnpackFrom
	 *            The configuration to unpack from.
	 * @return The unpacked configuration.
	 */
	public static Configuration unpackConfigurationInPrefix(String prefix,
			Configuration toUnpackFrom) {
    Configuration conf = new Configuration(false);
		conf.clear();
		prefix += ".";
		for (Entry<String, String> entry : toUnpackFrom) {
			String key = entry.getKey();
			if (key.startsWith(prefix)) {
				key = key.substring(prefix.length());
				conf.set(key, entry.getValue());
			}
		}
		return conf;
	}

}

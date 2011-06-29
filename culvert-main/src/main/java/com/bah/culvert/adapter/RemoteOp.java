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
package com.bah.culvert.adapter;

import com.bah.culvert.util.BaseConfigurable;

/**
 * Perform a remote operation on a table
 * @param <V> Results to return
 */
public abstract class RemoteOp<V> extends BaseConfigurable {

	private LocalTableAdapter localTableAdapter = null;

  /**
   * @return the adapter to the local table.
   */
	public final LocalTableAdapter getLocalTableAdapter() {
		return localTableAdapter;
	}

  /**
   * Set the local table adapter
   * <p>
   * The local table adapter is set by whatever mechanism performs
   * {@link TableAdapter#remoteExec(byte[][], org.apache.hadoop.conf.Configuration, Class)}
   * 
   * @param localTableAdapter to connect to the local table
   */
	public final void setLocalTableAdapter(LocalTableAdapter localTableAdapter) {
		this.localTableAdapter = localTableAdapter;
	}

  /**
   * Invoke this routine remotely with the provided arguments.
   * 
   * @param args The arguments, if any.
   * @return The value
   * @throws Exception on failure
   */
	public abstract V call(Object... args) throws Exception;

}

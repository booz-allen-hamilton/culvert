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
package com.bah.culvert.iterators;

import java.util.Iterator;

import com.bah.culvert.constraints.Or;
import com.bah.culvert.data.Result;

/**
 * An iterator that can also return the most recent result as many times as you
 * like, and can also seek
 * 
 * @see Iterator
 */
public interface SeekingCurrentIterator extends Iterator<Result> {

  /**
   * @return The last object returned by {@link #next()}, or null if
   *         <code>next()</code> has never been called.
   */
  public Result current();

  /**
   * Advance this iterator until a result with a key equal to or greater than
   * the provided argument is encountered.
   * <p>
   * The current key ({@link #current()}) is updated to be the key that has been
   * seeked to, if the key is present.{@link #current()} will return null if the
   * seeked key is not present
   * 
   * @param key The key to seek to.
   */
  public void seek(byte[] key);

  /**
   * Used by query orchestrator machinery to mark when a query iterator is no
   * longer being used for consideration as a sub-constraint. Implemented
   * specifically for {@link Or}, but could have applications elsewhere.
   */
  public void markDoneWith();

  /**
   * Used by query orchestrator machinery to detect if a query iterator is
   * finished.
   * 
   * @return <tt>true</tt> if this iterator has been {@link #markDoneWith()}.
   */
  public boolean isMarkedDoneWith();
}

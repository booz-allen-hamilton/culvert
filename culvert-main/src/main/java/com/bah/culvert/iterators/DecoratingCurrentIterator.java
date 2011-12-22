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

import com.bah.culvert.data.Result;
import com.bah.culvert.util.Bytes;

/**
 * Provides a wrapper class to get {@link SeekingCurrentIterator} functionality
 * from a plain {@link Iterator}.
 * 
 * @see Iterator
 */
public class DecoratingCurrentIterator implements SeekingCurrentIterator {
  private boolean done = false;
  protected Result current = null;
  private final Iterator<Result> delegate;

  /**
   * Create a {@link SeekingCurrentIterator} that delegates all of its method
   * calls to an underlying iterator.
   * 
   * @param delegate The underlying iterator to delegate method calls to.
   */
  public DecoratingCurrentIterator(Iterator<Result> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Result current() {
    return current;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = delegate.hasNext();
    if (!hasNext)
      markDoneWith();

    return hasNext;
  }

  @Override
  public Result next() {
    current = delegate.next();
    return current;
  }

  @Override
  public void remove() {
    delegate.remove();
  }

  @Override
  public void seek(byte[] key) {
    // if the key to seek to is less than the current key
    while (hasNext()) {
      if (current == null) { // initialize if current not set
        next();
        if (Bytes.compareTo(key, current.getRecordId()) < 0)
          break;
      } else {
        if (Bytes.compareTo(key, current.getRecordId()) > 0)
          next();
        else
          break;
      }
    }
    if (current != null)
      // if the desired value isn't here then set current to null
      if (Bytes.compareTo(key, current.getRecordId()) > 0) {
        current = null;
      }
    // now, we know that the current key is equal to or greater than the
    // seek input.

  }

  @Override
  public void markDoneWith() {
    this.done = true;
  }

  @Override
  public boolean isMarkedDoneWith() {
    return done;
  }
}

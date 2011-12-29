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
package com.bah.culvert.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * A range of keys.
 */
public class CRange implements WritableComparable<CRange> {

  /** Range for the entire table */
  public static final CRange FULL_TABLE_RANGE = new CRange(new byte[0]);

  private byte[] start;
  private boolean startInclusive;
  private byte[] end;
  private boolean endInclusive;

  /**
   * For use with {@link Writable#readFields(DataInput)}
   */
  public CRange() {
    this.start = new byte[0];
    this.end = new byte[0];
    this.startInclusive = true;
    this.endInclusive = true;
  }

  /**
   * Create a range that just includes the start key
   * @param start only key to use
   */
  public CRange(byte[] start) {
    this(start, true, start, true);
  }

  /**
   * Create a range that is [start, end]
   * @param start inclusive
   * @param end inclusive
   */
  public CRange(byte[] start, byte[] end) {
    this(start, true, end, true);
  }

  /**
   * Create a range and full specify which parts of the range of inclusive
   * @param start to start from
   * @param startInclusive range includes start
   * @param end to end at
   * @param endInclusive range includes end
   */
  public CRange(byte[] start, boolean startInclusive, byte[] end,
      boolean endInclusive) {
    if (start == null) {
      throw new NullPointerException("start is not allowed to be null");
    }
    if (end == null) {
      throw new NullPointerException("end is not allowed to be null");
    }
    this.start = start;
    this.startInclusive = startInclusive;
    this.end = end;
    this.endInclusive = endInclusive;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // Get the byte length
    int startLength = WritableUtils.readVInt(in);
    int endLength = WritableUtils.readVInt(in);

    start = new byte[startLength];
    end = new byte[endLength];

    // read the data
    in.readFully(start);
    in.readFully(end);

    startInclusive = in.readBoolean();
    endInclusive = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Write out the byte length
    WritableUtils.writeVInt(out, start.length);
    WritableUtils.writeVInt(out, end.length);

    // Write out the actual values
    out.write(start);
    out.write(end);

    out.writeBoolean(startInclusive);
    out.writeBoolean(endInclusive);
  }

  @Override
  public int compareTo(CRange other) {
    if (other == null) {
      return -1;
    }
    int compare = 0;
    if (start != null && other.start != null) {
      compare = WritableComparator.compareBytes(start, 0, start.length,
          other.start, 0, other.start.length);
      if (compare != 0) {
        return compare;
      }
    }

    if (startInclusive != other.startInclusive) {
      if (startInclusive) { // we are getting earlier data so it is "less"
        return 1;
      } else {
        return -1;
      }
    }

    if (end != null && other.end != null) {
      compare = WritableComparator.compareBytes(end, 0, end.length, other.end,
          0, other.end.length);
      if (compare != 0) {
        return compare;
      }
    }

    if (endInclusive != other.endInclusive) {
      if (endInclusive) { // we are getting later data so it is "greater"
        return -1;
      } else {
        return 1;
      }
    }

    return compare;
  }

  /**
   * @return the start key
   */
  public byte[] getStart() {
    return start;
  }

  /**
   * @return <tt>true</tt> range includes the start key, <tt>false</tt>
   *         otherwise
   */
  public boolean isStartInclusive() {
    return startInclusive;
  }

  /**
   * @return the end key
   */
  public byte[] getEnd() {
    return end;
  }

  /**
   * @return <tt>true</tt> range includes the end key, <tt>false</tt> otherwise
   */
  public boolean isEndInclusive() {
    return endInclusive;
  }

  @Override
  public boolean equals(Object other) {
    LexicographicBytesComparator bytesComparator = LexicographicBytesComparator.INSTANCE;
    if (!(other instanceof CRange)) {
      return false;
    }

    CRange compare = (CRange) other;

    if ((start == null && compare.start != null) || end == null
        && compare.end != null) {
      return false;
    }

    if ((start != null && compare.start == null) || end != null
        && compare.end == null) {
      return false;
    }
    if (bytesComparator.compare(start, compare.start) == 0) {
      if (startInclusive == compare.startInclusive) {
        if (bytesComparator.compare(end, compare.end) == 0) {
          if (endInclusive == compare.endInclusive) {
            return true;
          }
        }
      }
    }

    return false;
  }

  @Override
  public int hashCode() {
    int prime = 97;
    int startInclusiveHash = startInclusive ? prime : 0;
    int endInclusiveHash = endInclusive ? prime : 0;
    int i = 0;
    if (start != null) {
      i = start.hashCode() + startInclusiveHash;
    }
    if (end != null) {
      i += end.hashCode() + endInclusiveHash;
    }
    return i;
  }
}

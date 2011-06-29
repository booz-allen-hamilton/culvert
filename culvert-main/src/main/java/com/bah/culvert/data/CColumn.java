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
package com.bah.culvert.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.LexicographicBytesComparator;

/**
 * Stores the column family and qualifier. Used to pull out data
 */
public class CColumn implements WritableComparable<CColumn> {

  public static final CColumn ALL_COLUMNS = new CColumn(new byte[0]);
  private static final long DEFAULT_TIMESTAMP = Long.MAX_VALUE;

	private byte[] columnQualifier;
	private byte[] columnFamily;
	private long timestamp;
	
  /**
   * For use with {@link #readFields(DataInput)}
   */
  public CColumn() {

  }

	public CColumn(byte[] columnFamily){
    this(columnFamily, new byte[0], DEFAULT_TIMESTAMP);
	}
	
	public CColumn(byte[] columnFamily, byte[] columnQualifier){
    this(columnFamily, columnQualifier, DEFAULT_TIMESTAMP);
	}
	
	public CColumn(byte[] columnFamily, byte[] columnQualifier, long timestamp){
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
		this.timestamp = timestamp;
	}
	
	public byte[] getColumnQualifier(){
		return columnQualifier;
	}
	
	public byte[] getColumnFamily(){
		return columnFamily;
	}
	
	public long getTimestamp(){
		return timestamp;
	}

  @Override
  public void readFields(DataInput in) throws IOException {
    this.columnFamily = Bytes.readByteArray(in);
    this.columnQualifier = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeLong(this.timestamp);

  }

  @Override
  public int compareTo(CColumn other) {
    LexicographicBytesComparator comparator = LexicographicBytesComparator.INSTANCE;
    int compare = comparator.compare(this.columnFamily, other.columnFamily);
    if (compare == 0) {
      if (this.columnQualifier == null && other.columnQualifier == null)
        return 0;
      if (this.columnQualifier != null && other.columnQualifier != null) {
        compare = comparator.compare(this.columnQualifier,
            other.columnQualifier);
        if (compare == 0)
          compare = Long.valueOf(this.timestamp).compareTo(
              Long.valueOf(other.timestamp));
      }
    }
    return compare;
  }
}

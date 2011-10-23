/**
 * Copyright 2011 Booz Allenimport java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
h the License.  You may obtain a copy of the License at
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
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A key and value for the
 */
public class CKeyValue implements WritableComparable<CKeyValue>{

	private static final byte ZERO_BYTES[] = new byte[0];
	private static final long MAX_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Default value for the timestamp - useful for checking if the timestamp has
   * been initialized
   */
  public static final long DEFAULT_TIME_STAMP = MAX_TIMESTAMP;
  private byte[] rowId;
	private byte[] family;
	private byte[] qualifier;
  private long timestamp;
	private byte[] value;
	
  /**
   * For use with {@link Writable#readFields(DataInput)}
   */
	public CKeyValue(){}
	
  /**
   * Create a key and value with just a row ID and empty everything else
   * @param rowId of the key,value
   */
	public CKeyValue(byte[] rowId){
		this(rowId, ZERO_BYTES, ZERO_BYTES, MAX_TIMESTAMP, ZERO_BYTES);
	}

	public CKeyValue(byte[] rowId, byte[] family, byte[] qualifier){
		this(rowId, family, qualifier, MAX_TIMESTAMP, ZERO_BYTES);
	}
	
	public CKeyValue(byte[] rowId, byte[] family, byte[] qualifier, byte[] value){
		this(rowId, family, qualifier, MAX_TIMESTAMP, value);
	}
	
	public CKeyValue(CKeyValue other){
		this(other.rowId, other.family, other.qualifier, other.timestamp, other.value);
	}
	
	public CKeyValue(byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value){
		this.rowId = row;
		this.family = family;
		this.qualifier = qualifier;
		this.timestamp = timestamp;
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//Get the byte length
		int rowLength = WritableUtils.readVInt(in);
		int familyLength = WritableUtils.readVInt(in);
		int qualifierLength = WritableUtils.readVInt(in);
		int valueLength = WritableUtils.readVInt(in);
		
		this.rowId = new byte[rowLength];
		this.family = new byte[familyLength];
		this.qualifier = new byte[qualifierLength];
		this.value = new byte[valueLength];
		
		//read the data
		in.readFully(this.rowId);
		in.readFully(this.family);
		in.readFully(this.qualifier);
		in.readFully(this.value);
		this.timestamp = WritableUtils.readVLong(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//Write out the byte length
		WritableUtils.writeVInt(out, this.rowId.length);
		WritableUtils.writeVInt(out, this.family.length);
		WritableUtils.writeVInt(out, this.qualifier.length);
		WritableUtils.writeVInt(out, this.value.length);
		
		//Write out the actual values
		out.write(this.rowId);
		out.write(this.family);
		out.write(this.qualifier);
		out.write(this.value);
		
		WritableUtils.writeVLong(out, this.timestamp);
	}

  @Override
  public int compareTo(CKeyValue other) {
    int compare = this.compareKey(other);
    if (compare == 0)
      compare = WritableComparator.compareBytes(this.value, 0, this.value.length,
          other.value, 0, other.value.length);

    return compare;
  }

  /**
   * Compares the key portion of a KeyValue
   * @param other to compare to
   * @return 0 if they are equal, a byte wise comparison of rowId, family,
   *         qualifier, and then timestamp
   */
	public int compareKey(CKeyValue other) {
		int compare = WritableComparator.compareBytes(this.rowId, 0, this.rowId.length, other.rowId, 0, other.rowId.length);
		if(compare != 0){
			return compare;
		}
		
		compare = WritableComparator.compareBytes(this.family, 0, this.family.length, other.family, 0, other.family.length);
		if(compare != 0){
			return compare;
		}
		
		compare = WritableComparator.compareBytes(this.qualifier, 0, this.qualifier.length, other.qualifier, 0, other.qualifier.length);
		if(compare != 0){
			return compare;
		}
		
    // TODO add this back in when we are considering time stamps
    // if(this.timestamp < other.timestamp)
    // return -1;
    // else if(this.timestamp > other.timestamp)
    // return 1;
		
		return compare;
	}

	
  @Override
  public boolean equals(Object other) {
    if (other instanceof CKeyValue)
      return this.compareTo((CKeyValue) other) == 0;
    return false;
  }

  @Override
  public int hashCode() {
		int prime = 97;
		
		return prime * (this.rowId.hashCode() + this.family.hashCode() + this.qualifier.hashCode() + (int)(prime * this.timestamp) + this.value.hashCode());
	}
	
	/*
	 * Basic get methods
	 */
	public int getRowOffset() {
		return this.rowId.length;
	}

	public int getFamilyOffset() {
		return this.family.length;
	}

	public int getQualifierOffset() {
		return this.qualifier.length;
	}

	public int getValueOffset() {
		return this.value.length;
	}
	
	public byte[] getRowId() {
		return this.rowId;
	}

	public byte[] getFamily() {
		return this.family;
	}

	public byte[] getQualifier() {
		return this.qualifier;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public byte[] getValue() {
		return this.value;
	}

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CKeyValue: \n\trw:");
    sb.append(Arrays.toString(this.rowId));
    sb.append("(");
    sb.append(new String(this.rowId));
    sb.append(")");
    sb.append("\tcf:");
    sb.append(Arrays.toString(this.family));
    sb.append("(");
    sb.append(new String(this.family));
    sb.append(")");
    sb.append("\tcq:");
    sb.append(Arrays.toString(this.qualifier));
    sb.append("(");
    sb.append(new String(this.qualifier));
    sb.append(")");
    sb.append("\tts:");
    sb.append(this.timestamp);
    sb.append("\tvl:");
    sb.append(Arrays.toString(this.value));
    sb.append("(");
    sb.append(new String(this.value));
    sb.append(")");
    return sb.toString();
  }

}

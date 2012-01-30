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
package com.bah.culvert.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

/**
 * Utility class for converting to/from and using bytes
 */
public class Bytes {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  public static byte[] START_END_KEY;

  /**
   * Read a UTF-8 string from the byte array
   * @param bytes
   * @return the specified string
   */
  public static String stringFromBytes(byte[] bytes) {
    return new String(bytes, UTF8);
  }

  /**
   * Convert the specified {@link String} to UTF-8 bytes
   * @param string
   * @return byte [] representing the {@link String}
   */
  public static byte[] toBytes(String string) {
    return string.getBytes(UTF8);
  }

  /**
   * Conver the byte [] to an int
   * @param b
   * @return
   */
  public static int toInt(byte[] b) {
    if (b.length != Integer.SIZE / 8) {
      throw new IllegalArgumentException(
          "byte array wrong size for Integer conversion");
    }
    int n = 0;
    for (int i = 0; i < b.length; i++) {
      n <<= 8;
      n ^= b[i] & 0xFF;
    }
    return n;
  }
  
  /**
   * Convert the byte [] to a float
   * @param b
   * @return
   */
  public static float toFloat(byte[] b){
    if(b.length != Float.SIZE / 8){
	  throw new IllegalArgumentException(
	    "byte array wrong size for Float conversion");
    }
	return ByteBuffer.allocate(b.length).put(b).getFloat();
  }
  
  /**
   * Convert the byte [] to a double
   * @param b
   * @return
   */
  public static float toDouble(byte[] b){
    if(b.length != Double.SIZE / 8){
	  throw new IllegalArgumentException(
	    "byte array wrong size for Double conversion");
    }
	return ByteBuffer.allocate(b.length).put(b).getDouble();
  }

  /**
   * Convert the byte array to a {@link String}
   * @param b
   * @return
   */
  public static String toString(byte[] b) {
    if (b == null) {
      return null;
    }
    if (b.length == 0) {
      return "";
    }
    return new String(b);
  }

  /**
   * Lexographically compare two byte arrays
   * @param currentKey
   * @param key
   * @return
   */
  public static int compareTo(byte[] currentKey, byte[] key) {
    LexicographicBytesComparator byteComparator = LexicographicBytesComparator.INSTANCE;
    return byteComparator.compare(currentKey, key);
  }

  /**
   * Determine if two byte arrays are equal
   * @param currentKey
   * @param key
   * @return <tt>true</tt> if they are, <tt>false</tt> otherwise
   */
  public static boolean equals(byte[] currentKey, byte[] key) {
    LexicographicBytesComparator byteComparator = LexicographicBytesComparator.INSTANCE;
    return byteComparator.compare(currentKey, key) == 0 ? true : false;
  }

  /**
   * Sum the elements of the two arrays into new array
   * @param array1
   * @param array2
   * @return
   */
  public static byte[] add(byte[] array1, byte[] array2) {
    return ArrayUtils.addAll(array1, array2);

  }

  /**
   * Convert the integer to a byte []
   * @param i
   * @return
   */
  public static byte[] toBytes(int i) {
    int bytes = Integer.SIZE / 8;

    return ByteBuffer.allocate(bytes).putInt(i).array();
  }

  /**
   * Convert a short to a byte []
   * @param i
   * @return
   */
  public static byte[] toBytes(short i) {
    int bytes = Short.SIZE / 8;

    return ByteBuffer.allocate(bytes).putShort(i).array();
  }

  /**
   * Convert a character to a byte[]
   * @param c
   * @return
   */
  public static byte[] toBytes(char c) {
    int bytes = Character.SIZE / 8;
    return ByteBuffer.allocate(bytes).putChar(c).array();
  }

  /**
   * Convert the long to a byte []
   * @param i
   * @return
   */
  public static byte[] toBytes(long i) {
    int bytes = Long.SIZE / 8;

    return ByteBuffer.allocate(bytes).putLong(i).array();
  }

  /**
   * Concatenate two byte arrays.
   * @param values The values to concatenate.
   * @return The concatenated values.
   */
  public static byte[] catBytes(byte[]... values) {
    int totalSize = 0;
    for (byte[] value : values) {
      totalSize += value.length;
    }
    byte[] result = new byte[totalSize];

    int currentPos = 0;
    for (byte[] value : values) {
      System.arraycopy(value, 0, result, currentPos, value.length);
      currentPos += value.length;
    }
    return result;
  }

  /**
   * Arbitrary increment of the array as if it is a giant big-endian unsigned
   * binary number. The value is incremented in place, and the original
   * reference passed in is returned for convenience in method chaining, etc.
   * @param byteValue The byte array to increment.
   * @return The reference to the array passed as <code>byteValue</code>.
   */
  public static byte[] increment(byte[] byteValue) {
    byte carry = 1;
    for (int i = byteValue.length - 1; i >= 0 && carry != 0; i--) {
      for (int j = 0; j < 8 && carry != 0; j++) {
        byte newCarry = (byte) (carry & byteValue[i]);
        if (j != 7) {
          newCarry <<= 1;
        } else {
          newCarry = (byte) ((0xff & newCarry) >>> 7);
        }
        byteValue[i] = (byte) (byteValue[i] ^ carry);
        carry = newCarry;
      }
    }
    return byteValue;
  }

  /**
   * Write the bytes to the output stream
   * @param out to write to
   * @param bytes to write
   * @throws IOException on failure to write
   */
  public static void writeByteArray(DataOutput out, byte[] bytes)
      throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Read a a byte [] written with {@link #writeByteArray(DataOutput, byte[])}
   * @param input to read from
   * @return the first element in the input stream as a byte[]
   * @throws IOException on failure to read
   */
  public static byte[] readByteArray(DataInput input) throws IOException {
    byte[] bytes = new byte[input.readInt()];
    input.readFully(bytes);
    return bytes;
  }

  /**
   * Return the Lexicographically-sortable encoding of this double-precision
   * number. Please note that this encoding is NOT the same as the IEEE 1394
   * encoding.
   * @param doubleValue
   * @return the lexicographically sortable value of this double.
   */
  public static byte[] toBytes(double doubleValue) {
    int bytes = Double.SIZE / 8;
    return ByteBuffer.allocate(bytes).putDouble(doubleValue).array();
  }

  /**
   * Return the Lexicographically-sortable encoding of this double-precision
   * number. Please note that this encoding is NOT the same as the IEEE 1394
   * encoding.
   * @param floatValue
   * @return the lexicographically sortable value of this double.
   */
  public static byte[] toBytes(float floatValue) {
    int bytes = Float.SIZE / 8;
    return ByteBuffer.allocate(bytes).putFloat(floatValue).array();
    return null;
  }

  /**
   * Just write the boolean as a single bit, positive or negative, in the
   * conventional sense.
   * @param equals
   * @return byte representation of the boolean
   */
  public static byte[] toBytes(boolean equals) {
    if (equals)
      return new byte[] { 1 };
    else
      return new byte[] { 0 };
  }

  /**
   * Returns the very next possible value after a given byte array,
   * lexicographically speaking. This is simply the same byte array with a 0
   * appended to it.
   * @param value
   * @return an new byte array representing the next possible value
   */
  public static byte[] lexIncrement(byte[] value) {
    return Arrays.copyOf(value, value.length + 1);
  }

  /**
   * Gets the max value using the {@link LexicographicBytesComparator} to
   * compare values.
   * @param left
   * @param right
   * @return largest byte value. If equal, returns the right.
   */
  public static byte[] max(byte[] left, byte[] right) {
    int compare = LexicographicBytesComparator.INSTANCE.compare(left, right);

    if (compare > 0)
      return left;
    return right;
  }

  /**
   * Gets the min value using the {@link LexicographicBytesComparator} to
   * compare values.
   * @param left
   * @param right
   * @return smallest byte value. If equal, returns the right.
   */
  public static byte[] min(byte[] left, byte[] right) {
    int compare = LexicographicBytesComparator.INSTANCE.compare(left, right);

    if (compare < 0)
      return left;
    return right;
  }
}

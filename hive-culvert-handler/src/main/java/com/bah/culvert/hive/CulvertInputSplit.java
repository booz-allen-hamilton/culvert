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
package com.bah.culvert.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

import com.bah.culvert.constraints.Constraint;

/**
 * Store an InputSplit to read from an index table. Used then to create a
 * scanner over the data table
 */
@SuppressWarnings("deprecation")
public class CulvertInputSplit implements InputSplit, Writable {

  /**
   * The hosts that mapreduce should prefer when executing a task for this split
   */
  private Collection<String> locations;
  /** The query constraint that returns this fragment of input */
  private Constraint queryConstraint;

  /**
   * Create an InputSplit around the specified index table
   * 
   * @param index that this split is operating on
   * @param startKey start of the index to read
   * @param endKey end of the index to read
   */
  public CulvertInputSplit(Constraint queryConstraint,
      Collection<String> locations) {
    this.locations = locations;
    this.queryConstraint = queryConstraint;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLength()
   */
  @Override
  public long getLength() throws IOException {
    // Seems only to be used for sorting splits...
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() throws IOException {
    return this.locations.toArray(new String[locations.size()]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(in);
    String constraintClassName = in.readUTF();
    try {
      this.queryConstraint = Constraint.class.cast(Class.forName(
          constraintClassName).newInstance());
    } catch (InstantiationException e) {
      throw new IOException("Unable to instantiate " + constraintClassName, e);
    } catch (IllegalAccessException e) {
      throw new IOException(
          "Illegal access, make sure constructor and class are public: "
              + constraintClassName, e);
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to find class " + constraintClassName, e);
    }
    this.queryConstraint.readFields(in);
    this.locations = new ArrayList<String>(in.readInt());
    for (int i = 0; i < locations.size(); i++)
      locations.add(in.readUTF());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(queryConstraint.getClass().getName());
    queryConstraint.write(out);
    out.writeInt(locations.size());
    for (String location : locations)
      out.writeChars(location);
  }

  /**
   * Get the query constraint that represents this fragment of input.
   * 
   * @return The query constraint that represents this fragment of input.
   */
  public Constraint getQueryConstraint() {
    return queryConstraint;
  }

}
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
package com.bah.culvert;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.hive.CulvertHiveUtils;
import com.bah.culvert.hive.CulvertSerDe;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.Bytes;
import com.google.common.collect.Iterables;

@RunWith(JUnit4.class)
public class TestCulvertSerde {

  @Ignore(value="Failing due to unimplimented Bytes.toBytes(float) method ")
  @Test
  public void testSerdeInitializesAndSerializes() throws Throwable {
    // setup our columns from hive - foo and bar
    List<String> cols = Arrays.asList("foo", "bar");
    List<TypeInfo> types = Arrays.asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.floatTypeInfo);
    // we'll pass in the values as comma-separated text bytes
    byte[] seps = ",".getBytes();
    // use NULL to represent null
    Text nullSeq = new Text("NULL");
    // make the object inspector with the above settings
    ObjectInspector structInspector = LazyFactory.createLazyStructInspector(
        cols, types, seps, nullSeq, false, false, (byte) 0);
    // create the structs
    LazyStruct st1 = (LazyStruct) LazyFactory.createLazyObject(structInspector);
    LazyStruct st2 = (LazyStruct) LazyFactory.createLazyObject(structInspector);
    // create the values to put in the structs
    String val1 = "baz,0.004";
    String val2 = "boo,NULL";
    // need byte array refs to init structs
    ByteArrayRef barf1 = new ByteArrayRef();
    barf1.setData(val1.getBytes());
    ByteArrayRef barf2 = new ByteArrayRef();
    barf2.setData(val2.getBytes());
    // finally, set the struct contents.
    st1.init(barf1, 0, barf1.getData().length);
    st2.init(barf2, 0, barf2.getData().length);
    // create a culvert serde.
    CulvertSerDe serde = new CulvertSerDe();
    // create the properties and conf for the serde.
    // the serde doesn't expect anything in the conf, but will set values on it.
    Configuration conf = new Configuration();
    Properties tbl = new Properties();
    tbl.setProperty(Constants.LIST_COLUMNS, "foo,bar");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES, Constants.STRING_TYPE_NAME
        + ":" + Constants.FLOAT_TYPE_NAME);
    String mapping = ":key,fooFam:barQual";
    CulvertHiveUtils.setCulvertMapping(tbl, mapping);
    // initialize the serde
    serde.initialize(conf, tbl);
    // use the serde to make a put
    Put put = serde.serialize(st1, structInspector);
    // check the key values worked right
    CKeyValue[] kvs = Iterables.toArray(put.getKeyValueList(), CKeyValue.class);
    Assert.assertEquals(1, kvs.length);
    Assert.assertArrayEquals("baz".getBytes(), kvs[0].getRowId());
    Assert.assertArrayEquals("fooFam".getBytes(), kvs[0].getFamily());
    Assert.assertArrayEquals("barQual".getBytes(), kvs[0].getQualifier());
    Assert.assertArrayEquals(Bytes.toBytes(0.004f), kvs[0].getValue());
    put = serde.serialize(st2, structInspector);
    // our second value checks if it can properly handle nulls.
    kvs = Iterables.toArray(put.getKeyValueList(), CKeyValue.class);
    Assert.assertEquals(1, kvs.length);
    Assert.assertArrayEquals("boo".getBytes(), kvs[0].getRowId());
    Assert.assertArrayEquals(null, kvs[0].getFamily());
    Assert.assertArrayEquals(null, kvs[0].getQualifier());
    Assert.assertArrayEquals(null, kvs[0].getValue());

  }

}

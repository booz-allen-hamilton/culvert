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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.bah.culvert.util.Utils;

public class ResultTest {
	
	@Before
	public void setup(){}

	@Test
	public void testResultConstructors(){
		Result r1 = new Result();
		Result r2 = new Result(new byte[]{0,0,0,3});
		
		r1.setRecordId(new byte[]{0,0,0,4});
		
		Assert.assertArrayEquals(new byte[]{0,0,0,3}, r2.getRecordId());
		Assert.assertArrayEquals(new byte[]{0,0,0,4}, r1.getRecordId());
	}
	
	@Test
	public void testResultCopy(){
		Result r1 = new Result();
		Result r2 = new Result(new byte[]{0,0,0,3});
		r1.copy(r2);		
		
		Assert.assertArrayEquals(new byte[]{0,0,0,3}, r2.getRecordId());
		Assert.assertArrayEquals(new byte[]{0,0,0,3}, r1.getRecordId());
	}
	
	@Test
	public void testResultSetValues(){
		CKeyValue ckv = new CKeyValue(new byte[]{0,0,0,8});
		Result r1 = new Result();
		Result r2 = new Result(new byte[]{0,0,0,3});
		r1.setValues((CKeyValue [])null);
		r2.setValues(ckv);
		
    Assert.assertNotNull(r1.getKeyValues());
		Assert.assertNotNull(r2.getKeyValues());
	}
	
	@Test
  public void testResultWrite() throws InstantiationException,
      IllegalAccessException, IOException {
		
    // test simple ckv
		CKeyValue ckv = new CKeyValue(new byte[]{0,0,0,8});
		Result r1 = new Result(ckv);
    Result ret = (Result) Utils.testReadWrite(r1);
    assertEquals(1, ret.getKeyValues().size());
    assertEquals(ckv, ret.getKeyValues().get(0));

    // test more complex ckv
    ckv = new CKeyValue(new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3, 4 });
    r1 = new Result(ckv);
    ret = (Result) Utils.testReadWrite(r1);
    assertEquals(1, ret.getKeyValues().size());
    assertEquals(ckv, ret.getKeyValues().get(0));
	}
	
	

}

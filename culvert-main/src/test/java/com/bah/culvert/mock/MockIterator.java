/**
 * Copyright 2011 Boozimport java.util.UUID;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.util.Bytes;
ache License, Version 2.0 (the
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
package com.bah.culvert.mock;

import java.util.UUID;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.Result;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.util.Bytes;

public class MockIterator implements SeekingCurrentIterator
{
	private final Result[] results;
	private int index = 0; // location in my array
	private int previous_index = 0;
	private final int size; // size oy my array
	boolean done = false;

	/**
	 * Construct MockIterator with unique IDs
	 * 
	 * used to have a set with all distinct uids
	 */
	public MockIterator(int size)
	{
		this.size = size;
		results = new Result[size];
		for (int i = 0; i < size; i++)
		{
			UUID idOne = UUID.randomUUID();
			results[i] = new Result(Bytes.toBytes(idOne.toString()), new CKeyValue());
		}
	}

	/**
	 * Construct MockIterator with given ids
	 * 
	 * used to have a set with specific uids
	 */
	public MockIterator(int... ids)
	{
		size = ids.length;
		results = new Result[size];
		for (int i = 0; i < size; i++)
		{
			results[i] = new Result(Bytes.toBytes(ids[i]), new CKeyValue());
		}
	}

	/**
	 * Construct MockIterator with common uids
	 * 
	 * used to have a set with all common uids
	 */
	public MockIterator(int size, boolean identity)
	{
		this.size = size;
		results = new Result[size];
		for (int i = 0; i < size; i++)
		{
			results[i] = new Result(Bytes.toBytes(i + 1), new CKeyValue());
		}
	}

	@Override
  public boolean hasNext()
	{
		return index < size;
	}

	@Override
  public Result next()
	{
		if (index < size)
		{
			previous_index = index;
			return results[index++];
		}
		else
		{
			previous_index = -1;
			return null;
		}
	}

	@Override
  public void remove()
	{
		// optional function, not implementing.
	}

	@Override
  public Result current()
	{
		if (index == 0)
		{
			return results[0];
		}
		else if (previous_index != -1)
		{
			return results[index - 1];
		}
		else
		{
			return null;
		}
	}

	// advance to this key.
	@Override
  public void seek(byte[] key)
	{
		int i = index;
		for (; i < size; i++)
		{
			if (Bytes.compareTo(key, results[i].getRecordId()) <= 0)
			{
				return;
			}
			else
			{
				index++;
			}
		}
		if (i == size)
		{
			index = size;
		}
	}

	public void seek(int i)
	{
		seek(Bytes.toBytes(i));
	}

	@Override
  public void markDoneWith()
	{
		done = true;
	}

	@Override
  public boolean isMarkedDoneWith()
	{
		return done;
	}
}

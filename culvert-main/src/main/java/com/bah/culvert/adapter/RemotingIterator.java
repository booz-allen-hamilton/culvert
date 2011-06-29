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
package com.bah.culvert.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.adapter.RemotingIterator.RemoteQueryStatusHolder.QueryStatus;
import com.bah.culvert.configuration.CConfiguration;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.ResultComparator;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.MultiRuntimeException;

/**
 * Used to perform a remote parallel fetch.
 */
class RemotingIterator implements Iterator<Result> {

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(RemotingIterator.class);

  /**
   * Properly implements a lazy-loaded singleton for remote query status
   */
  protected static final class RemoteQueryStatusHolder {
    public static final Map<String, QueryStatus> QUERY_STATUS_MAP = new ConcurrentHashMap<String, QueryStatus>();

    public static final class QueryStatus {
      Iterator<Result> iterator;
      long lastAccessTime;
    }
  }

  /**
   * Starts a remote query and puts the first timeout thread in the execution
   * service.
   */
  public static final class RemoteQueryStarter extends RemoteOp<String> {
    /**
     * How long to keep the QueryStatus alive before timing out.
     */
    private static final String REMOTE_QUERY_STATUS_KEEPALIVE_SETTING = "culvert.query.timeout.status.keepalive";
    public static long QUERY_KEEP_ALIVE;

    /**
     * Properly implements a lazy-loaded singleton for remote work queue.
     */
    private static final class ExecutorServiceHolder {
      private static final String REMOTE_QUERY_CORE_POOL_SETTING = "culvert.query.timeout.executor.core.pool";
      private static final String REMOTE_QUERY_MAX_POOL_SETTING = "culvert.query.timeout.executor.max.pool";
      private static final String REMOTE_QUERY_KEEPALIVE_SETTING = "culvert.query.timeout.executor.keepalive";
      private static final String REMOTE_QUERY_QUEUE_SIZE_SETTING = "culvert.query.timeout.executor.queuesize";

      /**
       * The executor service (one per remote server) used to clean up queries
       * that have reached their timeout.
       */
      public static final ExecutorService E_SERVICE;
      static {
        Configuration conf = CConfiguration.getDefault();
        long defaultKeepAlive = 60000;

        int coreSize = conf.getInt(REMOTE_QUERY_CORE_POOL_SETTING, 2);
        int maxSize = conf.getInt(REMOTE_QUERY_MAX_POOL_SETTING, 64);
        long executorKeepAlive = conf.getLong(REMOTE_QUERY_KEEPALIVE_SETTING,
            defaultKeepAlive);
        QUERY_KEEP_ALIVE = conf.getLong(REMOTE_QUERY_STATUS_KEEPALIVE_SETTING,
            defaultKeepAlive / 2);
        int queueCapacity = conf.getInt(REMOTE_QUERY_QUEUE_SIZE_SETTING,
            1 << 20);
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
            queueCapacity);
        E_SERVICE = new ThreadPoolExecutor(coreSize, maxSize,
            executorKeepAlive, TimeUnit.MILLISECONDS, queue);
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bah.culvert.adapter.RemoteOp#call(java.lang.Object[])
     * 
     * Input: Index index, byte[] startKey, byte[] endKey Output: String id -
     * UUID identifying the Iterator
     * 
     * This method take the Index and retrieve an iterator for the provided
     * start and end keys. The iterator will return a Result in sorted order as
     * defined by the ResultComparator {@see
     * com.bah.culvert.data.ResultComparator} from a TreeSet. The iterator and
     * the current time are stored in the QueryStatus object.
     * 
     * The QueryStatus is stored in the QueryStatusMap identified by the
     * generated UUID string. The QueryStatus will be periodically checked for
     * recent access. If it hasn't been recently accessed, then the QueryStatus
     * object will be removed from the map and will be unaccessible.
     */
    @Override
    public String call(Object... args) throws Exception {
      /*
       * presently, we store results in memory... if this becomes an issue a
       * temporary table might be more appropriate
       */
      // Check the arguments
      if (args.length != 3) {
        throw new Exception("Expected 3 arguments for Index, byte[], byte[]."
            + " Recieved " + args.length + " arguments");
      }
      if (!(args[0] instanceof Index)) {
        throw new Exception("First argument should be an Index");
      }
      if (!(args[1] instanceof byte[])) {
        throw new Exception("Second argument should be a byte[]");
      }
      if (!(args[2] instanceof byte[])) {
        throw new Exception("Third argument should be a byte[]");
      }

      // Get the arguments
      Index index = (Index) args[0];
      // We only want the range that exists within the region.
      byte[] start = Bytes.max(this.getLocalTableAdapter().getStartKey(),
          (byte[]) args[0]);
      byte[] end = Bytes.min(this.getLocalTableAdapter().getEndKey(),
          (byte[]) args[1]);

      // Create the row ID which will be used to identify the query results
      final String id = UUID.randomUUID().toString();

      // Get the results and store them in a list of sorting
      List<Result> resultList = new ArrayList<Result>();
      Iterator<Result> resultIterator = index.handleGet(start, end);
      while (resultIterator.hasNext()) {
        resultList.add(resultIterator.next());
      }
      Collections.sort(resultList, new ResultComparator());

      // Create the QueryStatus object with the current time as the
      // last access time
      long now = System.currentTimeMillis();
      Iterator<Result> sortedIterator = resultList.iterator();

      final QueryStatus queryStatus = new QueryStatus();
      queryStatus.iterator = sortedIterator;
      queryStatus.lastAccessTime = now;

      // Store the QueryStatus object
      RemoteQueryStatusHolder.QUERY_STATUS_MAP.put(id, queryStatus);

      // Create the service to check if the QueryStatus has been
      // recently accessed.
      ExecutorServiceHolder.E_SERVICE.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Thread.sleep(QUERY_KEEP_ALIVE);

          // Compare the last access time to see if the timeout
          // has occurred
          long now = System.currentTimeMillis();

          if (now - queryStatus.lastAccessTime > QUERY_KEEP_ALIVE) {
            // Remove the query
            RemoteQueryStatusHolder.QUERY_STATUS_MAP.remove(id);
          } else {
            // Resubmit this timeout check
            ExecutorServiceHolder.E_SERVICE.submit(this);
          }
          return null;
        }
      });

      return id;
    }
  }

  /**
   * Runs in the remote context to see if that (portion) of the query has a new
   * result.
   */
  public static final class HasNextChecker extends RemoteOp<Boolean> {

    @Override
    public Boolean call(Object... args) throws Exception {
      String id = (String) args[0];
      QueryStatus status = RemoteQueryStatusHolder.QUERY_STATUS_MAP.get(id);
      if (status == null) {
        throw new RuntimeException("Query timeout or invalid query.");
      } else {
        // Update the access time
        status.lastAccessTime = System.currentTimeMillis();
        return status.iterator.hasNext();
      }
    }

  }

  /**
   * Runs in the remote context to get the next result (if any) for that portion
   * of the query.
   */
  public static final class NextResultGetter extends RemoteOp<Result> {
    @Override
    public Result call(Object... args) throws Exception {
      String id = this.getConf().get(QUERY_ID_SETTING);
      QueryStatus status = RemoteQueryStatusHolder.QUERY_STATUS_MAP.get(id);
      if (status == null) {
        throw new RuntimeException("Query timeout or invalid query.");
      } else {
        // Update the access time
        status.lastAccessTime = System.currentTimeMillis();
        return status.iterator.next();
      }
    }
  }

  /**
   * The query id setting in the conf, used to pass query id to remote queries
   */
  private static final String QUERY_ID_SETTING = "culvert.queryid";
  /** The remote table to connect to and retrieve results from */
  private final TableAdapter remoteTable;
  /** The configuration to use, just holds the query id for now */
  private final Configuration conf;
  /**
   * The result queue to hold local results in. Generally makes implementing the
   * parallel fetch easier.
   */
  private final PriorityQueue<Result> resultQueue = new PriorityQueue<Result>();
  private final byte[] remoteStart;
  private final byte[] remoteEnd;

  /**
   * @param remoteTable
   *          The remote table to connect to.
   * @param queryId
   *          The query id to use.
   * @param remoteRangeKeys
   *          The range keys to query over.
   */
  public RemotingIterator(TableAdapter remoteTable, byte[] remoteStart,
      byte[] remoteEnd) {
    this.remoteTable = remoteTable;
    this.remoteStart = remoteStart;
    this.remoteEnd = remoteEnd;
    this.conf = new Configuration();
  }

  @Override
  public boolean hasNext() {
    /*
     * looks in the queue first, to see if there's any results pending locally,
     * then looks on the remote server if not
     */
    if (resultQueue.size() > 0) {
      return true;
    }
    boolean next = false;
    List<Boolean> remoteStates = remoteTable.remoteExec(remoteStart, remoteEnd,
        HasNextChecker.class, conf.get(QUERY_ID_SETTING));
    List<Throwable> remoteExceptions = new ArrayList<Throwable>(0);
    for (Boolean future : remoteStates) {
      next |= future;
    }
    if (remoteExceptions.size() > 0) {
      throw MultiRuntimeException.get(remoteExceptions);
    }
    return next;
  }

  @Override
  public Result next() {
    /*
     * Add results to a result queue if we're out, if not just return what's
     * there.
     */
    if (resultQueue.size() > 0) {
      return resultQueue.poll();
    } else {
      List<Result> results = remoteTable.remoteExec(remoteStart, remoteEnd,
          NextResultGetter.class, conf.get(QUERY_ID_SETTING));
      List<Throwable> exceptions = new ArrayList<Throwable>(0);
   
      resultQueue.addAll(results);
      
      if (exceptions.size() > 0) {
        throw MultiRuntimeException.get(exceptions);
      }
      if (resultQueue.size() == 0) {
        throw new ArrayIndexOutOfBoundsException(
            "No more results available from remote servers");
      }
      return resultQueue.poll();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}

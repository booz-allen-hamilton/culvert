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

package com.bah.culvert.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.bah.culvert.Client;
import com.bah.culvert.constraints.And;
import com.bah.culvert.constraints.Constraint;
import com.bah.culvert.constraints.IndexRangeConstraint;
import com.bah.culvert.data.CRange;
import com.bah.culvert.data.Result;
import com.bah.culvert.data.index.Index;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.LexicographicBytesComparator;

@SuppressWarnings("deprecation")
public class CulvertInputFormat implements InputFormat<NullWritable, Result> {

  @Override
  public RecordReader<NullWritable, Result> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
    CulvertRecordReader crr = new CulvertRecordReader((CulvertInputSplit) split);
    return crr;
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    String expression = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (expression == null) {
      // return what??
    }
    ExprNodeDesc filterExpr = Utilities.deserializeExpression(expression, conf);
    CulvertPredicateHandlerDelegate predicateHandler = new CulvertPredicateHandlerDelegate();
    CulvertIndexMapping[] culvertIndexMappings = CulvertHiveUtils
        .getIndexMappings(conf);
    /*
     * there should be no residual predicate, just the pushed predicate, since
     * we already handled that in the storage handler.
     */
    List<IndexSearchCondition> indexSearchConditions = predicateHandler
        .decomposePredicateToList(filterExpr, culvertIndexMappings);
    List<Constraint> compiledConstraints = new ArrayList<Constraint>();
    Map<String, CRange> rangeMap = new HashMap<String, CRange>();
    // get start and end ranges from the conditions
    for (IndexSearchCondition condition : indexSearchConditions) {
      /*
       * The hive column is a simple string -- easy This column name will let us
       * get an index from the culvert configuration specified in the conf.
       */
      String hiveColumn = condition.getColumnDesc().getColumn();
      /*
       * the operation maps to the op name specified in
       * CulvertPredicateHandlerDelegate.createPredicateAnalyzerForCulvert()
       */
      String operation = condition.getComparisonOp();
      /*
       * We need to manipuate this quite a bit- we need to turn it into an
       * object and an object inspector, and then use the object inspector to
       * find a byte value for the key.
       */
      ExprNodeConstantEvaluator nodeEvaluator = new ExprNodeConstantEvaluator(
          condition.getConstantDesc());
      PrimitiveObjectInspector inspector;
      Object value;
      // we can assume that the inspector is a primitive
      try {
        inspector = (PrimitiveObjectInspector) nodeEvaluator.initialize(null);
        value = nodeEvaluator.evaluate(null);
      } catch (HiveException e) {
        throw new RuntimeException(
            "Unable to get constant portion of query expression expression "
                + condition, e);
      }
      Object primitive = inspector.getPrimitiveJavaObject(value);
      // look for the storage type in the column mapping
      CulvertIndexMapping culvertIndexMapping = getMapping(hiveColumn,
          culvertIndexMappings);

      if (culvertIndexMapping == null) {
        throw new IllegalArgumentException(
            "Could not find a culvert mapping for this hive column "
                + hiveColumn + " out of "
                + Arrays.toString(culvertIndexMappings));
      }
      // we have a utility to get the byte representation based on the hive
      // column type and the actual primitive itself.
      byte[] culvertValue = CulvertHiveUtils.getBytesForPrimitiveObject(
          primitive, culvertIndexMapping.getType());
      CRange range = rangeMap.get(hiveColumn);
      if (range == null) {
        // ok, start a new range
        // we know we'll always want to insert for this operation
        if (operation.equals(GenericUDFOPEqual.class.getName())) {
          // =
          range = new CRange(culvertValue);
        } else if (operation.equals(GenericUDFOPGreaterThan.class.getName())) {
          // >
          range = new CRange(Bytes.lexIncrement(culvertValue),
              Bytes.START_END_KEY);
        } else if (operation.equals(GenericUDFOPEqualOrGreaterThan.class
            .getName())) {
          // =>
          range = new CRange(culvertValue, Bytes.START_END_KEY);
        } else if (operation
            .equals(GenericUDFOPEqualOrLessThan.class.getName())) {
          // =<
          range = new CRange(Bytes.START_END_KEY, culvertValue);
        }
        /*
         * TODO we can't implement < because lexDecrement is undefined (in math
         * speak). Need to work around that by pushing down inclusive/exclusive
         * on the CRange.
         */
        rangeMap.put(hiveColumn, range);
      } else {
        LexicographicBytesComparator c = LexicographicBytesComparator.INSTANCE;
        if (operation.equals(GenericUDFOPEqual.class.getName())) {
          // = (if two conflicting equals, the whole predicate becomes a no-op
          if (c.compare(range.getStart(), culvertValue) != 0) {
            rangeMap.clear();
            return new InputSplit[0];
          }
        } else if (operation.equals(GenericUDFOPGreaterThan.class.getName())) {
          // >
          // narrow the existing range
          byte[] exclusiveValue = Bytes.lexIncrement(culvertValue);
          int comp = c.compare(exclusiveValue, range.getEnd());
          if (comp < 1) {
            rangeMap.put(hiveColumn,
                new CRange(Bytes.max(exclusiveValue, range.getStart())));
          } else if (comp == 0) {
            // no-op - this is the same as what we're doing already
          } else {
            // if we get conflicting statements, kill it all...
            rangeMap.clear();
            return new InputSplit[0];
          }
        } else if (operation.equals(GenericUDFOPEqualOrGreaterThan.class
            .getName())) {
          // =>
          // narrow the existing range
          int comp = c.compare(culvertValue, range.getEnd());
          if (comp < 1) {
            rangeMap.put(hiveColumn,
                new CRange(Bytes.max(culvertValue, range.getStart())));
          } else if (comp == 0) {
            // no-op - this is the same as what we're doing already
          } else {
            // if we get conflicting statements, kill it all...
            rangeMap.clear();
            return new InputSplit[0];
          }
        } else if (operation
            .equals(GenericUDFOPEqualOrLessThan.class.getName())) {
          // =<
          int comp = c.compare(culvertValue, range.getStart());
          if (comp > 1) {
            rangeMap.put(hiveColumn,
                new CRange(Bytes.min(culvertValue, range.getEnd())));
          } else if (comp == 0) {
            // no-op
          } else {
            // if we get conflicting statements, kill it...
            rangeMap.clear();
            return new InputSplit[0];
          }
        }
      }
    }
    // <-- end getting start/end ranges
    Client culvertClient = new Client(conf);

    Index[] culvertIndices = culvertClient.getIndicesForTable(CulvertHiveUtils
        .getCulvertTable(conf));
    // create a map with the broken up ranges
    SortedMap<String, List<Constraint>> brokenRanges = new TreeMap<String, List<Constraint>>();
    for (Entry<String, CRange> rangeEntry : rangeMap.entrySet()) {
      String hiveColumn = rangeEntry.getKey();
      CRange totalRange = rangeEntry.getValue();
      Index indexToUse = getIndexForHiveColumn(hiveColumn,
          culvertIndexMappings, culvertIndices);
      /*
       * There should be an index from when we did the predicate pushdown in the
       * storage handler
       */
      if (indexToUse == null) {
        throw new RuntimeException("No indices exist over the requested column");
      }
      /*
       * Set the start and end on the splits to start where the overall range
       * starts and ends so we don't query the entire shard, which would give
       * the wrong result.
       */
      List<CRange> startEnds = indexToUse.getSplits();
      List<Constraint> newRanges = new ArrayList<Constraint>();
      if (startEnds.size() == 1) {
        newRanges.add(new IndexRangeConstraint(indexToUse, totalRange));
        brokenRanges.put(hiveColumn, newRanges);
        continue;
      }
      assert (startEnds.size() != 0);
      CRange firstShard = startEnds.get(0);
      CRange first = new CRange(totalRange.getStart(), firstShard.getEnd());
      newRanges.add(new IndexRangeConstraint(indexToUse, first));
      for (int i = 1; i < startEnds.size() - 1; i++) {
        newRanges.add(new IndexRangeConstraint(indexToUse, startEnds.get(i)));
      }
      CRange lastShard = startEnds.get(startEnds.size() - 1);
      CRange last = new CRange(lastShard.getStart(), totalRange.getEnd());
      newRanges.add(new IndexRangeConstraint(indexToUse, last));
      brokenRanges.put(hiveColumn, newRanges);
    }
    // <-- finished creating fragment map

    /*
     * Finally, take the cross product of all the fragments in order to find the
     * splits.
     */
    List<List<Constraint>> andRangeList = recursiveBuildRangeTuples(brokenRanges);
    InputSplit[] splits = new InputSplit[andRangeList.size()];
    for (int i = 0; i < splits.length; i++) {
      List<Constraint> constraints = andRangeList.get(i);
      Set<String> locations = new HashSet<String>();
      for (Constraint c : constraints) {
        locations.addAll(((IndexRangeConstraint) c).getIndex()
            .getPreferredHosts());
      }
      // add field selection too...
      List<Constraint> fieldSelectingConstraints = new ArrayList<Constraint>(
          constraints.size());
      // TODO - wrap these constraints in field selecting constraints now that
      // we've pulled the preferred hosts out of them
      splits[i] = new CulvertInputSplit(new And(constraints), locations);
    }
    return null;
  }

  private Index getIndexForHiveColumn(String hiveColumn,
      CulvertIndexMapping[] culvertIndexMappings, Index[] culvertIndices) {
    CulvertIndexMapping culvertIndexMapping = getMapping(hiveColumn,
        culvertIndexMappings);
    Index indexToUse = null;
    /*
     * iterate the indices to find one to use on the column, in the future we
     * should select indices intelligently
     */
    for (Index ix : culvertIndices) {
      LexicographicBytesComparator c = LexicographicBytesComparator.INSTANCE;
      if (c
          .compare(ix.getColumnFamily(), culvertIndexMapping.getColumnFamily()) == 0
          && c.compare(ix.getColumnQualifier(),
              culvertIndexMapping.getColumnQualifier()) == 0) {
        indexToUse = ix;
      }
    }
    return indexToUse;
  }

  /**
   * Builds the cross product of each element in a map of CRange Lists.
   * @param brokenRanges
   * @return
   */
  private static List<List<Constraint>> recursiveBuildRangeTuples(
      final SortedMap<String, List<Constraint>> brokenRanges) {
    int size = brokenRanges.size();
    /*
     * Special 0 case - no ranges, then return an empty list. Happens if there's
     * an empty query, but not during normal operation.
     */
    if (size == 0) {
      return new ArrayList<List<Constraint>>();
    }
    /*
     * Special 1 case - is the base case for normal operation (ie. the query
     * isn't empty)
     */
    else if (size == 1) {
      Entry<String, List<Constraint>> entry = brokenRanges.entrySet()
          .iterator().next();
      List<List<Constraint>> rangeList = new ArrayList<List<Constraint>>();
      for (Constraint range : entry.getValue()) {
        ArrayList<Constraint> newList = new ArrayList<Constraint>();
        newList.add(range);
        rangeList.add(newList);
      }
      return rangeList;
    }
    /*
     * General case - we need to do the cross product of the different map
     * elements.
     */
    else {
      List<List<Constraint>> newListList = new ArrayList<List<Constraint>>();
      List<List<Constraint>> nextListList = recursiveBuildRangeTuples(brokenRanges
          .tailMap(getSecondKey(brokenRanges)));
      for (Constraint range : brokenRanges.entrySet().iterator().next()
          .getValue()) {
        for (List<Constraint> nextList : nextListList) {
          nextList = new ArrayList<Constraint>(nextList);
          nextList.add(range);
          newListList.add(nextList);
        }
      }
      return newListList;
    }
  }

  /**
   * Gets the second key in the keySet of a sortedMap.
   * @param brokenRanges
   *          The sorted map to get the second key of
   * @return The second key in the map, or null if the map has less than two
   *         keys.
   */
  private static String getSecondKey(SortedMap<String, ?> brokenRanges) {
    Iterator<String> it = brokenRanges.keySet().iterator();
    if (it.hasNext())
      it.next();
    else
      return null;
    if (it.hasNext())
      return it.next();
    else
      return null;
  }

  /**
   * Get the index mapping for particular hive column from an array of index
   * mappings.
   * @param hiveColumn
   *          The hive column to look for.
   * @param culvertIndexMappings
   *          The index mappings to look through.
   * @return The first index mapping with the name of the hive column.
   */
  private static CulvertIndexMapping getMapping(String hiveColumn,
      CulvertIndexMapping[] culvertIndexMappings) {
    for (CulvertIndexMapping mapping : culvertIndexMappings) {
      if (mapping.getHiveColumn().equals(hiveColumn)) {
        return mapping;
      }
    }
    throw new RuntimeException("Cannot find culvert mapping for column "
        + hiveColumn + " in mappings: " + Arrays.toString(culvertIndexMappings));
  }

}

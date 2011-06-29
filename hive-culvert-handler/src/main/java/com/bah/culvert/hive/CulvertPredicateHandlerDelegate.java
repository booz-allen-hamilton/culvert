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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

/**
 * Delegate class that handles storage predicate parsing. This is used in two
 * places (somewhat redundantly) by both the storage handler and the input
 * format's get splits method.
 */
@SuppressWarnings("deprecation")
public class CulvertPredicateHandlerDelegate implements
    HiveStoragePredicateHandler {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler#
   * decomposePredicate(org.apache.hadoop.mapred.JobConf,
   * org.apache.hadoop.hive.serde2.Deserializer,
   * org.apache.hadoop.hive.ql.plan.ExprNodeDesc)
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
      Deserializer deserializer, ExprNodeDesc predicate) {
    /*
     * first we need to make sure we have a culvert serde, because we need the
     * index mapping from it.
     */
    // if (!(deserializer instanceof CulvertSerDe)) {
    // throw new IllegalArgumentException(
    // "deserializer must be an instance of CulvertSerDe");
    // }
    // CulvertSerDe culvertSerde = (CulvertSerDe) deserializer;
    return decomposePredicate(predicate,
        CulvertHiveUtils.getIndexMappings(jobConf));

  }

  /**
   * Decomposes a predicate using only the CulvertIndexMappings. This is
   * convenient because the SerDe is not reasonably available in the input
   * format. Additionally, the JobConf is unnecessary.
   * @param predicate
   *          The predicate to decompose.
   * @param culvertIndexMappings
   *          The index mappings.
   * @return The decomposed predicate.
   * @see #decomposePredicate(JobConf, Deserializer, ExprNodeDesc)
   */
  public DecomposedPredicate decomposePredicate(ExprNodeDesc predicate,
      CulvertIndexMapping[] culvertIndexMappings) {
    List<IndexSearchCondition> searchConditions = decomposePredicateToList(
        predicate, culvertIndexMappings);
    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    /*
     * once we have the culvert mappings, we can create a predicate analyzer.
     * this is done by invoking a factory with a pretty large string of
     * arguments, so it gets encapsulated in another static method for
     * readability
     */
    IndexPredicateAnalyzer analyzer = createPredicateAnalyzerForCulvert(culvertIndexMappings);
    decomposedPredicate.pushedPredicate = analyzer
        .translateSearchConditions(searchConditions);
    /*
     * this is done in decomposePredicateToList() earlier... should figure out a
     * way to redesign this to make this whole thing more efficient
     */
    decomposedPredicate.residualPredicate = analyzer.analyzePredicate(
        predicate, searchConditions);
    // finally, return the decomposed predicate
    return decomposedPredicate;
  }

  /**
   * Decomposes predicate, returning a simple index search condition list
   * instead of a full blown DecomposedPredicate.
   * @param predicate
   * @param culvertIndexMappings
   * @return The list of search conditions. These can be interpreted as the
   *         logical "and" of all the underlying expressions.
   */
  public List<IndexSearchCondition> decomposePredicateToList(
      ExprNodeDesc predicate, CulvertIndexMapping[] culvertIndexMappings) {
    /*
     * once we have the culvert mappins, we can create a predicate analyzer.
     * this is done by invoking a factory with a pretty large string of
     * arguments, so it gets encapsulated in another static method for
     * readability
     */
    IndexPredicateAnalyzer analyzer = createPredicateAnalyzerForCulvert(culvertIndexMappings);
    /*
     * The analyzer is kind of stupid. It basically returns the decomposed
     * predicate, but we need to invoke two different methods on it to get the
     * decomposed predicate
     */
    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    analyzer.analyzePredicate(predicate, searchConditions);
    return searchConditions;
  }

  /**
   * Creates an {@link IndexPredicateAnalyzer} for culvert index analysis.
   * <p>
   * The index analyzer effectively expresses what types of operations can be
   * handled by culvert efficiently and returns a decomposed and residual
   * predicate which expresses what culvert can and cannot handle.
   * @param indices
   *          The array of culvert index mappings that represents the mapping of
   *          HIVE columns to bigtable column families, qualifiers, or the row
   *          id.
   * @return An analyzer that will help us break out the predicates.
   */
  private static IndexPredicateAnalyzer createPredicateAnalyzerForCulvert(
      CulvertIndexMapping[] indices) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // Set the allowed operations
    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());

    // Set the allowed columns that we can do the predicate over.
    for (CulvertIndexMapping culvertIndex : indices) {
      analyzer.allowColumnName(culvertIndex.getHiveColumn());
    }

    return analyzer;
  }

}

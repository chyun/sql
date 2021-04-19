/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.expression.aggregation;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.LONG;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.aggregation.ApproxPercentileAggregator.ApproxPercentileState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ApproxPercentileAggregatorTest extends AggregationTest {
  @Test
  public void approx_percentile_with_percentile_less_0() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> aggregation(
            dsl.approxPercentile(DSL.literal(-1), DSL.ref("integer_value", INTEGER)), tuples)
    );
    assertEquals("percentile should be in [0,100] in "
        + "approx_percentile aggregation, got [-1.0]", exception.getMessage());
  }

  @Test
  public void approx_percentile_with_percentile_greater_100() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> aggregation(
            dsl.approxPercentile(DSL.literal(101), DSL.ref("integer_value", INTEGER)), tuples)
    );
    assertEquals("percentile should be in [0,100] in "
        + "approx_percentile aggregation, got [101.0]", exception.getMessage());
  }

  @Test
  public void approx_percentile_integer_field_expression() {
    ExprValue result = aggregation(
        dsl.approxPercentile(DSL.literal(99.9), DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_long_field_expression() {
    ExprValue result = aggregation(
        dsl.approxPercentile(DSL.literal(99.9), DSL.ref("long_value", LONG)), tuples);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_float_field_expression() {
    ExprValue result = aggregation(
        dsl.approxPercentile(DSL.literal(99.9), DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_double_field_expression() {
    ExprValue result = aggregation(
        dsl.approxPercentile(DSL.literal(99.9), DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_arithmetic_expression() {
    ExprValue result = aggregation(dsl.approxPercentile(DSL.literal(99.9),
        dsl.multiply(DSL.ref("integer_value", INTEGER),
            DSL.literal(ExprValueUtils.integerValue(10)))), tuples);
    assertEquals(40.0, result.value());
  }

  @Test
  public void approx_percentile_string_field_expression() {
    ApproxPercentileAggregator approxPercentileAggregator = (ApproxPercentileAggregator)
        new ApproxPercentileAggregator(ImmutableList.of(DSL.ref("string_value", STRING)),
            ExprCoreType.STRING).initValues(
            Arrays.asList(DSL.literal(99.9)));
    ApproxPercentileState approxPercentileState = approxPercentileAggregator.create();
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> approxPercentileAggregator
            .iterate(
                ExprValueUtils.tupleValue(ImmutableMap.of("string_value", "m")).bindingTuples(),
                approxPercentileState)
    );
    assertEquals("invalid to get doubleValue from value of type STRING", exception.getMessage());
  }

  @Test
  public void filtered_approx_percentile() {
    ExprValue result = aggregation(
        dsl.approxPercentile(DSL.literal(99.9), DSL.ref("integer_value", INTEGER))
            .condition(dsl.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))), tuples);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_with_missing() {
    ExprValue result =
        aggregation(dsl.approxPercentile(DSL.literal(99.9), DSL.ref("integer_value", INTEGER)),
            tuples_with_null_and_missing);
    assertEquals(2.0, result.value());
  }

  @Test
  public void approx_percentile_with_null() {
    ExprValue result =
        aggregation(dsl.approxPercentile(DSL.literal(99.9), DSL.ref("double_value", DOUBLE)),
            tuples_with_null_and_missing);
    assertEquals(4.0, result.value());
  }

  @Test
  public void approx_percentile_with_all_missing_or_null() {
    ExprValue result =
        aggregation(dsl.approxPercentile(DSL.literal(99.9), DSL.ref("double_value", DOUBLE)),
            tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> dsl.approxPercentile(DSL.literal(99.9), DSL.ref("double_value", DOUBLE))
            .valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: approx_percentile", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator approxPercentileAggregator = dsl
        .approxPercentile(DSL.literal(99.9), DSL.ref("integer_value", INTEGER));
    assertEquals("approx_percentile(integer_value)", approxPercentileAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator approxPercentileAggregator = dsl
        .approxPercentile(DSL.literal(99.9), dsl.multiply(DSL.ref("integer_value", INTEGER),
            DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(String.format("approx_percentile(*(%s, %d))",
        DSL.ref("integer_value", INTEGER), 10),
        approxPercentileAggregator.toString());
  }
}

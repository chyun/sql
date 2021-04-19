package com.amazon.opendistroforelasticsearch.sql.expression.aggregation;

import static com.amazon.opendistroforelasticsearch.sql.utils.ExpressionUtils.format;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.tdunning.math.stats.TDigest;
import java.util.List;
import java.util.Locale;

public class ApproxPercentileAggregator
    extends Aggregator<ApproxPercentileAggregator.ApproxPercentileState> {

  public ApproxPercentileAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.APPROX_PERCENTILE.getName(), arguments, returnType);
  }

  @Override
  public ApproxPercentileState create() {
    return new ApproxPercentileState((this.initValues.get(0)).valueOf(null).doubleValue());
  }

  @Override
  protected ApproxPercentileState iterate(ExprValue value, ApproxPercentileState state) {
    state.digest.add(ExprValueUtils.getDoubleValue(value));
    state.count = state.count + 1;
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "approx_percentile(%s)", format(getArguments()));
  }

  /**
   * Count State.
   */
  protected static class ApproxPercentileState implements AggregationState {

    private double lower = 0.0;
    private double upper = 100.0;
    private int count;
    double compression = 200;

    double quantile;
    TDigest digest = TDigest.createAvlTreeDigest(compression);

    ApproxPercentileState(Double quantile) {
      this.count = 0;
      if (quantile >= lower && quantile <= upper) {
        this.quantile = quantile / 100;
      } else {
        throw new ExpressionEvaluationException(
            String.format("percentile should be in [0,100] "
                + "in approx_percentile aggregation, got [%s]", quantile));
      }
    }

    @Override
    public ExprValue result() {
      return count == 0 ? ExprNullValue.of()
          : ExprValueUtils.doubleValue(digest.quantile(this.quantile));
    }
  }
}

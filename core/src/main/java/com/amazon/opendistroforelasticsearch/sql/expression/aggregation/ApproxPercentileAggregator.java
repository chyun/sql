package com.amazon.opendistroforelasticsearch.sql.expression.aggregation;

import com.amazon.opendistroforelasticsearch.sql.ast.expression.Literal;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.tdunning.math.stats.TDigest;

import java.util.List;

/**
 * 类的实现描述:
 *
 * @author liqun.wu
 * @date 2021/4/18.
 */
public class ApproxPercentileAggregator
    extends Aggregator<ApproxPercentileAggregator.ApproxPercentileState> {
  private double quantile;

  public ApproxPercentileAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.APPROX_PERCENTILE.getName(), arguments, returnType);
    quantile = (Double) ((Literal) arguments.get(0)).getValue();
  }

  @Override
  public ApproxPercentileState create() {
    return new ApproxPercentileState(this.quantile);
  }

  @Override
  protected ApproxPercentileState iterate(ExprValue value, ApproxPercentileState state) {
    state.digest.add(ExprValueUtils.getDoubleValue(value));
    state.count = state.count + 1;
    return state;
  }

  /**
   * Count State.
   */
  protected static class ApproxPercentileState implements AggregationState {
    private int count;
    double compression = 200;
    double quantile = 0.50;
    TDigest digest = TDigest.createAvlTreeDigest(compression);

    ApproxPercentileState(double quantile) {
      this.count = 0;
      this.quantile = quantile;
    }

    @Override
    public ExprValue result() {
      return ExprValueUtils.doubleValue(digest.quantile(this.quantile));
    }
  }
}

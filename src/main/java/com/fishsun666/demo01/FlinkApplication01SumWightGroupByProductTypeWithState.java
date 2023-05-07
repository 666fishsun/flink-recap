package com.fishsun666.demo01;

import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.source.SiteShouldSignSource;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 21:49
 * @Desc: 对于已签收的数据, 结算重量根据产品类型进行汇总, 并且使用key by state
 * @Version: v1.0
 */

public class FlinkApplication01SumWightGroupByProductTypeWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtils.createEnv();
        env.addSource(new SiteShouldSignSource())
                .setParallelism(1)
                .filter(x -> x.getSignType() != 10)
                .setParallelism(1)
                .name("filter-signed-data")
                .uid("filter-signed-data")
                .name("site-should-sign-source")
                .uid("uid-site-should-sign-source")
                .keyBy(SiteShouldSign::getProductTypeName)
                .map(new RichMapFunction<SiteShouldSign, Tuple2<String, BigDecimal>>() {
                    private ValueState<Tuple2<String, BigDecimal>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Tuple2<String, BigDecimal> stringBigDecimalTuple2 = new Tuple2<>();
                        ValueStateDescriptor<Tuple2<String, BigDecimal>> valueStateDescriptor =
                                new ValueStateDescriptor<Tuple2<String, BigDecimal>>("group-sum-calc-weight",
                                        TypeInformation.of(
                                                new TypeHint<Tuple2<String, BigDecimal>>() {}
                                        )
                                );
                        state = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public Tuple2<String, BigDecimal> map(SiteShouldSign value) throws Exception {
                        if (state.value() == null) {
                            state.update(Tuple2.of(value.getProductTypeName(), value.getCalcWeight()));
                        } else {
                            state.update(
                                    Tuple2.of(
                                            state.value().f0,
                                            state.value().f1.add(value.getCalcWeight())
                                    )
                            );
                        }
                        return state.value();
                    }
                })
                .setParallelism(1)
                .name("map")
                .print()
                .setParallelism(1)
                .name("print");
        env.execute(FlinkApplication01SumWightGroupByProductTypeWithState.class.getSimpleName());
    }
}

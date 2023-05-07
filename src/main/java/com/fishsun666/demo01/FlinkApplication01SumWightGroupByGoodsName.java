package com.fishsun666.demo01;

import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.source.SiteShouldSignSource;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 21:49
 * @Desc: 对结算重量根据货物名称进行汇总
 * @Version: v1.0
 */

public class FlinkApplication01SumWightGroupByGoodsName {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtils.createEnv();
        DataStream<SiteShouldSign> siteShouldSignDataStreamSource = env.addSource(new SiteShouldSignSource())
                .setParallelism(1)
                .name("site-should-sign-source")
                // .uid("uid-site-should-sign-source")
                ;
        siteShouldSignDataStreamSource
                .keyBy(SiteShouldSign::getGoodsName)
                .reduce(new ReduceFunction<SiteShouldSign>() {
                    @Override
                    public SiteShouldSign reduce(SiteShouldSign value1, SiteShouldSign value2) throws Exception {
                        return SiteShouldSign.builder()
                                .goodsName(value1.getGoodsName())
                                .calcWeight(value1.getCalcWeight().add(value2.getCalcWeight()))
                                .build();
                    }
                }).setParallelism(1)
                .name("reduce-calc-weight")
                .map(new MapFunction<SiteShouldSign, Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> map(SiteShouldSign value) throws Exception {
                        return Tuple2.of(value.getGoodsName(), value.getCalcWeight());
                    }
                }).setParallelism(1)
                .name("map")
                .print()
                .setParallelism(1)
                .name("print");
        env.execute(FlinkApplication01SumWightGroupByGoodsName.class.getSimpleName());
    }
}

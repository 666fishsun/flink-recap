package com.fishsun666.demo02;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/10 23:27
 * @Desc:
 * @Version: v1.0
 */

public class FlinkApp02ReadFromKafka {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String SOURCE_TOPIC_NAME = "tx_site_should_sign_in";
    private static final String CONSUME_GROUP_ID = "test";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtils.createEnv();
        KafkaSource<SiteShouldSign> kafkaSource = KafkaSource.<SiteShouldSign>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(SOURCE_TOPIC_NAME)
                .setGroupId(CONSUME_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<SiteShouldSign>() {
                    @Override
                    public SiteShouldSign deserialize(byte[] message) throws IOException {
                        return JSONObject.parseObject(new String(message), SiteShouldSign.class);
                    }
                })
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka_source")
                .name("kafka_source")
                .uid("uid_kafka_source")
                .setParallelism(1)
                .keyBy(SiteShouldSign::getProductTypeName)
                .map(new RichMapFunction<SiteShouldSign, Tuple2<String, Long>>() {

                    private ReducingState<Long> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Long> longReducingStateDescriptor = new ReducingStateDescriptor<>("reduce-cnt",
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return value1 + value2;
                                    }
                                }, Long.class);
                        this.state  =getRuntimeContext()
                                .getReducingState(longReducingStateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(SiteShouldSign value) throws Exception {
                        if (state.get() == null) {
                            state.add(0L);
                        }
                        state.add(1L);
                        return Tuple2.<String, Long>of(value.getProductTypeName(), state.get());
                    }
                }).setParallelism(1)
                .name("map")
                .uid("uid-map")
                .print().name("print")
                .uid("print")
                .setParallelism(1);

        env.execute(FlinkApp02ReadFromKafka.class.getSimpleName());
    }
}

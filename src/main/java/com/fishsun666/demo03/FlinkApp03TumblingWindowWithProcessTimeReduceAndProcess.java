package com.fishsun666.demo03;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/10 23:47
 * @Desc:
 * @Version: v1.0
 */

public class FlinkApp03TumblingWindowWithProcessTimeReduceAndProcess {

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
                .map(x -> Tuple2.<String, Long>of(x.getProductTypeName(), 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                    /**
                     * Gets the type information described by this TypeHint.
                     *
                     * @return The type information described by this TypeHint.
                     */
                    @Override
                    public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }))
                .setParallelism(1)
                .name("map")
                .uid("uid-map")
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.<String, Long>of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        String key = "process-" + s;
                        Long cnt = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            cnt += element.f1;
                        }
                        out.collect(Tuple2.<String, Long>of(key, cnt));
                    }
                }).setParallelism(1)
                .name("process")
                .uid("uid-process")
                .print()
                .setParallelism(1)
                .name("print")
                .uid("uid-print")
        ;
        env.execute(FlinkApp03TumblingWindowWithProcessTimeReduceAndProcess.class.getSimpleName());
    }
}

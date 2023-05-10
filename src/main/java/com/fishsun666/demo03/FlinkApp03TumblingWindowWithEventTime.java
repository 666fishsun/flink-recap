package com.fishsun666.demo03;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/10 23:47
 * @Desc:
 * @Version: v1.0
 */

public class FlinkApp03TumblingWindowWithEventTime {

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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SiteShouldSign>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<SiteShouldSign>() {
                                    @Override
                                    public long extractTimestamp(SiteShouldSign element, long recordTimestamp) {
                                        return element.getSendTime() != null ? element.getSendTime().getTime() : System.currentTimeMillis();
                                    }
                                })
                )
                .map(new MapFunction<SiteShouldSign, SiteShouldSign>() {
                    @Override
                    public SiteShouldSign map(SiteShouldSign value) throws Exception {
                        return SiteShouldSign.builder()
                                .totalPiece(1L)
                                .productTypeName(value.getProductTypeName())
                                .build();
                    }
                })
                .keyBy(SiteShouldSign::getProductTypeName)
                .window(TumblingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .reduce(new ReduceFunction<SiteShouldSign>() {
                            @Override
                            public SiteShouldSign reduce(SiteShouldSign value1, SiteShouldSign value2) throws Exception {
                                return SiteShouldSign.builder()
                                        .productTypeName(value1.getProductTypeName())
                                        .totalPiece(value1.getTotalPiece() + value2.getTotalPiece())
                                        .build();
                            }
                        },
                        new ProcessWindowFunction<SiteShouldSign, SiteShouldSign, String, TimeWindow>() {
                            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

                            @Override
                            public void process(String s, ProcessWindowFunction<SiteShouldSign, SiteShouldSign, String, TimeWindow>.Context context, Iterable<SiteShouldSign> elements, Collector<SiteShouldSign> out) throws Exception {
                                System.out.println("event time: " + sdf.format(context.currentProcessingTime()));
                                System.out.println("start time: " + sdf.format(context.window().getStart()));
                                System.out.println("end time: " + sdf.format(context.window().getEnd()));
                                SiteShouldSign elem = SiteShouldSign.builder()
                                        .totalPiece(0L)
                                        .build();
                                for (SiteShouldSign element : elements) {
                                    elem.setProductTypeName(element.getProductTypeName());
                                    elem.setTotalPiece(elem.getTotalPiece() + element.getTotalPiece());
                                }
                                out.collect(elem);
                            }
                        }
                )
                .map(new MapFunction<SiteShouldSign, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(SiteShouldSign value) throws Exception {
                        return Tuple2.<String, Long>of(value.getProductTypeName(), value.getTotalPiece());
                    }
                })
                .setParallelism(1)
                .print()
                .setParallelism(1)
                .name("print")
                .uid("uid-print")
        ;
        env.execute(FlinkApp03TumblingWindowWithEventTime.class.getSimpleName());
    }
}

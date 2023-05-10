package com.fishsun666.demo02;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import com.fishsun666.source.SiteShouldSignSource;
import com.fishsun666.source.SiteShouldSignSourceWithCK;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/8 21:25
 * @Desc:
 * @Version: v1.0
 */

public class FlinkApp02SinkData2kafka {
    private static final String SINK_KAFKA_TOPIC = "tx_site_should_sign_in";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtils.createEnv();
        Properties properties = new Properties();
        properties.put("transaction.timeout.ms", "10000");
        KafkaSink<SiteShouldSign> kafkaSink = KafkaSink.<SiteShouldSign>builder()
                .setKafkaProducerConfig(properties)
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setRecordSerializer(new KafkaRecordSerializationSchema<SiteShouldSign>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(SiteShouldSign element, KafkaSinkContext context, Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(SINK_KAFKA_TOPIC, JSONObject.toJSONString(element).getBytes(StandardCharsets.UTF_8));
                    }
                })
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        env.addSource(new SiteShouldSignSource())
                .returns(SiteShouldSign.class)
                .setParallelism(1)
                .name("source-site-should-sign")
                .uid("uid-site-should-sign")
                .sinkTo(kafkaSink)
                .setParallelism(1)
                .name("sink-site-should-sign2kafka")
                .uid("uid-sink-site-should-sign2kafka");

        env.execute(FlinkApp02SinkData2kafka.class.getSimpleName());
    }
}

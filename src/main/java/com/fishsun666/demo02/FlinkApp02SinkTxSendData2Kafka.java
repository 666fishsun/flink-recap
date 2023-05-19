package com.fishsun666.demo02;


import com.fishsun666.sink.StatefulPrint;
import com.fishsun666.source.ParquetFileSource;
import com.fishsun666.utils.EnvUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2023/5/13 14:47
 * @Desc :
 */
public class FlinkApp02SinkTxSendData2Kafka {
  private static final String SINK_KAFKA_TOPIC = "tx_oper_center_send";
  private static final String BOOTSTRAP_SERVER = "10.10.106.148:25502";

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = EnvUtils.createEnv(8083, "tx_oper_center_send");

    DataStream<String> parquetSource = env.addSource(new ParquetFileSource(Arrays.asList("data/tx_oper_center_send/dt=2023-05-10/hour=0")))
            .name(String.format("%s_source", SINK_KAFKA_TOPIC))
            .uid(String.format("uid_%s_source", SINK_KAFKA_TOPIC));

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    parquetSource
            .addSink(new FlinkKafkaProducer<>(SINK_KAFKA_TOPIC, new SimpleStringSchema(), properties))
            .setParallelism(1)
            .name(String.format("kafka_%s", SINK_KAFKA_TOPIC))
            .uid(String.format("uid_kafka_%s", SINK_KAFKA_TOPIC))
            .disableChaining();
    parquetSource.addSink(new StatefulPrint(10000L)).setParallelism(1)
            .name(String.format("%s_print", SINK_KAFKA_TOPIC))
            .uid(String.format("uid_%s_print", SINK_KAFKA_TOPIC));
    env.execute(SINK_KAFKA_TOPIC);
    StreamGraph streamGraph = env.getStreamGraph();
    JobGraph jobGraph = streamGraph.getJobGraph();
    jobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath("file:///C:/Users/zxs14/workspace/flink-recap/ck/tx_oper_center_send/c7c88021e8d07d15b6161b133110e190", false)
    );
  }
}

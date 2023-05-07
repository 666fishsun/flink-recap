package com.fishsun666.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 22:29
 * @Desc:
 * @Version: v1.0
 */

public class EnvUtils {

    /**
     * 创建streamEnv, 并且使用Rocksdb backend
     * @return
     */
    public static StreamExecutionEnvironment createEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 开启ck
        env.enableCheckpointing(3 * 1000L);
        // 使用exactly-once ck
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置ck之间最少有1000ms的间隔
        env.getCheckpointConfig().setCheckpointInterval(1 * 1000L);
        // 设置ck的时间必须在1分钟内完成, 否则就被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置同一时间只能使用一个ck
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置cancel时仍保留数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 允许使用非对齐检查
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 5分钟内若失败3次则不再重启
        // 每次重启间隔为10秒
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3,
                        Time.minutes(5),
                        Time.seconds(10)
                )
        );
        return env;
    }
}

package com.fishsun666.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 22:29
 * @Desc:
 * @Version: v1.0
 */

public class EnvUtils {
    public static StreamExecutionEnvironment createEnv() {
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    }
}

package com.fishsun666.source;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.fishsun666.utils.ParquetUtils.listFiles;
import static com.fishsun666.utils.ParquetUtils.readParquet;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 23:21
 * @Desc: 有状态的source
 * @Version: v1.0
 */

public class SiteShouldSignSourceWithCK<T> extends RichSourceFunction<T> implements CheckpointedFunction {

    ListState<SiteShouldSign> state = null;
    List<SiteShouldSign> data = new ArrayList<>();
    private List<String> dataDirList;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataDirList = Arrays.asList("data/cdc_tx_site_should_sign/dt=2023-05-06/hour=3",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=6",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=8",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=18",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=20");
    }

    /**
     * 打ck时调用
     * 将数据写入状态中
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        for (SiteShouldSign datum : data) {
            state.add(datum);
        }
    }

    /**
     * 初始化或者从状态中恢复
     *
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListState<SiteShouldSign> siteShouldSignData = context.getOperatorStateStore() // 获取状态state
                .getListState(new ListStateDescriptor<SiteShouldSign>("site_should_sign_data",
                        SiteShouldSign.class));
        if (context.isRestored()) { // 如果是从状态中恢复
            for (SiteShouldSign siteShouldSign : siteShouldSignData.get()) {
                data.add(siteShouldSign);
            }
        }
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        for (String dataDir : dataDirList) {
            List<File> files = listFiles(dataDir);
            for (File file : files) {
                List<Map<String, String>> parquetContent = readParquet("file://" + file.getAbsolutePath());
                for (Map<String, String> stringStringMap : parquetContent) {
                    String messageBody = stringStringMap.get("message_body");
                    for (Object data : JSONObject.parseObject(messageBody).getJSONArray("data")) {
                        SiteShouldSign siteShouldSign = JSONObject.parseObject(data.toString(), SiteShouldSign.class);
                        ctx.collect(siteShouldSign);
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {

    }
}

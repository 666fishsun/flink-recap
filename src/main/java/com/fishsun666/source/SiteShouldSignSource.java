package com.fishsun666.source;

import com.alibaba.fastjson.JSONObject;
import com.fishsun666.model.SiteShouldSign;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.fishsun666.utils.ParquetUtils.listFiles;
import static com.fishsun666.utils.ParquetUtils.readParquet;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 22:36
 * @Desc:
 * @Version: v1.0
 */

public class SiteShouldSignSource extends RichSourceFunction<SiteShouldSign> {
    List<String> dataDirList = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataDirList = Arrays.asList("data/cdc_tx_site_should_sign/dt=2023-05-06/hour=3",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=6",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=8",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=18",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=20");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<SiteShouldSign> ctx) throws Exception {
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

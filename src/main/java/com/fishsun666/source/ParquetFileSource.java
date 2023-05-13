package com.fishsun666.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.fishsun666.utils.ParquetUtils.listFiles;
import static com.fishsun666.utils.ParquetUtils.readParquet;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2023/5/13 14:49
 * @Desc :
 */
public class ParquetFileSource extends RichSourceFunction<String> implements CheckpointedFunction {
  private List<String> dataDirList = null;

  private Long cursor;

  private ListState<Long> cursorState = null;

  public ParquetFileSource(List<String> dataDirList) {
    this.dataDirList = dataDirList;
  }

  @Override
  public void open(Configuration configuration) {
    cursor = 0L;
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    Long runtimeCursor = 0L;
    for (String dataDir : dataDirList) {
      List<File> files = listFiles(dataDir);
      for (File file : files) {
        List<Map<String, String>> parquetContent = readParquet("file:///" + file.getAbsolutePath());
        for (Map<String, String> stringStringMap : parquetContent) {
          runtimeCursor += 1L;
          if (runtimeCursor > cursor) {
            cursor += 1L;
            ctx.collect(stringStringMap.get("message_body"));
          }
        }
      }
    }
  }

  @Override
  public void cancel() {

  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    System.out.println("snapshot " + cursor);
    cursorState.clear();
    cursorState.addAll(Collections.singletonList(cursor));
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    ListStateDescriptor<Long> cursorDescription = new ListStateDescriptor<>("cursor", Long.class);
    cursorState = functionInitializationContext.getOperatorStateStore()
            .getListState(cursorDescription);
    if (functionInitializationContext.isRestored()) {
      if (cursorState.get() == null) {
        cursorState.clear();
        cursorState.addAll(Collections.singletonList(0L));
      } else {
        cursor = cursorState.get().iterator().next();
      }
    }
    System.out.println("cursor loaded: " + cursor);
  }
}

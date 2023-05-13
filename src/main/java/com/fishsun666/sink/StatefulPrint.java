package com.fishsun666.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2023/5/13 20:50
 * @Desc :
 */
public class StatefulPrint extends RichSinkFunction<String> implements CheckpointedFunction {
  private long cnt;
  private long startTime;

  private long threshold;

  public StatefulPrint(long threshold) {
    this.threshold = threshold;
  }

  private ListState<Long> listState;

  @Override
  public void open(Configuration parameters) throws Exception {
    cnt = 0L;
    startTime = System.currentTimeMillis();
    Preconditions.checkArgument(threshold > 0L);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    listState.clear();
    listState.addAll(Arrays.asList(threshold, startTime, cnt));
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Long> longListStateDescriptor = new ListStateDescriptor<>("print-sink-state", Long.class);
    listState = context.getOperatorStateStore().getListState(longListStateDescriptor);
    if (context.isRestored()) {
      int idx = 0;
      for (Long state : listState.get()) {
        if (idx == 0) {
          threshold = state;
        } else if (idx == 1) {
          startTime = state;
        } else if (idx == 2) {
          cnt = state;
        }
        idx += 1;
      }
    }
  }

  @Override
  public void invoke(String value, Context context) throws Exception {
    cnt += 1L;
    if (cnt % threshold == 0) {
      System.out.format("thread: %s, cnt: %d, duration: %d, content: %s\n",
              Thread.currentThread().getName(),
              cnt,
              System.currentTimeMillis() - startTime,
              value);
    }
  }
}

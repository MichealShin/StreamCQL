package com.pzw.streaming.flink.plan.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**全局窗口触发器,存放所有累积数据
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/27 下午6:26
 */
public class GlobalWindowCountTrigger<W extends Window> extends Trigger<Object,W> {

    private final long maxCount;
    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<Long>("count",new Sum(), LongSerializer.INSTANCE);

    private GlobalWindowCountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        count.add(1L);

        if(count.get()>= maxCount) {
            count.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(countStateDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(countStateDesc);
    }

    private static class Sum implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long a, Long b) throws Exception {
            return a+b;
        }
    }

    public static GlobalWindowCountTrigger of(long count) {
        return new GlobalWindowCountTrigger<>(count);
    }
}

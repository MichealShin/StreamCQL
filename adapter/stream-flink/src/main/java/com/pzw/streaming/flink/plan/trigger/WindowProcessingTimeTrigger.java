package com.pzw.streaming.flink.plan.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/31 下午3:47
 */
public class WindowProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

    private WindowProcessingTimeTrigger() {

    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
    }

    public static WindowProcessingTimeTrigger of() {
        return new WindowProcessingTimeTrigger();
    }
}

package com.pzw.streaming.flink.plan;

import com.huawei.streaming.api.opereators.WindowCommons;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.pzw.streaming.flink.plan.trigger.GlobalWindowCountTrigger;
import com.pzw.streaming.flink.plan.trigger.WindowProcessingTimeTrigger;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

/**
 * 窗口类型执行计划
 *
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/16 下午12:04
 */
public abstract class WindowPlan extends FunctionPlan {

    private transient com.huawei.streaming.api.opereators.Window window;
    private IExpression filterExpr;
    private boolean noWindow = true;

    public com.huawei.streaming.api.opereators.Window getWindow() {
        return window;
    }

    public void setWindow(com.huawei.streaming.api.opereators.Window window) {
        this.window = window;
        if (window != null) {
            noWindow = false;
        }
    }

    public List<IExpression> getSelectExpressions() {
        return getExpressions();
    }

    public IExpression getFilterExpression() {
        return filterExpr;
    }

    public void setFilterExpr(IExpression filterExpr) {
        this.filterExpr = filterExpr;
    }




    @SuppressWarnings("unchecked")
    protected WindowedStream createWindowedStream(KeyedStream input) throws StreamingException {
        if (window == null) {
            return input.countWindow(1).
                    trigger(GlobalWindowCountTrigger.of(1));
        }
        switch (window.getName()) {

            case WindowCommons.TIME_BATCH_WINDOW:

                return input.timeWindow(Time.milliseconds(window.getLength())).
                        trigger(WindowProcessingTimeTrigger.of());

            case WindowCommons.TIME_SLIDE_WINDOW:
                //TODO StreamingCQL目前滑动间隔是写死的,需要在语法层面修改成可配置
                return input.timeWindow(Time.milliseconds(window.getLength()), Time.milliseconds(5)).
                        trigger(WindowProcessingTimeTrigger.of());

            case WindowCommons.LENGTH_BATCH_WINDOW:
                return input.countWindow(window.getLength());

            case WindowCommons.LENGTH_SLIDE_WINDOW:
                //TODO 滑动间隔可配置
                return input.countWindow(window.getLength(), 1);

            default:
                throw new StreamingException("currently not support this type of window:" + window.getName());
        }
    }

    @SuppressWarnings("unchecked")
    protected AllWindowedStream createAllWindowedStream(DataStream input) throws StreamingException {

        if (window == null) {
            return input.countWindowAll(1).
                    trigger(GlobalWindowCountTrigger.of(1));
        }

        switch (window.getName()) {

            case WindowCommons.TIME_BATCH_WINDOW:

                return input.timeWindowAll(Time.milliseconds(window.getLength())).
                        trigger(WindowProcessingTimeTrigger.of());

            case WindowCommons.TIME_SLIDE_WINDOW:

                return input.timeWindowAll(Time.milliseconds(window.getLength()), Time.milliseconds(5)).
                        trigger(WindowProcessingTimeTrigger.of());

            case WindowCommons.LENGTH_BATCH_WINDOW:

                return input.countWindowAll(window.getLength());

            case WindowCommons.LENGTH_SLIDE_WINDOW:

                return input.countWindowAll(window.getLength(), 1);

            default:
                throw new StreamingException("currently not support this type of window:" + window.getName());
        }
    }

    public boolean isNoWindowGroupBy() {
        return noWindow;
    }
}

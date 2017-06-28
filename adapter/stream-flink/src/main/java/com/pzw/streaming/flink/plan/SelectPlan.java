package com.pzw.streaming.flink.plan;

import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:11
 */
public class SelectPlan extends FunctionPlan implements MapFunction<TupleEvent, TupleEvent> {

    @Override
    public TupleEvent map(TupleEvent event) throws Exception {

        List<IExpression> selectExprs = getExpressions();
        Object[] values = new Object[selectExprs.size()];

        for (int i = 0; i < selectExprs.size(); i++) {
            values[i] = selectExprs.get(i).evaluate(event);
        }

        IEventType eventType = getOutputEventTypes().values().iterator().next();

        return new TupleEvent(eventType.getEventTypeName(), eventType, values);
    }


    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges) throws StreamingException {
        return inputs.get(0).map(this).
                setParallelism(getParallelNumber());
    }
}

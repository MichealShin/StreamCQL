package com.pzw.streaming.flink.plan;

import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:11
 */
public class FilterPlan extends FunctionPlan implements FilterFunction<TupleEvent> {

    @Override
    public boolean filter(TupleEvent event) throws Exception {
        IExpression filterExpr = getExpressions().get(0);
        return (Boolean) filterExpr.evaluate(event);
    }


    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges)
            throws StreamingException {

        return inputs.get(0).filter(this).
                setParallelism(getParallelNumber());
    }
}

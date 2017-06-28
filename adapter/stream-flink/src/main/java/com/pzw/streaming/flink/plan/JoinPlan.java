package com.pzw.streaming.flink.plan;

import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.pzw.streaming.flink.plan.common.TupleEventKeySelector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:27
 */
public class JoinPlan extends WindowPlan implements CoGroupFunction<TupleEvent, TupleEvent, TupleEvent> {


    @Override
    public void coGroup(Iterable<TupleEvent> iterable, Iterable<TupleEvent> iterable1,
                        Collector<TupleEvent> collector) throws Exception {

    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges) throws StreamingException {
        DataStream<TupleEvent> oneInput = inputs.get(0);
        DataStream<TupleEvent> twoInput = inputs.get(1);

        PlanEdge oneEdge = inputEdges.get(0);
        PlanEdge twoEdge = inputEdges.get(1);

        WindowedStream windStream = createWindowedStream(oneInput.keyBy(0));

        return oneInput.coGroup(twoInput)
                .where(new TupleEventKeySelector(oneEdge.getShuffleExpressions()))
                .equalTo(new TupleEventKeySelector(twoEdge.getShuffleExpressions()))
                .window(getWindowAssiginer(windStream))
                .trigger(getWindowTrigger(windStream))
                .evictor(getWindowEvictor(windStream))
                .apply(new JoinPlan());
    }

    private WindowAssigner getWindowAssiginer(WindowedStream windStream) throws StreamingException {
        try {
            Field field = windStream.getClass().getDeclaredField("windowAssigner");
            field.setAccessible(true);
            return (WindowAssigner) field.get(windStream);
        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamingException(e.getMessage(), e);
        }
    }

    private Trigger getWindowTrigger(WindowedStream windStream) throws StreamingException {
        try {
            Field field = windStream.getClass().getDeclaredField("trigger");
            field.setAccessible(true);
            return (Trigger) field.get(windStream);
        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamingException(e.getMessage(), e);
        }
    }

    private Evictor getWindowEvictor(WindowedStream windStream) throws StreamingException {
        try {
            Field field = windStream.getClass().getDeclaredField("evictor");
            field.setAccessible(true);
            return (Evictor) field.get(windStream);
        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamingException(e.getMessage(), e);
        }
    }


}

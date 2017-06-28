package com.pzw.streaming.flink.plan;

import com.google.common.collect.Lists;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.pzw.streaming.flink.graph.GroupbyKeyInSelectReplacer;
import com.pzw.streaming.flink.plan.builder.AggregatePlanBuilder.AggregateExpressionWithId;
import com.pzw.streaming.flink.plan.common.TupleEventKeySelector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:22
 */
public class AggregatePlan extends WindowPlan
        implements  WindowFunction<TupleEvent, TupleEvent, Tuple, Window> {


    private List<AggregateExpressionWithId> aggregates = Lists.newArrayList();

    private Map<AggregateExpression, PropertyValueExpression> aggMapProperty;


    @Override
    public void apply(Tuple key, Window window, Iterable<TupleEvent> events,
                      Collector<TupleEvent> collector) throws Exception {

        TupleEvent event = events.iterator().next();
        //将key中的字段添加到event后,后续select运算读取key的值
        if(key!=null) {
            event = addKeyToEvent(event, key);
        }

        //计算select值
        List<IExpression> selects = getSelectExpressions();

        Object[] selectValues = new Object[selects.size()];

        for (int i = 0; i < selects.size(); i++) {
            selectValues[i] = selects.get(i).evaluate(event);
        }

        IEventType eventType = getOutputEventTypes().values().iterator().next();
        TupleEvent selectEvent = new TupleEvent(eventType.getEventTypeName(), eventType, selectValues);

        //计算过滤值
        IExpression filter = getFilterExpression();

        if (filter != null) {
            Boolean accept = (Boolean) filter.evaluate(selectEvent);

            if (accept!=null&&accept) {
                collector.collect(selectEvent);
            }
        } else { //无过滤表达式
            collector.collect(selectEvent);
        }
    }

    private TupleEvent addKeyToEvent(TupleEvent event, Tuple key) {

        int oldValueLength = event.getAllValues().length;
        Object[] values = new Object[oldValueLength + key.getArity()];

        System.arraycopy(event.getAllValues(), 0, values, 0, oldValueLength);

        for (int i = oldValueLength; i < values.length; i++) {
            values[i] = key.getField(i - oldValueLength);
        }


        int oldAttrLength = event.getEventType().getAllAttributes().length;
        Attribute[] attrs = new Attribute[oldAttrLength + key.getArity()];

        System.arraycopy(event.getEventType().getAllAttributes(), 0, attrs, 0, oldAttrLength);

        for (int i = oldAttrLength; i < attrs.length; i++) {

            int groupByIndex = i - oldAttrLength;
            String groupByFieldName = GroupbyKeyInSelectReplacer.getGroupByExpressionReplacedName(groupByIndex);

            attrs[i] = new Attribute(key.getField(i - oldAttrLength).getClass(), groupByFieldName);
        }

        TupleEventType type = new TupleEventType(event.getEventType().getEventTypeName(), attrs);

        return new TupleEvent(event.getStreamName(), type, values);
    }

    public List<AggregateExpressionWithId> getAggregates() {
        return aggregates;
    }

    public void setAggMapProperty(Map<AggregateExpression, PropertyValueExpression> aggMapProperty) {
        this.aggMapProperty = aggMapProperty;
    }


    /**
     * 聚合器
     */
    class CommonAccumulator {

        List<AggregateExpression> aggregates;
        TupleEvent tupleEvent;

        public CommonAccumulator(List<AggregateExpression> aggregates) {
            this.aggregates = aggregates;

            for (AggregateExpression aggExpression : aggregates) {
                aggExpression.getAggegator().clear();
            }
        }

        public void add(TupleEvent value) {

            for (AggregateExpression aggExpression : aggregates) {

                IExpression argExpression = aggExpression.getAggArgExpression();
                Object argValue = argExpression.evaluate(value);

                aggExpression.getAggegator().enter(argValue, false);
            }
            tupleEvent = value;
        }

        public TupleEvent getResult() {

            Attribute[] attrs = new Attribute[aggregates.size()];

            for (int i = 0; i < attrs.length; i++) {

                AggregateExpression aggExpression = aggregates.get(i);
                attrs[i] = new Attribute(aggExpression.getType(), aggMapProperty.get(aggExpression).getPropertyName());
            }

            IEventType type = new TupleEventType("aggOutput", attrs);

            Object[] values = new Object[aggregates.size()];

            for (int i = 0; i < aggregates.size(); i++) {
                AggregateExpression aggExpression = aggregates.get(i);
                values[i] = aggExpression.evaluate(tupleEvent);
            }

            return new TupleEvent("aggOutput", type, values);
        }


    }

    class CommonAggregateFunction implements AggregateFunction<TupleEvent, CommonAccumulator, TupleEvent> {

        @Override
        public CommonAccumulator createAccumulator() {

            List<AggregateExpression> aggExpressionsClone = Lists.newArrayList();

            for (AggregateExpressionWithId aggExpression : aggregates) {

                AggregateExpression aggExpressionClone = aggExpression.clone();
                aggExpressionsClone.add(aggExpressionClone);
            }

            return new CommonAccumulator(aggExpressionsClone);
        }


        @Override
        public void add(TupleEvent value, CommonAccumulator accumulator) {
            accumulator.add(value);
        }

        @Override
        public TupleEvent getResult(CommonAccumulator accumulator) {
            return accumulator.getResult();
        }

        @Override
        public CommonAccumulator merge(CommonAccumulator a, CommonAccumulator b) {
            throw new IllegalStateException("currently not support session window,so this method should never be called");
        }
    }


    @SuppressWarnings("unchecked")
    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges) throws StreamingException {

        DataStream input = inputs.get(0);
        final PlanEdge edge = inputEdges.get(0);

        //包含group by key的聚合
        if (edge.getShuffleExpressions() != null) {

            KeyedStream keyedStream = input.keyBy(new TupleEventKeySelector(edge.getShuffleExpressions()));

            return createWindowedStream(keyedStream).
                    aggregate(new CommonAggregateFunction(), this).
                    setParallelism(getParallelNumber());

        } else { //无group by key的聚合

            return createAllWindowedStream(input).
                    aggregate(new CommonAggregateFunction(),new AggregateAllWindowFunction()).
                    setParallelism(getParallelNumber());
        }
    }


    private class AggregateAllWindowFunction implements AllWindowFunction<TupleEvent,TupleEvent,Window> {

        @Override
        public void apply(Window window, Iterable<TupleEvent> values, Collector<TupleEvent> out) throws Exception {
            AggregatePlan.this.apply(null,window,values,out);
        }
    }

}

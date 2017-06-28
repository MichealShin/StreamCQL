package com.pzw.streaming.flink.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 执行计划抽象类
 *
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:38
 */
public abstract class AbstractPlan implements Serializable {

    /**
     * 并发数量
     */
    private int parallelNumber;

    private Map<String, IEventType> outputEventTypes = Maps.newHashMap();

    private List<AbstractPlan> children = Lists.newArrayList();
    private List<PlanEdge> edges = Lists.newArrayList();

    private List<AbstractPlan> parents = Lists.newArrayList();

    public List<AbstractPlan> getChildren() {
        return children;
    }

    public void setChildren(List<AbstractPlan> children) {
        this.children = children;
    }

    public List<PlanEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<PlanEdge> edges) {
        this.edges = edges;
    }

    public int getParallelNumber() {
        return parallelNumber;
    }

    public void setParallelNumber(int parallelNumber) {
        this.parallelNumber = parallelNumber;
    }

    public Map<String, IEventType> getOutputEventTypes() {
        return outputEventTypes;
    }

    public void setOutputEventTypes(Map<String, IEventType> outputEventTypes) {
        this.outputEventTypes = outputEventTypes;
    }

    public List<AbstractPlan> getParents() {
        return parents;
    }

    public PlanEdge getEdgeForChild(AbstractPlan child) {

        for(int i=0;i<children.size();i++) {
            if(children.get(i) == child) {
                return edges.get(i);
            }
        }
        throw new IllegalStateException("cannot find edge for child:"+child);
    }

    public void addChild(AbstractPlan child, PlanEdge edge) {
        children.add(child);
        edges.add(edge);

        child.parents.add(this);

        IEventType eventType = edge.getEventType();
        outputEventTypes.put(eventType.getEventTypeName(), eventType);
    }

    public void setConfig(StreamingConfig config) throws StreamingException {

    }

    /**
     * 将执行计划转换为DataStream
     *
     * @param inputs
     * @param inputEdges
     * @return
     */
    public abstract DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                                     List<DataStream<TupleEvent>> inputs,
                                                     List<PlanEdge> inputEdges) throws StreamingException;

}

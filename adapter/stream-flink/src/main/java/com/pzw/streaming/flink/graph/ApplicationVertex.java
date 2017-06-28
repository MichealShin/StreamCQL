package com.pzw.streaming.flink.graph;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.application.DistributeType;

import java.util.List;


/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午2:15
 */
public class ApplicationVertex {

    private Operator operator;

    private List<ApplicationVertex> children= Lists.newArrayList();

    private List<OperatorTransition> edges=Lists.newArrayList();



    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public List<ApplicationVertex> getChildren() {
        return children;
    }

    public void setChildren(List<ApplicationVertex> children) {
        this.children = children;
    }

    public List<OperatorTransition> getEdges() {
        return edges;
    }

    public void setEdges(List<OperatorTransition> edges) {
        this.edges = edges;
    }

    public String getId() {
        return operator.getId();
    }

    public void addChild(ApplicationVertex childVertex,OperatorTransition childTransition) {
        children.add(childVertex);
        edges.add(childTransition);
    }
}

package com.pzw.streaming.flink.plan;

import com.google.common.collect.Lists;
import com.huawei.streaming.expression.IExpression;
import org.apache.flink.api.common.functions.Function;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午4:33
 */
public abstract class FunctionPlan extends AbstractPlan {

    private List<IExpression> expressions= Lists.newArrayList();


    public List<IExpression> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<IExpression> expressions) {
        this.expressions = expressions;
    }
}

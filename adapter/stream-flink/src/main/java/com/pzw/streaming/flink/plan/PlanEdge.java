package com.pzw.streaming.flink.plan;

import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.expression.IExpression;

import java.io.Serializable;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午5:22
 */
public class PlanEdge implements Serializable {

    public enum ShuffleType {
        SHUFFLE_RANDOM,
        SHUFFLE_EXPRESSION,
        ;
    }

    private ShuffleType shuffleType;
    private IExpression[] shuffleExpressions;
    private IEventType eventType;
    private String streamName;

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public void setShuffleType(ShuffleType shuffleType) {
        this.shuffleType = shuffleType;
    }

    public IExpression[] getShuffleExpressions() {
        return shuffleExpressions;
    }

    public void setShuffleExpressions(IExpression[] shuffleExpressions) {
        this.shuffleExpressions = shuffleExpressions;
    }

    public IEventType getEventType() {
        return eventType;
    }

    public void setEventType(IEventType eventType) {
        this.eventType = eventType;
    }
}

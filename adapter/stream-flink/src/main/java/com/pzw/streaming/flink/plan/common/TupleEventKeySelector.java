package com.pzw.streaming.flink.plan.common;

import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.IExpression;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/24 下午7:13
 */
public class TupleEventKeySelector implements KeySelector<TupleEvent, Tuple>, ResultTypeQueryable<Tuple> {

    private IExpression[] keyExpressions;
    private TupleTypeInfo<Tuple> typeInfo;

    public TupleEventKeySelector(IExpression[] keyExpressions) {
        this.keyExpressions = keyExpressions;

        TypeInformation<?>[] fieldTypeInfos = new TypeInformation[keyExpressions.length];

        for (int i = 0; i < keyExpressions.length; i++) {
            fieldTypeInfos[i] = TypeInformation.of(keyExpressions[i].getClass());
        }

        typeInfo = new TupleTypeInfo<>(fieldTypeInfos);
    }

    @Override
    public Tuple getKey(TupleEvent event) throws Exception {
        Tuple key = Tuple.getTupleClass(keyExpressions.length).newInstance();

        for (int i = 0; i < keyExpressions.length; i++) {
            Object value = keyExpressions[i].evaluate(event);
            key.setField(value, i);
        }

        return key;
    }

    @Override
    public TypeInformation<Tuple> getProducedType() {
        return typeInfo;
    }
}

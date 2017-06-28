package com.pzw.streaming.flink.util;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.AbstractPlan;
import org.apache.flink.api.common.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/16 上午9:56
 */
public class PlanUtils {

    private static Logger LOG = LoggerFactory.getLogger(PlanUtils.class);


    public static <T extends Function> AbstractPlan getLastPlan(AbstractPlan plan) {

        if(plan==null)
            return null;

        while (plan.getChildren().size()!=0) {
            plan=plan.getChildren().get(0);
        }

        return plan;
    }


    public static TupleEventType createEventType(Schema schema)
            throws ExecutorException {

        List<Attribute> attrs = Lists.newArrayList();

        for (int i = 0; i < schema.getCols().size(); i++) {
            Class<?> type = getColumnDataType(schema, i);
            String colName = schema.getCols().get(i).getName();
            attrs.add(new Attribute(type, colName));
        }

        return new TupleEventType(schema.getId(), attrs);
    }

    private static Class<?> getColumnDataType(Schema schema, int i)
            throws ExecutorException {
        try {
            return Class.forName(schema.getCols().get(i).getType(), true, CQLUtils.getClassLoader());
        } catch (ClassNotFoundException e) {
            ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, schema.getCols().get(i).getType());
            throw exception;
        }
    }


    public static StreamSerDe createSerdeInstance(String serdeClassName, StreamingConfig config)
            throws StreamingException {

        if (serdeClassName == null) {
            return null;
        }

        StreamSerDe des = null;
        try {

            des = (StreamSerDe) Class.forName(serdeClassName, true, CQLUtils.getClassLoader()).newInstance();
        } catch (ReflectiveOperationException e) {
            ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, serdeClassName);
            LOG.error("Failed to create Deser instance.", exception);
            throw exception;
        }

        try {
            des.setConfig(config);
        } catch (Exception e) {
            LOG.warn("error in create serde instance:"+serdeClassName,e);
            throw new StreamingException(e.getMessage(),e);
        }

        return des;
    }
}

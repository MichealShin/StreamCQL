package com.pzw.streaming.flink;

import com.huawei.streaming.application.Application;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.IExecutorPlanGenerator;
import com.huawei.streaming.exception.StreamingException;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.graph.ApplicationGraphGenerator;
import com.pzw.streaming.flink.plan.builder.ExecutePlanBuilder;
import com.pzw.streaming.flink.plan.AbstractPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 上午11:59
 */
public class FlinkExecutorPlanGenerator implements IExecutorPlanGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutorPlanGenerator.class);

    @Override
    public Application generate(com.huawei.streaming.api.Application vap) throws ExecutorException {

        try {
            ApplicationGraph graph = new ApplicationGraphGenerator(vap).generator();

            List<AbstractPlan> sourcePlans = new ExecutePlanBuilder(graph).build();

            StreamingConfig config = new StreamingConfig();
            config.putAll(graph.getConfs());

            FlinkApplication application = new FlinkApplication(graph.getApplicationName(), config);
            application.setSourcePlans(sourcePlans);

            return application;

        } catch (StreamingException e) {
            LOG.error("fail to create flink application", e);
            throw new ExecutorException(e,e.getErrorCode());
        }
    }
}

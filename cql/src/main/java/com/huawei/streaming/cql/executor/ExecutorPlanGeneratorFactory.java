package com.huawei.streaming.cql.executor;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 上午11:44
 */
public class ExecutorPlanGeneratorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorPlanGeneratorFactory.class);

    public static IExecutorPlanGenerator create(StreamingConfig config) throws CQLException {

        String generator=config.getStringValue(StreamingConfig.STREAMING_EXECUTOR_PLAN_GENERATOR,ExecutorPlanGenerator.class.getName());

        try {
            return (IExecutorPlanGenerator) Class.forName(generator).newInstance();

        } catch (Exception e) {
            CQLException exception = new CQLException(e, ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error(exception.getMessage(), e);
            throw exception;
        }
    }
}

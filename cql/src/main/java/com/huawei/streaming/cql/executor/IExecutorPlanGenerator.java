package com.huawei.streaming.cql.executor;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.cql.exception.ExecutorException;

/**物理执行计划生成器接口
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 上午11:34
 */
public interface IExecutorPlanGenerator {
    com.huawei.streaming.application.Application generate(Application vap)
            throws ExecutorException;
}

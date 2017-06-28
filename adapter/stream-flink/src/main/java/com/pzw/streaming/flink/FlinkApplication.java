package com.pzw.streaming.flink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.application.Application;
import com.huawei.streaming.application.ApplicationResults;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.util.ConfigUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.ContextEnvironmentFactory;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午2:01
 */
public class FlinkApplication extends Application {

    private List<AbstractPlan> sourcePlans = Lists.newArrayList();

    private List<AbstractPlan> visitQueue = Lists.newArrayList();
    private Map<AbstractPlan, DataStream<TupleEvent>> visiteResults = Maps.newHashMap();

    private String jarPath;

    /**
     * <默认构造函数>
     *
     * @param appName 应用名称、
     * @param config  配置属性
     * @throws StreamingException 流处理异常
     */
    public FlinkApplication(String appName, StreamingConfig config) throws StreamingException {
        super(appName, config);
    }


    public void setSourcePlans(List<AbstractPlan> sourcePlans) {
        this.sourcePlans = sourcePlans;
    }

    @Override
    public void launch() throws StreamingException {

        try {
            String executionMode = getConf().getStringValue(StreamingConfig.STREAMING_FLINK_EXECUTION_MODE, ConfigUtils.FLINK_EXECUTION_LOCAL);

            StreamExecutionEnvironment env;

            if (ConfigUtils.FLINK_EXECUTION_LOCAL.endsWith(executionMode)) { //本地执行模式

                env = StreamExecutionEnvironment.createLocalEnvironment();
            } else { //远程执行模式

                Configuration configuration = GlobalConfiguration.loadConfiguration(getConf().getStringValue(StreamingConfig.STREAMING_FLINK_CONFIG_PATH));
                PackagedProgram program = new PackagedProgram(new File(jarPath));

                ClusterClient client = ConfigUtils.createRemoteClusterClient(executionMode, configuration, program);

                ContextEnvironmentFactory factory = new ContextEnvironmentFactory(client,
                        program.getAllLibraries(),
                        program.getClasspaths(),
                        program.getUserCodeClassLoader(),
                        getConf().getIntValue(StreamingConfig.STREAMING_FLINK_DEFAULT_PARALLEL, 1),
                        false,
                        program.getSavepointSettings());

                Method setAsContextMethod = ContextEnvironment.class.getDeclaredMethod("setAsContext", ContextEnvironmentFactory.class);
                setAsContextMethod.setAccessible(true);
                setAsContextMethod.invoke(null, factory);

                env = StreamExecutionEnvironment.getExecutionEnvironment();
            }


            CheckpointingMode mode = ConfigUtils.getCheckpointingMode(getConf().
                    getStringValue(StreamingConfig.STREAMING_FLINK_CHECKPOINT_MODE,ConfigUtils.CHECKPOINT_MODE_NONE));

            if (mode != null) { //开启checkpoint模式
                env.enableCheckpointing(getConf().getLongValue(StreamingConfig.STREAMING_FLINK_CHECKPOINT_INTERVAL), mode);
                env.setStateBackend(new FsStateBackend(getConf().getStringValue(StreamingConfig.STREAMING_FLINK_FS_BACKEND_URI),true));//异步checkpoint
            }

            buildDataStream(env);

            env.execute(getAppName());

        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamingException("error in execute application " + getAppName(), e);
        }

    }


    private void buildDataStream(StreamExecutionEnvironment env) throws StreamingException {
        visitQueue.addAll(sourcePlans);

        while (!visitQueue.isEmpty()) {
            setupDataStream(env, visitQueue.remove(0));
        }
    }

    private void setupDataStream(StreamExecutionEnvironment env, AbstractPlan plan) throws StreamingException {
        if (visiteResults.containsKey(plan))
            return;

        if (isAllParentVisited(plan)) {

            List<DataStream<TupleEvent>> inputs = Lists.newArrayList();
            List<PlanEdge> inputEdges = Lists.newArrayList();

            for (AbstractPlan parent : plan.getParents()) {
                inputs.add(visiteResults.get(parent));
                inputEdges.add(parent.getEdgeForChild(plan));
            }

            DataStream<TupleEvent> dataStream = plan.translate(env, inputs, inputEdges);

            visiteResults.put(plan, dataStream);

            visitQueue.addAll(0, plan.getChildren());
        }
    }

    private boolean isAllParentVisited(AbstractPlan plan) {
        for (AbstractPlan parent : plan.getParents()) {
            if (!visiteResults.containsKey(parent))
                return false;
        }
        return true;
    }

    @Override
    public ApplicationResults getApplications() throws StreamingException {
        return null;
    }

    @Override
    public boolean isApplicationExists() throws StreamingException {
        return false;
    }

    @Override
    public void killApplication() throws StreamingException {

    }

    @Override
    public void setUserPackagedJar(String userJar) {
        this.jarPath = userJar;
    }


    @Override
    public void deactiveApplication() throws StreamingException {
        throw new StreamingException("deactiveApplication is not supportted in flink application");

    }

    @Override
    public void activeApplication() throws StreamingException {
        throw new StreamingException("activeApplication is not supportted in flink application");

    }

    @Override
    public void rebalanceApplication(int workerNum) throws StreamingException {
        throw new StreamingException("rebalance is not supportted in flink application");
    }
}

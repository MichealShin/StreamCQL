/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.storm;

import com.huawei.streaming.application.Application;
import com.huawei.streaming.application.ApplicationResults;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.application.GroupInfo;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.storm.components.ComponentCreator;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Storm应用管理
 * <功能详细描述>
 *
 */
public class StormApplication extends Application
{
    private static final Logger LOG = LoggerFactory.getLogger(StormApplication.class);

    private TopologyBuilder builder;

    private StormConf stormConf;

    private StreamingSecurity streamingSecurity = null;

    /**
     * <默认构造函数>
     *
     */
    public StormApplication(StreamingConfig config, String appName)
        throws StreamingException
    {
        super(appName, config);
        builder = new TopologyBuilder();
        stormConf = new StormConf(config);
        streamingSecurity = SecurityFactory.createSecurity(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launch()
        throws StreamingException
    {
        //local模式下不去检查应用程序是否已经存在
        //只在远程模式下检查
        if (!stormConf.isSubmitLocal() && isApplicationExists())
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_EXISTS, getAppName());
            LOG.error("Application already exists.");
            throw exception;
        }

        if (stormConf.isSubmitLocal())
        {
            localLaunch();
        }
        else
        {
            remoteLaunch();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isApplicationExists()
        throws StreamingException
    {
        LOG.debug("Start to check is application exists.");
        Map<String, Object> conf = stormConf.createStormConf();
        NimbusClient client = null;
        try
        {
            streamingSecurity.initSecurity();
            client = NimbusClient.getConfiguredClient(conf);
            ClusterSummary clusterInfo = client.getClient().getClusterInfo();
            List<TopologySummary> list = clusterInfo.get_topologies();
            for (TopologySummary ts : list)
            {
                if (ts.get_name().equals(getAppName()))
                {
                    return true;
                }
            }
            return false;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (client != null)
            {
                client.close();
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationResults getApplications()
        throws StreamingException
    {
        NimbusClient client = null;
        try
        {
            streamingSecurity.initSecurity();
            client = NimbusClient.getConfiguredClient(stormConf.createStormConf());
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            List<TopologySummary> topologies = clusterSummary.get_topologies();
            return new StormApplicationResults(topologies);
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (client != null)
            {
                client.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killApplication()
        throws StreamingException
    {
        NimbusClient client = null;
        try
        {
            streamingSecurity.initSecurity();
            client = NimbusClient.getConfiguredClient(stormConf.createStormConf());
            client.getClient().killTopologyWithOpts(getAppName(), createKillOptions());
            int maxRetryTime = stormConf.getKillApplicationOverTime();
            while (true)
            {
                if (maxRetryTime <= 0)
                {
                    StreamingException exception = new StreamingException(ErrorCode.PLATFORM_KILL_OVERTIME);
                    LOG.error("Kill application timeout.", exception);
                    throw exception;
                }
                ClusterSummary clusterInfo = client.getClient().getClusterInfo();
                List<TopologySummary> list = clusterInfo.get_topologies();
                if (isApplicationExistsAfterKilled(list))
                {
                    maxRetryTime--;
                    sleepSeconds(1);
                }
                else
                {
                    break;
                }
            }
        }
        catch (NotAliveException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, getAppName());
            LOG.error("Application {} not exists.", getAppName());
            throw exception;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            //为了兼容社区版本和HA版本，防止引入HA相关异常
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }

            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }

        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (client != null)
            {
                client.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUserPackagedJar(String userJar)
    {
        LOG.info("reset submit jar to {}", userJar);
        stormConf.setDefaultJarPath(userJar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deactiveApplication()
        throws StreamingException
    {
        String appName = getAppName();
        LOG.info("Start to deactive application {}.", appName);
        NimbusClient nimbusClient = null;
        try
        {
            streamingSecurity.initSecurity();

            nimbusClient = NimbusClient.getConfiguredClient(stormConf.createStormConf());
            Nimbus.Client client = nimbusClient.getClient();

            List<TopologySummary> topologySummarys = client.getClusterInfo().get_topologies();
            //获取应用程序当前状态
            ApplicationStatus status = getAppStatus(topologySummarys, appName);
            //执行deactive操作可用性检查，不可用则抛出异常
            status.deactiveValidate(appName);

            client.deactivate(getAppName());
        }
        catch (NotAliveException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, appName);
            LOG.error("Application {} not exists.", appName);
            throw exception;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            //为了兼容社区版本和HA版本，防止引入HA相关异常
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }

            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (nimbusClient != null)
            {
                nimbusClient.close();
            }
        }

        LOG.info("Success to deactive application {}.", appName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void activeApplication()
        throws StreamingException
    {
        String appName = getAppName();
        LOG.info("Start to active application {}.", appName);
        NimbusClient nimbusClient = null;
        try
        {
            streamingSecurity.initSecurity();

            nimbusClient = NimbusClient.getConfiguredClient(stormConf.createStormConf());
            Nimbus.Client client = nimbusClient.getClient();

            List<TopologySummary> topologySummarys = client.getClusterInfo().get_topologies();
            //获取应用程序当前状态
            ApplicationStatus status = getAppStatus(topologySummarys, appName);
            //执行active操作可用性检查，不可用则抛出异常
            status.activeValidate(appName);

            client.activate(appName);
        }
        catch (NotAliveException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, appName);
            LOG.error("Application {} not exists.", getAppName());
            throw exception;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (nimbusClient != null)
            {
                nimbusClient.close();
            }
        }
        LOG.info("Success to active application {}.", appName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rebalanceApplication(int workerNum)
        throws StreamingException
    {
        String appName = getAppName();
        LOG.info("Start to rebalance application {}.", appName);
        NimbusClient nimbusClient = null;
        try
        {
            streamingSecurity.initSecurity();

            nimbusClient = NimbusClient.getConfiguredClient(stormConf.createStormConf());
            Nimbus.Client client = nimbusClient.getClient();

            List<SupervisorSummary> supervisorSummarys = client.getClusterInfo().get_supervisors();
            List<TopologySummary> topologySummarys = client.getClusterInfo().get_topologies();
            //获取应用程序当前状态
            ApplicationStatus status = getAppStatus(topologySummarys, appName);
            //执行rebalance操作可用性检查，不可用则抛出异常
            status.rebalanceValidate(appName);
            //检查worker数量是否合法，不合法则抛出异常
            checkUserWorkerNum(supervisorSummarys, topologySummarys, workerNum);

            client.rebalance(appName, createRebalanceOptions(workerNum));
        }
        catch (NotAliveException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, appName);
            LOG.error("Application {} not exists.", getAppName());
            throw exception;
        }
        catch (InvalidTopologyException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY, appName);
            LOG.error("Application {} is invalid.", getAppName());
            throw exception;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (TException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
            LOG.error("Failed to connect to application server for thrift error.");
            throw exception;
        }
        catch (Exception e)
        {
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
            if (nimbusClient != null)
            {
                nimbusClient.close();
            }
        }
        LOG.info("Success to rebalance application {}.", appName);
    }

    private RebalanceOptions createRebalanceOptions(int workerNum)
    {
        RebalanceOptions options = new RebalanceOptions();
        options.set_wait_secs(stormConf.getRebalanceWaitSecs());
        options.set_num_workers(workerNum);
        return options;
    }

    private boolean isApplicationExistsAfterKilled(List<TopologySummary> list)
    {
        boolean isFound = false;
        for (TopologySummary ts : list)
        {
            if (ts.get_name().equals(getAppName()))
            {
                isFound = true;
                break;
            }
        }
        return isFound;
    }

    private KillOptions createKillOptions()
    {
        KillOptions kop = new KillOptions();
        kop.set_wait_secs(stormConf.getKillWaitingSeconds());
        return kop;
    }

    private void sleepSeconds(int seconds)
    {
        try
        {
            TimeUnit.SECONDS.sleep(seconds);
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupted while thread sleep.");
        }
    }

    /**
     * 创建topology，远程提交拓扑时使用
     *
     */
    private void createTopology()
        throws StreamingException
    {
        createSpouts();
        createBolts();
    }

    private void createSpouts()
        throws StreamingException
    {
        List<? extends IRichOperator> sources = getInputStreams();
        checkInputStreams(sources);
        for (IRichOperator input : sources)
        {
            IRichSpout spout = ComponentCreator.createSpout(input);
            builder.setSpout(input.getOperatorId(), spout, input.getParallelNumber());
        }
    }

    private void checkInputStreams(List<? extends IRichOperator> operators)
        throws StreamingException
    {
        if (null == operators || operators.isEmpty())
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NO_INPUT_OPERATOR);
            LOG.error("No input operator.");
            throw exception;
        }
    }

    private void createBolts()
        throws StreamingException
    {
        List<IRichOperator> orderedFunOp = genFunctionOpsOrder();
        if (orderedFunOp == null || orderedFunOp.isEmpty())
        {
            LOG.debug("Topology don't have any function operator");
            return;
        }

        for (IRichOperator operator : orderedFunOp)
        {
            setOperatorGrouping(operator);
        }
    }

    private void setOperatorGrouping(IRichOperator operator)
        throws StreamingException
    {
        BoltDeclarer bolt = createBoltDeclarer(operator);
        List<String> streams = operator.getInputStream();
        if (streams == null)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error("The operator input streaming is null.");
            throw exception;
        }

        for (String streamName : operator.getInputStream())
        {
            GroupInfo groupInfo = operator.getGroupInfo().get(streamName);
            setBoltGrouping(bolt, streamName, groupInfo);
        }
    }

    private void setBoltGrouping(BoltDeclarer bolt, String streamName, GroupInfo groupInfo)
        throws StreamingException
    {
        if (null == groupInfo)
        {
            setDefaultBoltGrouping(bolt, streamName);
            return;
        }

        DistributeType distribute = groupInfo.getDitributeType();
        switch (distribute)
        {
            case FIELDS:
                Fields fields = new Fields(groupInfo.getFields());
                IRichOperator operator = getOperatorByOutputStreamName(streamName);
                if (operator == null)
                {
                    StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                    LOG.error("Can't find operator by stream name : {} .", streamName, exception);
                    throw exception;
                }
                bolt.fieldsGrouping(operator.getOperatorId(), streamName, fields);
                break;
            case GLOBAL:
                break;
            case LOCALORSHUFFLE:
                break;
            case ALL:
                break;
            case DIRECT:
                break;
            case CUSTOM:
                break;
            case SHUFFLE:
            case NONE:
            default:
                setDefaultBoltGrouping(bolt, streamName);
        }
    }

    private void setDefaultBoltGrouping(BoltDeclarer bolt, String streanName)
        throws StreamingException
    {
        IRichOperator operator = getOperatorByOutputStreamName(streanName);
        if (operator == null)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error("Can't find operator by stream name : {} .", streanName, exception);
            throw exception;
        }
        bolt.shuffleGrouping(operator.getOperatorId(), streanName);
    }

    private BoltDeclarer createBoltDeclarer(IRichOperator operator)
        throws StreamingException
    {
        IRichBolt bolt = ComponentCreator.createBolt(operator, stormConf);
        return builder.setBolt(operator.getOperatorId(), bolt, operator.getParallelNumber());
    }

    private void localLaunch()
        throws StreamingException
    {
        Map<String, Object> stormConfig = this.stormConf.createStormConf();
        createTopology();
        StormTopology topology = builder.createTopology();
        if (this.stormConf.isTestModel())
        {
            return;
        }
        this.stormConf.setStormJar();
        LocalCluster cluster = null;
        try
        {
            streamingSecurity.initSecurity();
            cluster = new LocalCluster();
            cluster.submitTopology(getAppName(), stormConfig, topology);
            long aliveTime = this.stormConf.getLocalTaskAliveTime();
            sleepMilliSeconds(aliveTime);
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }

            if (cluster != null)
            {
                cluster.shutdown();
            }
        }
    }

    private void remoteLaunch()
        throws StreamingException
    {
        //创建APP拓扑
        createTopology();
        Map<String, Object> conf = stormConf.createStormConf();
        stormConf.setStormJar();
        StormTopology topology = builder.createTopology();
        synchronized (StormSubmitter.class)
        {
            submitTopology(conf, topology);
        }
    }

    private void submitTopology(Map<String, Object> conf, StormTopology topology)
        throws StreamingException
    {
        try
        {
            streamingSecurity.initSecurity();
            StormSubmitter.submitTopology(getAppName(), conf, topology);
        }
        catch (AlreadyAliveException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_EXISTS, getAppName());
            LOG.error("Application already exists.");
            throw exception;
        }
        catch (InvalidTopologyException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error("The submit topology is invalid.");
            throw exception;
        }
        catch (AuthorizationException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
            LOG.error("No Authorization.");
            throw exception;
        }
        catch (Exception e)
        {
            //为了兼容社区版本和HA版本，防止引入HA相关异常
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }

            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to connect to application server.");
            throw exception;
        }
        finally
        {
            try
            {
                streamingSecurity.destroySecurity();
            }
            catch (StreamingException e)
            {
                LOG.warn("Destory Security error.");
            }
        }
    }

    private void sleepMilliSeconds(long milliSeconds)
    {
        try
        {
            TimeUnit.MILLISECONDS.sleep(milliSeconds);
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupt while thread sleep.");
        }
    }

    private ApplicationStatus getAppStatus(List<TopologySummary> topologySummarys, String appName)
        throws StreamingException
    {
        String status = null;
        List<String[]> res = new StormApplicationResults(topologySummarys).getResults(appName);
        for (String[] single : res)
        {
            if (single[0].equals(appName))
            {
                status = single[1].toLowerCase(Locale.US);
            }
        }
        if (null == status)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, getAppName());
            LOG.error("Application {} not exists.", getAppName());
            throw exception;
        }
        return ApplicationStatus.getType(status);
    }

    private int getFreeSlotNum(List<SupervisorSummary> summarys)
    {
        int num = 0;
        for (SupervisorSummary summary : summarys)
        {
            int used = summary.get_num_used_workers();
            int total = summary.get_num_workers();
            int free = total - used;
            num += free;
        }
        return num;
    }

    private Pair<Integer, Integer> getExecutorNum(List<TopologySummary> summaries)
    {
        int currentExecutorNum = 0;
        int currentWorkerNum = 0;
        for (TopologySummary summary : summaries)
        {
            if (summary.get_name().equals(getAppName()))
            {
                currentExecutorNum = summary.get_num_executors();
                currentWorkerNum = summary.get_num_workers();
            }
        }
        return new Pair<Integer, Integer>(currentExecutorNum, currentWorkerNum);
    }

    private void checkUserWorkerNum(List<SupervisorSummary> supervisorSummaries,
        List<TopologySummary> topologySummaries, int num)
        throws StreamingException
    {
        int currentExecutorNum = getExecutorNum(topologySummaries).getFirst();
        int availableSlotNum = getFreeSlotNum(supervisorSummaries) + getExecutorNum(topologySummaries).getSecond();
        if ((num > availableSlotNum) || (num > currentExecutorNum))
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_WORKER_NUMBER,
                String.valueOf(availableSlotNum),
                String.valueOf(currentExecutorNum));
            LOG.error("The worker number is invalid");
            throw exception;
        }
    }
}

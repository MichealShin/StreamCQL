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

package com.huawei.streaming.cql.executor.userdefined;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.serde.BaseSerDe;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 向控制台打印的Bolt
 * <功能详细描述>
 *
 */
public class ConsolePrintOutput implements IOutputStreamOperator
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -7280927308996596403L;
    
    private static final Logger LOG = LoggerFactory.getLogger(ConsolePrintOutput.class);

    private StreamSerDe serde;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        LOG.info(getTupleString(event));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        this.serde = streamSerDe;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
    {
        config = conf;
    }

    /**
     * 获取配置属性
     * 编译时接口
     *
     */
    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

    private String getTupleString(TupleEvent event)
    {
        String result = null;
        try
        {
            result = (String)serde.serialize(BaseSerDe.changeEventsToList(event));
            
        }
        catch (StreamSerDeException e)
        {
            LOG.error("failed to serialize data.");
        }
        return result;
    }
    
}

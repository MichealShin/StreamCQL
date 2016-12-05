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

import java.util.Map;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IFunctionStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * filter Stream
 *
 */
public class WebFilterStream implements IFunctionStreamOperator
{
    private static final String FILTER_STRING = "http://www.huawei.com";
    
    private static final long serialVersionUID = 9046591894755351055L;
    
    private Map<String, IEmitter> emitters = null;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        this.config = conf;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEmitter(Map<String, IEmitter> emitterMap)
    {
        emitters = emitterMap;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        if (event == null)
        {
            return;
        }
        Object[] values = event.getAllValues();
        if (FILTER_STRING.equals(values[1].toString()))
        {
            for(IEmitter emitter : emitters.values())
            {
                emitter.emit(values);
            }
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        
    }

}

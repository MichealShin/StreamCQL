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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.BaseSerDe;

/**
 * 默认的数据序列化类
 *
 */
public class StrJoinSerializer extends BaseSerDe
{
    private static final Logger LOG = LoggerFactory.getLogger(StrJoinSerializer.class);
    
    private static final long serialVersionUID = -2364817027725796314L;
    
    private String separator = ",";
    
    private StringBuilder sb = new StringBuilder();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        super.setConfig(conf);
        separator = getConfig().getStringValue(StreamingConfig.SERDE_SIMPLESERDE_SEPARATOR);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object[]> deSerialize(Object data)
        throws StreamSerDeException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List<Object[]> event)
        throws StreamSerDeException
    {
        if (event == null)
        {
            LOG.info("Input event is null");
            return null;
        }
        
        sb.delete(0, sb.length());
        
        for (int i = 0; i < event.size(); i++)
        {
            Object[] vals = event.get(i);
            for (int j = 0; j < vals.length; j++)
            {
                sb.append(vals[j].toString() + separator);
            }
            sb.replace(sb.length() - 1, sb.length() - 1, "");
        }
        
        return sb.substring(0, sb.length() - 1);
        
    }
}

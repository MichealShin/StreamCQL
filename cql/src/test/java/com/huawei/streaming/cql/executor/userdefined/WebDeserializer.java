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

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.serde.BaseSerDe;

/**
 * smartcare 场景测试反序列化类
 *
 */
public class WebDeserializer extends BaseSerDe
{
    private static final long serialVersionUID = 5293310260707418035L;
    
    private static final Logger LOG = LoggerFactory.getLogger(WebDeserializer.class);

    /**
     * 将原始数据按照设定格式分解
     *
     */
    @Override
    public List<Object[]> deSerialize(Object data)
        throws StreamSerDeException
    {
        if (data == null)
        {
            LOG.info("Input raw data is null");
            return Lists.newArrayList();
        }
        byte[] bytes = (byte[])data;
        
        String[] results = parseEDR(bytes);
        List<Object[]> splitResults = Lists.newArrayList();
        splitResults.add(results);
        return createAllInstance(splitResults);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List<Object[]> events)
        throws StreamSerDeException
    {
        return null;
    }
    
    private String[] parseEDR(byte[] bt)
    {
        String[] starr = new String[CQLConst.I_3];
        int WEB_MESSAGE_MSISDN_OFFSET = 41;
        int WEB_MESSAGE_MSISDN_LENGTH = 16;
        int WEB_MESSAGE_HOST_OFFSET = 357;
        int WEB_MESSAGE_HOST_LENGTH = 64;
        int WEB_MESSAGE_FST_URI_OFFSET = 421;
        int WEB_MESSAGE_FST_URI_LENGTH = 128;
        
        char[] chs = new char[WEB_MESSAGE_MSISDN_LENGTH];
        for (int i = WEB_MESSAGE_MSISDN_OFFSET; i < WEB_MESSAGE_MSISDN_OFFSET + WEB_MESSAGE_MSISDN_LENGTH; ++i)
        {
            if (bt[i] == 0x0)
            {
                break;
            }
            //sb1.append((char)bt[i]);
            chs[i - WEB_MESSAGE_MSISDN_OFFSET] = (char)bt[i];
        }
        starr[0] = String.valueOf(chs).trim();
        
        char[] chs2 = new char[WEB_MESSAGE_HOST_LENGTH];
        for (int i = WEB_MESSAGE_HOST_OFFSET; i < WEB_MESSAGE_HOST_OFFSET + WEB_MESSAGE_HOST_LENGTH; ++i)
        {
            if (bt[i] == 0x0)
            {
                break;
            }
            chs2[i - WEB_MESSAGE_HOST_OFFSET] = (char)bt[i];
        }
        starr[1] = String.valueOf(chs2).trim();
        
        char[] chs3 = new char[WEB_MESSAGE_FST_URI_LENGTH];
        for (int i = WEB_MESSAGE_FST_URI_OFFSET; i < WEB_MESSAGE_FST_URI_OFFSET + WEB_MESSAGE_FST_URI_LENGTH; ++i)
        {
            if (bt[i] == 0x0)
            {
                break;
            }
            chs3[i - WEB_MESSAGE_FST_URI_OFFSET] = (char)bt[i];
        }
        starr[CQLConst.I_2] = String.valueOf(chs3).trim();
        
        return starr;
    }
}

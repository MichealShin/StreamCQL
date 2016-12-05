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

package com.huawei.streaming.udfs;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.*;

/**
 * 数据类型转换测试
 *
 */
public class ToFloatTest
{

    /**
     * 测试数据类型转换
     */
    @Test
    public void testEvaluate() throws StreamingException
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        ToFloat toFloat = new ToFloat(config);
        ToDate toDate = new ToDate(config);
        ToTime toTime = new ToTime(config);
        ToTimeStamp toTimeStamp = new ToTimeStamp(config);
        ToDecimal toDecimal = new ToDecimal(config);
        assertTrue(toFloat.evaluate(1).equals(1.0F));
        assertTrue(toFloat.evaluate(1F).equals(1.0F));
        assertTrue(toFloat.evaluate(1.0f).equals(1.0F));
        assertTrue(toFloat.evaluate(1.4f).equals(1.4F));
        assertTrue(toFloat.evaluate(1.5f).equals(1.5F));
        assertTrue(toFloat.evaluate(1.6f).equals(1.6F));
        assertTrue(toFloat.evaluate(1.9f).equals(1.9F));
        assertTrue(toFloat.evaluate(1.9d).equals(1.9F));
        assertTrue(toFloat.evaluate("1").equals(1.0F));
        assertTrue(toFloat.evaluate("1.9").equals(1.9F));
        assertTrue(toFloat.evaluate(toDecimal.evaluate("1.9")).equals(1.9F));
        assertTrue(toFloat.evaluate(toDate.evaluate("1970-01-01")).equals(0.0F));
        assertTrue(toFloat.evaluate(toTime.evaluate("15:40:00")).equals(56400000.0F));
        assertTrue(toFloat.evaluate(toTimeStamp.evaluate("1970-01-01 15:40:00.000000")).equals(27600000.0F));
    }
}

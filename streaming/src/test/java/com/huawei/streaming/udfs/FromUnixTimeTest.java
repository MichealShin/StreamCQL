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

import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.MethodExpression;

/**
 * 时间类型的数据格式化为字符串形式
 * 
 */
public class FromUnixTimeTest
{
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluateLong()
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        FromUnixTime udf = new FromUnixTime(config);
        assertTrue(udf.evaluate(Long.valueOf(0)).toString().equals("1970-01-01 08:00:00"));
        assertTrue(udf.evaluate(Long.valueOf(1)).toString().equals("1970-01-01 08:00:01"));
        assertTrue(udf.evaluate(Long.valueOf(100)).toString().equals("1970-01-01 08:01:40"));
        assertTrue(udf.evaluate(Long.valueOf(-999999)).toString().equals("1969-12-20 18:13:21"));
    }
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluateLongString()
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        FromUnixTime udf = new FromUnixTime(config);
        assertTrue(udf.evaluate(Long.valueOf(0), "yyyy-MM-dd").toString().equals("1970-01-01"));
        assertTrue(udf.evaluate(Long.valueOf(0), "HH:mm:ss").toString().equals("08:00:00"));
        assertTrue(udf.evaluate(Long.valueOf(0), "yyyy-MM-dd HH:mm:ss").toString().equals("1970-01-01 08:00:00"));
        assertTrue(udf.evaluate(Long.valueOf(0), "yyyy-MM-dd HH:mm:ss.SSS").toString().equals("1970-01-01 08:00:00.000"));
    }
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluateIntegerString()
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        FromUnixTime udf = new FromUnixTime(config);
        assertTrue(udf.evaluate(0, "yyyy-MM-dd").equals("1970-01-01"));
    }
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluate1()
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        FromUnixTime udf = new FromUnixTime(config);
        assertNotNull(udf.evaluate(1000, "yyyy-MM-dd HH:mm:ss"));
        assertNotNull(udf.evaluate(1000, "yyyy-MM-dd"));
        assertNotNull(udf.evaluate(1000, "HH:mm:ss"));
        assertNotNull(udf.evaluate(1000l));
        assertNotNull(udf.evaluate(1000l, "yyyy-MM-dd HH:mm:ss"));
        assertNotNull(udf.evaluate(1000l, "yyyy-MM-dd"));
        assertNotNull(udf.evaluate(1000l, "HH:mm:ss"));
        
        IExpression[] exps = new IExpression[] {new ConstExpression(Integer.valueOf(1000)), new ConstExpression("HH:mm:ss")};
        MethodExpression methodExpression = new MethodExpression(new FromUnixTime(config), "evaluate", exps);
        
        System.out.println(methodExpression.evaluate(new TupleEvent()));
        
    }
}

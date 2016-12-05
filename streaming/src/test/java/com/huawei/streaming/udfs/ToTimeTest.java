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
import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * 字符串到时间转换测试用例
 * 
 */
public class ToTimeTest
{
 
    /**
     * 时间转换测试
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
        ToTime udf = new ToTime(config);
        assertNull(udf.evaluate("2014-09-25 17:07:00"));
        assertNotNull(udf.evaluate("17:07:00"));
        assertNull(udf.evaluate("17:07:90"));
        assertNotNull(udf.evaluate("16:44:00"));
    }
    
}

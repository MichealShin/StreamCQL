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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.huawei.streaming.exception.StreamingException;
import org.junit.Test;

/**
 * 日期比较测试用例
 * 
 */
public class DateDiffTest
{
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluate()
        throws StreamingException
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        config.put(StreamingConfig.STREAMING_OPERATOR_TIMEZONE, "GMT+08:00");
        DateDiff udf = new DateDiff(config);
        assertTrue(udf.evaluate("2013-10-17", "2013-10-17") == 0);
        assertTrue(udf.evaluate("2013-10-17 09:42:00.000", "2013-10-17 09:42:01.111") == 0);
        assertTrue(udf.evaluate("2013-10-17 09:42:00", "2013-10-17 09:42:01") == 0);
        assertTrue(udf.evaluate("2013-10-17 00:00:00", "2013-10-16 23:59:59") == 1);
        assertTrue(udf.evaluate("2013-10-17 09:42:00", "2013-10-16 09:42:01") == 1);
        assertTrue(udf.evaluate("2013-10-17 09:42:00", "2013-10-16 09:41:01") == 1);
        assertTrue(udf.evaluate("2013-10-17 22:42:00 +0800", "2013-10-17 22:41:01 -0800") == -1);
        assertTrue(udf.evaluate("2013-10-17 09:42:00", "2013-10-16 09:100:01") == null);
        assertTrue(udf.evaluate("2013-10-17", "2013-10-16 09:41:01") == 1);
        assertTrue(udf.evaluate("2013-10-17", "2013-10-16") == 1);
        assertTrue(udf.evaluate("2013-10-16", "2013-10-17") == -1);
        assertNull(udf.evaluate("2013-10-16", "2013"));
    }
}

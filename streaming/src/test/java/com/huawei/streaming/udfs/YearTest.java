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
import static org.junit.Assert.assertTrue;

import com.huawei.streaming.exception.StreamingException;
import org.junit.Test;

/**
 * 时间类udf函数测试用例
 * 
 */
public class YearTest
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
        Year udf = new Year(config);
        assertTrue(udf.evaluate("2013-10-17 09:58:00.111") == TestUDFCommon.I_2013);
        assertTrue(udf.evaluate("2013-10-17 09:58:00") == TestUDFCommon.I_2013);
        assertTrue(udf.evaluate("2013-10-17 25:62:00") == null);
        assertTrue(udf.evaluate("2012-12-31 17:58:00.111 -0800") == TestUDFCommon.I_2013);

        udf = new Year(config);
        assertTrue(udf.evaluate("2013-00-17") == null);
        assertTrue(udf.evaluate("2013-10-17") == TestUDFCommon.I_2013);
        udf = new Year(config);
        assertTrue(udf.evaluate("09:58:00") == null);
    }
    
}

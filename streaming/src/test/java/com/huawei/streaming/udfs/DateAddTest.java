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

import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.huawei.streaming.util.datatype.TimeZoneUtils;
import org.junit.Test;

/**
 * 日期相加测试
 * 
 */
public class DateAddTest
{
    
    /**
     * 日期相加测试用例
     */
    @Test
    public void testEvaluateStringInteger()
        throws StreamingException
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue().toString());
        }
        config.put(StreamingConfig.STREAMING_OPERATOR_TIMEZONE, "GMT+08:00");
        DateAdd udf = new DateAdd(config);
        assertTrue(udf.evaluate("2013-10-17 09:42:00.111", 1).equals("2013-10-18 09:42:00.111 +0800"));
        assertTrue(udf.evaluate("2013-10-17 09:42:00", 1).equals("2013-10-18 09:42:00.000 +0800"));
        assertTrue(udf.evaluate("2013-10-17 22:42:00 -0800", 1).equals("2013-10-19 14:42:00.000 +0800"));
        assertNull(udf.evaluate("2013-10-17 09:100:00",1));
        assertNull(udf.evaluate("2013-10-17 09:100:00", 10000));
        udf = new DateAdd(config);
        assertTrue(udf.evaluate("1970-01-01", 1).equals("1970-01-02"));
        assertTrue(udf.evaluate("2013-10-17", 1).equals("2013-10-18"));
        assertTrue(udf.evaluate("2013-10-17", -1).equals("2013-10-16"));
    }
    
    /**
     * 日期相加测试用例
     */
    @Test
    public void testEvaluateTimestampInteger() throws StreamingException
    {
        Map<String, String> config = Maps.newHashMap();
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }
        config.put(StreamingConfig.STREAMING_OPERATOR_TIMEZONE, "GMT+08:00");
        DateAdd udf = new DateAdd(config);
        ToTimeStamp toTimeStamp = new ToTimeStamp(config);
        assertTrue(udf.evaluate(toTimeStamp.evaluate("2013-10-17 09:42:00"), 1).equals("2013-10-18 09:42:00.000 +0800"));
        assertTrue(udf.evaluate(toTimeStamp.evaluate("2013-10-17 09:42:00"), -1).equals("2013-10-16 09:42:00.000 +0800"));
    }
}

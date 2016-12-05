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

package com.huawei.streaming.util.datatype;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.assertEquals;

/**
 * timestamp测试用例
 * Created by h00183771 on 2015/11/20.
 */
public class TimestampParserTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void formatSimpleTimeStampFormat()
     throws StreamingException, ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals(formatter.parse("1970-01-01 00:00:00").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-01 00:00:00").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-1 00:00:00").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-1 0:0:0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-1 0:0:0.0").getTime(), -28800000);
    }

    @Test
    public void formatMSTimeStampFormat()
     throws StreamingException, ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);
        assertEquals(formatter.parse("1970-01-01 00:00:00.0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-01 00:00:00.0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-1 00:00:00.0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-1 0:00:00.0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-1-1 0:0:0.0").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.00").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.000").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.000000").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.100").getTime(), -28799900);
        assertEquals(formatter.parse("1970-01-01 0:0:0.000000000").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.0000000000").getTime(), -28800000);
        assertEquals(formatter.parse("1970-01-01 0:0:0.001").getTime(), -28799999);
        assertEquals(formatter.parse("1970-01-01 0:0:0.0001").getTime(), -28799999);
        assertEquals(formatter.parse("1970-01-01 0:0:0.000001").getTime(), -28799999);
        assertEquals(formatter.parse("1970-01-01 0:0:0.000000001").getTime(), -28799999);
        assertEquals(formatter.parse("1970-01-01 0:0:0.0000000001").getTime(), -28799999);
        assertEquals(formatter.parse("1970-01-01 0:0:0.0100").getTime(), -28799900);
        assertEquals(formatter.parse("1970-01-01 0:0:0.010").getTime(), -28799990);
    }

    @Test
    public void formatMSTimeStampFormat2()
     throws StreamingException, ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);
        assertEquals(formatter.parse("1970-01-01 00:00:00.111").getTime(), -28799889);
        thrown.expect(ParseException.class);
        assertEquals(formatter.parse("1970-01-01 00:00:00").getTime(), -28800000);
    }

    @Test
    public void formatMSTimeStampFormat3()
     throws StreamingException, ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);
        thrown.expect(ParseException.class);
        assertEquals(formatter.parse("1970-01-01 00:00:00").getTime(), -28800000);
    }


    @Test
    public void formatMSTimeStampFormat4()
     throws StreamingException, ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);
        thrown.expect(ParseException.class);
        assertEquals(formatter.parse("1970-01-01 00:00:00.1111").getTime(), -28799889);
    }


    @Test
    public void formatMSTZTimeStampFormat()
     throws StreamingException, ParseException
    {

        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        //        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);
        assertEquals(formatter.parse("1970-01-01 00:00:00.0 -0800").getTime(), 28800000);
        assertEquals(formatter.parse("1970-01-01 00:00:00.0 -0000").getTime(), 0);
        assertEquals(formatter.parse("1970-01-01 00:00:00.0 +0000").getTime(), 0);
        assertEquals(formatter.parse("1970-01-01 08:00:00.0 +0800").getTime(), 0);
        assertEquals(formatter.parse("1970-01-01 00:00:00.0 +0800").getTime(), -28800000);
    }

    @Test(expected = ParseException.class)
    public void formatMSTZTimeStampFormatWithException0() throws ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setLenient(false);
        //格式化必须加+号或者-号
        formatter.parse("1970-01-01 00:00:00.0 0000").getTime();
    }

    @Test(expected = ParseException.class)
    public void formatMSTZTimeStampFormatWithException1() throws ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setLenient(false);
        //时区必须是4位
        formatter.parse("1970-01-01 00:00:00.0 +800");
    }

    @Test
    public void formatMSTZTimeStampFormatWithException2() throws ParseException
    {
        //验证不同格式数据格式化方法
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setLenient(false);
        thrown.expect(ParseException.class);
        //时区必须是4位
        formatter.parse("1970-01-01 00:00:00.0 +080");
    }

    @Test
    public void parseTest() throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.STREAMING_OPERATOR_TIMEZONE, "Asia/Shanghai");
        TimestampParser parser = new TimestampParser(config);
        assertEquals(((Timestamp)parser.createValue("1970-01-01 00:00:00")).getTime(), -28800000);
        assertEquals(((Timestamp)parser.createValue("1970-01-01 00:00:00 +0800")).getTime(), -28800000);
        assertEquals(((Timestamp)parser.createValue("1970-01-01 00:00:00.000")).getTime(), -28800000);
        assertEquals(((Timestamp)parser.createValue("1970-01-01 00:00:00.0 +0800")).getTime(), -28800000);
    }

    @Test
    public void formatTest() throws StreamingException, ParseException
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        formatter.setLenient(false);

        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.STREAMING_OPERATOR_TIMEZONE, "Asia/Shanghai");
        TimestampParser parser = new TimestampParser(config);
        assertEquals(parser.toStringValue(formatter.parse("1970-01-01 00:0:0.000")), "1970-01-01 00:00:00.000 +0800");
    }
}

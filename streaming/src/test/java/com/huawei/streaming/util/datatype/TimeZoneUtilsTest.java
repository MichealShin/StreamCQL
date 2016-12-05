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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.assertEquals;

/**
 * 时区解析测试用例
 * Created by h00183771 on 2015/11/18.
 */
public class TimeZoneUtilsTest
{
    /**
     * timestamp字符串格式
     */
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 带纳秒的时间戳字段
     * 一般用于格式化输出
     * 只能精确到毫秒
     */
    public static final String TIMESTAMP_MSTIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final String TIMESTAMP_MSTIME_TIMEZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS Z";

    private static final SimpleDateFormat STRING_MSTIME_FORMATTER = new SimpleDateFormat(TIMESTAMP_MSTIME_FORMAT);

    /*
     * timestamp类型允许输入带毫秒，纳秒的数字
     * 所以格式精确到秒级别。
     */
    private static final SimpleDateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);

    private static final SimpleDateFormat TIMESTAMP_TIMEZONE_FORMATTER = new SimpleDateFormat(TIMESTAMP_MSTIME_TIMEZONE_FORMAT);

    private static final SimpleDateFormat TIMEZONE_FORMATTER = new SimpleDateFormat("z");
    static
    {
        TIMESTAMP_FORMATTER.setLenient(false);
        STRING_MSTIME_FORMATTER.setLenient(false);
        TIMESTAMP_TIMEZONE_FORMATTER.setLenient(false);
        TIMEZONE_FORMATTER.setLenient(false);
    }


    @Test
    public void timeZoneTest()
     throws StreamingException, ParseException
    {
        assertEquals(TimeZoneUtils.parseTimeZone("GMT+08:00").getID(), "GMT+08:00");
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/Shanghai").getRawOffset(), 28800000);
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/Chongqing").getRawOffset(), 28800000);
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/Harbin").getRawOffset(), 28800000);
    }

    @Test
    public void timeZoneErrorTest()
     throws StreamingException, ParseException
    {
        assertEquals(TimeZoneUtils.parseTimeZone("08:00"), null);
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/ Shanghai"), null);
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/Chong qing"), null);
        assertEquals(TimeZoneUtils.parseTimeZone("Asia/Beijing"), null);
    }


    @Test
    public void testParse1()
     throws StreamingException, ParseException
    {
        /**
         * 输入时间测试
         */
        String s = "1970-01-01 00:00:00";
        TIMESTAMP_FORMATTER.setTimeZone(TimeZoneUtils.parseTimeZone("GMT+08:00"));
        Date date = TIMESTAMP_FORMATTER.parse(s);
        assertEquals(date.getTime(), -28800000);
        TIMESTAMP_FORMATTER.setTimeZone(TimeZoneUtils.parseTimeZone("GMT+00:00"));
        date = TIMESTAMP_FORMATTER.parse(s);
        assertEquals(date.getTime(), 0);

    }


    @Test
    public void formatTest()
     throws StreamingException, ParseException
    {
        /**
         * 输入时间测试
         */
        String s = "1970-01-01 00:00:00";
        TIMESTAMP_FORMATTER.setTimeZone(TimeZoneUtils.parseTimeZone("GMT+00:00"));
        Date date = TIMESTAMP_FORMATTER.parse(s);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        assertEquals(formatter.format(date), "1970-01-01 08:00:00.000 +0800");

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        assertEquals(formatter.format(date), "1970-01-01 08:00:00.000 CST");

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        assertEquals(formatter.format(date), "1970-01-01 08:00:00.000 +0800");

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZ");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        assertEquals(formatter.format(date), "1970-01-01 08:00:00.000 +0800");

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("Asia/Shanghai"));
        assertEquals(formatter.format(date), "1970-01-01 08:00:00.000 +0800");

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        formatter.setTimeZone(TimeZoneUtils.parseTimeZone("America/Sao_Paulo"));
        assertEquals(formatter.format(date), "1969-12-31 21:00:00.000 -0300");

    }

}

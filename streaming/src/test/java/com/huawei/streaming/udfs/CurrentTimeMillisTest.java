package com.huawei.streaming.udfs;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 获取系统当前毫秒时间测试用例
 */
public class CurrentTimeMillisTest
{

    /**
     * 测试用例
     */
    @Test
    public void testEvaluate()
    {
        CurrentTimeMillis udf = new CurrentTimeMillis(null);
        assertNotNull(udf.evaluate());
    }
}
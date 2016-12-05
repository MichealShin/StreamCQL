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

package com.huawei.streaming.expression;

import org.junit.Assert;
import org.junit.Test;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;

/**
 * <LikeExpressionTest>
 * <功能详细描述>
 * 
 */
public class LikeExpressionTest
{
    private IEvent theEvent = new TupleEvent();
    
    private IEvent[] theEvents = new IEvent[] {new TupleEvent()};
    
    private LikeExpression likeExpr = null;
    
    private Object actualResult = null;
    
    /**
     * testLikeExpression
     */
    @Test
    public void testLikeExpression()
    {
        try
        {
            likeExpr = new LikeExpression(new ConstExpression("aaaaa"), null, true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            likeExpr = new LikeExpression(null, null, true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "", true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
    }
    
    /**
     * testEvaluateIEvent
     */
    @Test
    public void testEvaluateIEvent()
        throws StreamingException
    {
        
        //NONE
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaaa", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaaa", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaab", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaab", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //END
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%a", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%a", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //BEGIN
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aa%", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aa%", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "ab%", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "ab%", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //MID
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%aa%", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%aa%", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab%", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab%", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //Complex
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "_aa%", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "_aa%", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab_", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab_", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a123"), "a[1-9][1-9][1-9]", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a123"), "a[1-9][1-9][1-9]", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a023"), "a[1-9][1-9][1-9]", true);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a023"), "a[1-9][1-9][1-9]", false);
        
        actualResult = likeExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
    }
    
    /**
     * testEvaluateIEventArray
     */
    @Test
    public void testEvaluateIEventArray()
        throws StreamingException
    {
        //NONE
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaaa", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaaa", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaab", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aaaab", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //END
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%a", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%a", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //BEGIN
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aa%", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aa%", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "ab%", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "ab%", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //MID
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%aa%", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%aa%", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab%", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab%", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //Complex
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "_aa%", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "_aa%", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab_", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "%ab_", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a123"), "a[1-9][1-9][1-9]", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a123"), "a[1-9][1-9][1-9]", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a023"), "a[1-9][1-9][1-9]", true);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        likeExpr = new LikeExpression(new ConstExpression("a023"), "a[1-9][1-9][1-9]", false);
        
        actualResult = likeExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
    }
    
    /**
     * testGetType
     */
    @Test
    public void testGetType()
        throws StreamingException
    {
        likeExpr = new LikeExpression(new ConstExpression("aaaaa"), "aa", true);
        Assert.assertEquals(Boolean.class, likeExpr.getType());
    }
    
    /**
     * testUTF8String
     */
    @Test
    public void testUTF8String1()
        throws StreamingException
    {
        String s = "现在接电话方便吗？";
        likeExpr = new LikeExpression(new ConstExpression(s), "%圣战%", true);
        Assert.assertFalse((boolean)likeExpr.evaluate(theEvent));
        s = "我正在开车，稍后联系你。";
        likeExpr = new LikeExpression(new ConstExpression(s), "%圣战%", true);
        Assert.assertFalse((boolean)likeExpr.evaluate(theEvent));
        likeExpr = new LikeExpression(new ConstExpression(s), "%开车,%", true);
        Assert.assertFalse((boolean)likeExpr.evaluate(theEvent));
        likeExpr = new LikeExpression(new ConstExpression(s), "%开车，%", true);
        Assert.assertTrue((boolean)likeExpr.evaluate(theEvent));
    }
    
    /**
     * testUTF8String
     */
    @Test
    public void testUTF8String2()
        throws StreamingException
    {
        String s = "现在接电话方便吗？";
        likeExpr = new LikeExpression(new ConstExpression(s), "_圣战%", true);
        Assert.assertFalse((boolean)likeExpr.evaluate(theEvent));
        s = "我正在开车，稍后联系你。";
        likeExpr = new LikeExpression(new ConstExpression(s), "联系%", true);
        Assert.assertFalse((boolean)likeExpr.evaluate(theEvent));
        likeExpr = new LikeExpression(new ConstExpression(s), "_正在%", true);
        Assert.assertTrue((boolean)likeExpr.evaluate(theEvent));
        likeExpr = new LikeExpression(new ConstExpression(s), "我正在%", true);
        Assert.assertTrue((boolean)likeExpr.evaluate(theEvent));
    }
}

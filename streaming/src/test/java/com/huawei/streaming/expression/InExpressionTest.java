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
import com.huawei.streaming.support.SupportConst;

/**
 * <InExpressionTest>
 * <功能详细描述>
 * 
 */
public class InExpressionTest
{
    
    private static final double DOU_2 = 2.0;
    
    private IEvent theEvent = new TupleEvent();
    
    private IEvent[] theEvents = new IEvent[] {new TupleEvent()};
    
    private InExpression inExpr = null;
    
    private Object actualResult = null;
    
    /**
     * testInExpression
     * 
     */
    @Test
    public void testInExpression()
    {
        try
        {
            inExpr =
                new InExpression(null, new ConstExpression[] {new ConstExpression(SupportConst.I_TWO),
                    new ConstExpression(SupportConst.I_THREE)}, true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            inExpr = new InExpression(new ConstExpression(SupportConst.I_TWO), null, true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            inExpr =
                new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                    new ConstExpression(SupportConst.I_TWO), new ConstExpression("a")}, true);
            
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            inExpr =
                new InExpression(new ConstExpression("a"), new ConstExpression[] {
                    new ConstExpression(SupportConst.I_TWO), new ConstExpression("a")}, true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
    }
    
    /**
     * testEvaluateIEvent
     * 
     */
    @Test
    public void testEvaluateIEvent()
        throws StreamingException
    {
        
        //Number
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_FOUR), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_FOUR), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        // Number transfer type
        
        inExpr =
            new InExpression(new ConstExpression(DOU_2), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(DOU_2), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(1.0), new ConstExpression[] {new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(1.0), new ConstExpression[] {new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //String
        inExpr =
            new InExpression(new ConstExpression("a"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("a"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("b"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("b"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //NULL
        
        inExpr =
            new InExpression(new ConstExpression(null, Float.class), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, Float.class), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, String.class), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, String.class), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
    }
    
    /**
     * testEvaluateIEventArray
     */
    @Test
    public void testEvaluateIEventArray()
        throws StreamingException
    {
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_FOUR), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_FOUR), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //Number Transfer
        
        inExpr =
            new InExpression(new ConstExpression(DOU_2), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(DOU_2), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(1.0), new ConstExpression[] {new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        //String
        inExpr =
            new InExpression(new ConstExpression("a"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("a"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("b"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression("b"), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //NULL
        inExpr =
            new InExpression(new ConstExpression(null, Float.class), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, Float.class), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, String.class), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, true);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        inExpr =
            new InExpression(new ConstExpression(null, String.class), new ConstExpression[] {new ConstExpression("a"),
                new ConstExpression("ab")}, false);
        
        actualResult = inExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
    }
    
    /**
     * testGetType
     */
    @Test
    public void testGetType()
        throws StreamingException
    {
        inExpr =
            new InExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression[] {
                new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_THREE)}, true);
        
        Assert.assertEquals(Boolean.class, inExpr.getType());
    }
    
}

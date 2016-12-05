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
 * <BetweenExpressionTest>
 * <功能详细描述>
 * 
 */
public class BetweenExpressionTest
{
    
    private IEvent theEvent = new TupleEvent();
    
    private IEvent[] theEvents = new IEvent[] {new TupleEvent()};
    
    private BetweenExpression betweenExpr = null;
    
    private Object actualResult = null;
    
    /**
     * testBetweenExpression
     */
    @Test
    public void testBetweenExpression()
    {
        try
        {
            betweenExpr =
                new BetweenExpression(null, new ConstExpression(SupportConst.I_TWO), new ConstExpression(
                    SupportConst.I_FIVE), true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            betweenExpr =
                new BetweenExpression(new ConstExpression(null, Float.class), null, new ConstExpression(
                    SupportConst.I_FIVE), true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            betweenExpr =
                new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                    null, true);
            
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            betweenExpr =
                new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                    new ConstExpression("a"), true);
            Assert.fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            betweenExpr =
                new BetweenExpression(new ConstExpression("a"), new ConstExpression(SupportConst.I_TWO),
                    new ConstExpression("a"), true);
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
        
        testNumberIEvent();
        
        testStringIEvent();
        
        testNullIEvent();
        
    }
    
    private void testNullIEvent()
        throws StreamingException
    {
        // null
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, String.class), new ConstExpression("aa"),
                new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, String.class), new ConstExpression("aa"),
                new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(null, actualResult);
        
    }
    
    private void testStringIEvent()
        throws StreamingException
    {
        //String   
        //between
        betweenExpr =
            new BetweenExpression(new ConstExpression("a"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("aa"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("af"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("b"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("c"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        //not between
        betweenExpr =
            new BetweenExpression(new ConstExpression("a"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("aa"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("af"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("b"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("c"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
    }
    
    private void testNumberIEvent()
        throws StreamingException
    {
        //Number             
        // between
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        // not between
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        //Number Transfer
        
        //between
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        //not between
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvent);
        
        Assert.assertEquals(true, actualResult);
        
    }
    
    /**
     * testEvaluateIEventArray
     */
    @Test
    public void testEvaluateIEventArray()
        throws StreamingException
    {
        
        testNumberIEventArray();
        
        testStringIEventArray();
        
        testNullIEventArray();
        
    }
    
    private void testNullIEventArray()
        throws StreamingException
    {
        // null
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, Float.class), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, String.class), new ConstExpression("aa"),
                new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(null, String.class), new ConstExpression("aa"),
                new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(null, actualResult);
        
    }
    
    private void testStringIEventArray()
        throws StreamingException
    {
        //String
        
        //between
        betweenExpr =
            new BetweenExpression(new ConstExpression("a"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("aa"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("af"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("b"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("c"), new ConstExpression("aa"), new ConstExpression("b"), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        //not between
        betweenExpr =
            new BetweenExpression(new ConstExpression("a"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("aa"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("af"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("b"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression("c"), new ConstExpression("aa"), new ConstExpression("b"), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
    }
    
    private void testNumberIEventArray()
        throws StreamingException
    {
        //Number             
        // between
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        // not between
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_ONE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.I_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        //Number Transfer
        
        //between
        betweenExpr =
            new BetweenExpression(new ConstExpression(1.0), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), true);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        //not between
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(1.0), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_TWO), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_THREE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_FIVE), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(false, actualResult);
        
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        actualResult = betweenExpr.evaluate(theEvents);
        
        Assert.assertEquals(true, actualResult);
        
    }
    
    /**
     * testGetType
     */
    @Test
    public void testGetType()
        throws StreamingException
    {
        betweenExpr =
            new BetweenExpression(new ConstExpression(SupportConst.D_SIX), new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_FIVE), false);
        
        Assert.assertEquals(Boolean.class, betweenExpr.getType());
    }
    
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.support.SupportConst;

/**
 * <CaseSearchedExpressionTest>
 * <功能详细描述>
 *
 */
public class CaseSearchedExpressionTest
{
    private CaseSearchedExpression expr = null;
    
    private IEvent theEvent = new TupleEvent();
    
    private IEvent[] theEvents = new IEvent[] {new TupleEvent()};
    
    private Object actualResult = null;
    
    private List<Pair<IExpression, IExpression>> whenThenExprs = new ArrayList<Pair<IExpression, IExpression>>();
    
    /**
     * Test CaseSearchedExpression(List, IExpression).
     */
    @Test
    public void testCaseSearchedExpression()
    {
        try
        {
            expr = new CaseSearchedExpression(null, new ConstExpression(SupportConst.I_ONE));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.I_ONE));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(null, new ConstExpression(SupportConst.I_ONE)));
            expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.I_ONE));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), null));
            expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.I_ONE));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.I_ONE)));
            expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.I_ONE));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
    }
    
    /**
     * Test CaseSearchedExpression(List, IExpression).
     */
    @Test
    public void testCaseSearchExpressionForResult()
    {
        //value boolean
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(true)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                "false")));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(true)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(false)));
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Boolean.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        //value String
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression("true")));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(false)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression("true")));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                "false")));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(String.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        //value number
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(true)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                SupportConst.L_TWO)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Long.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                SupportConst.F_TWO)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Float.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                SupportConst.D_TWO)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Double.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.L_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                SupportConst.D_TWO)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Double.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.F_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
                SupportConst.D_TWO)));
            
            expr = new CaseSearchedExpression(whenThenExprs, null);
            Assert.assertEquals(Double.class, expr.getType());
            Assert.assertTrue(true);
        }
        catch (StreamingException e)
        {
            fail();
        }
        
    }
    
    /**
     * Test evaluate(IEvent).
     */
    @Test
    public void testEvaluateIEvent()
        throws StreamingException
    {
        //boolean 
        whenThenExprs.clear();
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(true)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(false)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Boolean.class, expr.getType());
        assertEquals(true, actualResult);
        
        //string
        whenThenExprs.clear();
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression("true")));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression("false")));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(String.class, expr.getType());
        assertEquals("true", actualResult);
        
        //number
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.L_TWO)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.D_THREE)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.L_TWO)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Long.class, expr.getType());
        assertEquals(null, actualResult);
        
        expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.D_THREE));
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_THREE, actualResult);
        
    }
    
    /**
     * Test evaluate(IEvent[]).
     */
    @Test
    public void testEvaluateIEventArray()
        throws StreamingException
    {
        //boolean
        whenThenExprs.clear();
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(true)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(false)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Boolean.class, expr.getType());
        assertEquals(true, actualResult);
        
        //string
        whenThenExprs.clear();
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression("true")));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression("false")));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(String.class, expr.getType());
        assertEquals("true", actualResult);
        
        //number
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.L_TWO)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.D_THREE)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.L_TWO)));
        
        expr = new CaseSearchedExpression(whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Long.class, expr.getType());
        assertEquals(null, actualResult);
        
        expr = new CaseSearchedExpression(whenThenExprs, new ConstExpression(SupportConst.D_THREE));
        actualResult = expr.evaluate(theEvents);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_THREE, actualResult);
    }
    
}

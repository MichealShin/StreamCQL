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
 * <CaseSimpleExpressionTest>
 * <CASE input_expression 
 *  WHEN when_expression THEN result_expression 
 *  [ ...n ] 
 *  [ ELSE else_result_expression ] 
 *  END >
 * 
 */
public class CaseSimpleExpressionTest
{
    private CaseSimpleExpression expr = null;
    
    private IEvent theEvent = new TupleEvent();
    
    private IEvent[] theEvents = new IEvent[] {new TupleEvent()};
    
    private Object actualResult = null;
    
    private List<Pair<IExpression, IExpression>> whenThenExprs = new ArrayList<Pair<IExpression, IExpression>>();
    
    /**
     * Test CaseSimpleExpression(IExpression, List, IExpression).
     */
    @Test
    public void testCaseSimpleExpression()
    {
        try
        {
            expr =
                new CaseSimpleExpression(null, new ArrayList<Pair<IExpression, IExpression>>(), new ConstExpression(1));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), null, new ConstExpression(1));
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE), null));
            
            expr =
                new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, new ConstExpression(
                    SupportConst.I_ONE));
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
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.assertTrue(false);
        }
    }
    
    /**
     * Test CaseSimpleExpression(IExpression, List, IExpression).
     */
    @Test
    public void testCaseSimpleExpressionForCompare()
    {
        //compare boolean
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("true"), new ConstExpression(
                SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(true), whenThenExprs, null);
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
                SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(true), whenThenExprs, null);
            Assert.assertTrue(true);
            
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        //compare String
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("aaa"), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression("aaa"), whenThenExprs, null);
            fail();
        }
        catch (StreamingException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("aaa"), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(null, String.class),
                new ConstExpression(SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression("aaa"), whenThenExprs, null);
            Assert.assertTrue(true);
            
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("aaa"), new ConstExpression(
                SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("bbb"), new ConstExpression(
                SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression("aaa"), whenThenExprs, null);
            Assert.assertTrue(true);
            
        }
        catch (StreamingException e)
        {
            fail();
        }
        
        //compare number
        
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("bbb"), new ConstExpression(
                SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
            
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
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.L_TWO),
                new ConstExpression(SupportConst.I_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.D_ONE), whenThenExprs, null);
            Assert.assertTrue(true);
            
        }
        catch (StreamingException e)
        {
            fail();
        }
    }
    
    /**
     * Test CaseSimpleExpression(IExpression, List, IExpression).
     */
    @Test
    public void testCaseSimpleExpressionForResult()
    {
        //value boolean
        try
        {
            whenThenExprs.clear();
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(true)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression("false")));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
                new ConstExpression(true)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(false)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression("true")));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(false)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
                new ConstExpression("true")));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression("false")));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(true)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.L_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.F_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.I_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.D_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.L_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.D_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
            
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
                new ConstExpression(SupportConst.F_ONE)));
            whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
                new ConstExpression(SupportConst.D_TWO)));
            
            expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
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
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.I_TWO)));
        
        expr = new CaseSimpleExpression(new ConstExpression(true), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_ONE, actualResult);
        
        //string
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("aaaa"), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("bbbb"), new ConstExpression(
            SupportConst.I_TWO)));
        
        expr = new CaseSimpleExpression(new ConstExpression("aaaa"), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_ONE, actualResult);
        
        expr = new CaseSimpleExpression(new ConstExpression("cccc"), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Integer.class, expr.getType());
        assertEquals(null, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression("cccc"), whenThenExprs, new ConstExpression(
                SupportConst.I_THREE));
        actualResult = expr.evaluate(theEvent);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_THREE, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression(null), whenThenExprs,
                new ConstExpression(SupportConst.I_THREE));
        actualResult = expr.evaluate(theEvent);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_THREE, actualResult);
        
        //number
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
            new ConstExpression(SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.D_ONE),
            new ConstExpression(SupportConst.L_TWO)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
            new ConstExpression(SupportConst.D_THREE)));
        
        expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        expr = new CaseSimpleExpression(new ConstExpression(SupportConst.D_ONE), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression(SupportConst.D_THREE), whenThenExprs, new ConstExpression(
                SupportConst.I_FOUR));
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_FOUR, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression(null), whenThenExprs, new ConstExpression(SupportConst.I_FOUR));
        actualResult = expr.evaluate(theEvent);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_FOUR, actualResult);
        
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
            new ConstExpression(SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(null), new ConstExpression(
            SupportConst.L_TWO)));
        
        expr = new CaseSimpleExpression(new ConstExpression(null), whenThenExprs, null);
        actualResult = expr.evaluate(theEvent);
        assertEquals(Long.class, expr.getType());
        assertEquals(SupportConst.L_TWO, actualResult);
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
        
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(true), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(false), new ConstExpression(
            SupportConst.I_TWO)));
        
        expr = new CaseSimpleExpression(new ConstExpression(true), whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_ONE, actualResult);
        
        //string
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("aaaa"), new ConstExpression(
            SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression("bbbb"), new ConstExpression(
            SupportConst.I_TWO)));
        
        expr = new CaseSimpleExpression(new ConstExpression("aaaa"), whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_ONE, actualResult);
        
        expr = new CaseSimpleExpression(new ConstExpression("cccc"), whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Integer.class, expr.getType());
        assertEquals(null, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression("cccc"), whenThenExprs, new ConstExpression(
                SupportConst.I_THREE));
        actualResult = expr.evaluate(theEvents);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_THREE, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression(null), whenThenExprs,
                new ConstExpression(SupportConst.I_THREE));
        actualResult = expr.evaluate(theEvents);
        assertEquals(Integer.class, expr.getType());
        assertEquals(SupportConst.I_THREE, actualResult);
        
        //number
        whenThenExprs.clear();
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_ONE),
            new ConstExpression(SupportConst.I_ONE)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.D_ONE),
            new ConstExpression(SupportConst.L_TWO)));
        whenThenExprs.add(new Pair<IExpression, IExpression>(new ConstExpression(SupportConst.I_TWO),
            new ConstExpression(SupportConst.D_THREE)));
        
        expr = new CaseSimpleExpression(new ConstExpression(SupportConst.I_ONE), whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        expr = new CaseSimpleExpression(new ConstExpression(SupportConst.D_ONE), whenThenExprs, null);
        actualResult = expr.evaluate(theEvents);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_ONE, actualResult);
        
        expr =
            new CaseSimpleExpression(new ConstExpression(SupportConst.D_THREE), whenThenExprs, new ConstExpression(
                SupportConst.I_FOUR));
        actualResult = expr.evaluate(theEvents);
        assertEquals(Double.class, expr.getType());
        assertEquals(SupportConst.D_FOUR, actualResult);
    }
    
}

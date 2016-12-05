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

package com.huawei.streaming.window;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.support.SupportConst;
import com.huawei.streaming.support.SupportEventMng;
import com.huawei.streaming.support.SupportView;
import com.huawei.streaming.support.SupportViewDataCheck;
import com.huawei.streaming.view.IView;

/**
 * <NaturalDaySlideWindowTest>
 * <功能详细描述>
 * 
 */
public class NaturalDaySlideWindowTest
{
    private static final long TIME1 = 1379921851027L;
    
    private static final long TIME2 = 1379921851028L;
    
    private static final long TIME3 = 1379921851029L;
    
    private static final long TIME4 = 1379952000000L;
    
    private NaturalDaySlideWindow myView = null;
    
    private SupportView childView = null;
    
    private SupportEventMng mng = null;
    
    private IEventType eventType = null;
    
    /**
     * <setup>
     */
    @Before
    public void setUp()
        throws Exception
    {
        childView = new SupportView();
        mng = new SupportEventMng();
        eventType = mng.getTimeEventType();
    }
    
    /**
     * <cleanup>
     */
    @After
    public void tearDown()
        throws Exception
    {
        mng = null;
        eventType = null;
        myView = null;
        childView = null;
    }
    
    /**
     * Test NaturalDaySlideWindow(IExpression).
     */
    @Test
    public void testNaturalDaySlideWindow()
    {
        try
        {
            myView = new NaturalDaySlideWindow(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
    }
    
    /**
     * Test update(IEvent[], IEvent[]).
     */
    @Test
    public void testUpdate()
    {
        myView = new NaturalDaySlideWindow(new PropertyValueExpression("timestamp", Long.class));
        myView.addView(childView);
        
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, null);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c1");
        values.put("timestamp", TIME1);
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1});
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", TIME2);
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", TIME3);
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", TIME4);
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event2, event3}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event2, event3});
        
        myView.update(new IEvent[] {event4}, null);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event1, event2, event3});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event4});
    }
    
    /**
     * Test renewView().
     */
    @Test
    public void testRenewView()
    {
        IExpression timeExpr = new PropertyValueExpression("timestamp", Long.class);
        myView = new NaturalDaySlideWindow(timeExpr);
        
        IView renewView = myView.renewView();
        Assert.assertEquals(true, renewView instanceof NaturalDaySlideWindow);
        Assert.assertEquals(timeExpr, ((NaturalDaySlideWindow)renewView).getTimeExpr());
    }
    
}

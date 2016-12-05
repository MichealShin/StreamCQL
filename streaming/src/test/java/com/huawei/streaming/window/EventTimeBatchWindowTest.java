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

import java.sql.Date;
import java.sql.Timestamp;
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
 * <EventTimeBatchWindowTest>
 * <功能详细描述>
 * 
 */
public class EventTimeBatchWindowTest
{
    private EventTimeBatchWindow myView = null;
    
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
     * Test EventTimeBatchWindow(long, IExpression).
     */
    @Test
    public void testEventTimeBatchWindow()
    {
        try
        {
            myView = new EventTimeBatchWindow(0, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            myView = new EventTimeBatchWindow(-1, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            myView = new EventTimeBatchWindow(1, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Boolean.class));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", String.class));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
        
        try
        {
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Long.class));
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Integer.class));
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Double.class));
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Float.class));
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Date.class));
            myView = new EventTimeBatchWindow(1, new PropertyValueExpression("timestamp", Timestamp.class));
            Assert.assertTrue(true);
        }
        catch (IllegalArgumentException e)
        { 
            fail();
        }
    }
    
    /**
     * Test update(IEvent[], IEvent[]).
     * 时间戳字段数值类型
     */
    @Test
    public void testUpdate()
    {
        myView = new EventTimeBatchWindow(SupportConst.I_FIVE, new PropertyValueExpression("timestamp", Long.class));
        myView.addView(childView);
        
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, null);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c1");
        values.put("timestamp", SupportConst.L_ONE);
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", SupportConst.L_TWO);
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", SupportConst.L_THREE);
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", SupportConst.L_FOUR);
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        values.put("timestamp", SupportConst.L_FIVE);
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        values.put("timestamp", SupportConst.L_SIX);
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event2, event3, event4, event5, event6}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4, event5});
    }
    
    /**
     * Test update(IEvent[], IEvent[]).
     * 时间戳字段日期类型
     */
    @Test
    public void testUpdate2()
    {
        myView = new EventTimeBatchWindow(SupportConst.I_FIVE, new PropertyValueExpression("timestamp", Timestamp.class));
        myView.addView(childView);
        
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, null);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c1");
        values.put("timestamp",new Timestamp(SupportConst.L_ONE));
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", new Timestamp(SupportConst.L_TWO));
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", new Timestamp(SupportConst.L_THREE));
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", new Timestamp(SupportConst.L_FOUR));
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        values.put("timestamp", new Timestamp(SupportConst.L_FIVE));
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        values.put("timestamp", new Timestamp(SupportConst.L_SIX));
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event2, event3, event4, event5, event6}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4, event5});
    }
    
    /**
     * Test renewView().
     */
    @Test
    public void testRenewView()
    {
        IExpression timeExpr = new PropertyValueExpression("timestamp", Long.class);
        myView = new EventTimeBatchWindow(SupportConst.I_FIVE, timeExpr);
        
        IView renewView = myView.renewView();
        Assert.assertEquals(true, renewView instanceof EventTimeBatchWindow);
        Assert.assertEquals(SupportConst.I_FIVE, ((EventTimeBatchWindow)renewView).getKeepTime());
        Assert.assertEquals(timeExpr, ((EventTimeBatchWindow)renewView).getTimeExpr());
    }
    
}

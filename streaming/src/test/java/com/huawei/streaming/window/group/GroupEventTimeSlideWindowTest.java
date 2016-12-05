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

package com.huawei.streaming.window.group;

import static org.junit.Assert.assertTrue;
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
import com.huawei.streaming.support.SupportGroupView;
import com.huawei.streaming.support.SupportGroupViewDataCheck;
import com.huawei.streaming.view.MergeView;

/**
 * <GroupEventTimeSlideWindowTest>
 * <功能详细描述>
 * 
 */
public class GroupEventTimeSlideWindowTest
{
    private IExpression[] exprs = null;
    
    private GroupEventTimeSlideWindow myView = null;
    
    private MergeView mergeView = null;
    
    private SupportGroupView childView = null;
    
    private SupportEventMng mng = null;
    
    private IEventType eventType = null;
    
    /**
     * <setup>
     */
    @Before
    public void setUp()
        throws Exception
    {
        exprs = new IExpression[] {new PropertyValueExpression("a", Integer.class)};
        mergeView = new MergeView();
        childView = new SupportGroupView(exprs);
        
        mergeView.addView(childView);
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
        exprs = null;
        myView = null;
        mergeView.removeAllViews();
        mergeView = null;
        childView = null;
        mng = null;
    }
    
    /**
     * Test GroupEventTimeSlideWindow(IExpression[], long, IExpression).
     */
    @Test
    public void testGroupEventTimeSlideWindow()
    {
        try
        {
            myView = new GroupEventTimeSlideWindow(null, SupportConst.I_FIVE, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new GroupEventTimeSlideWindow(new IExpression[] {}, SupportConst.I_FIVE, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView =
                new GroupEventTimeSlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                    SupportConst.I_ZERO, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView =
                new GroupEventTimeSlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                    SupportConst.I_FIVE, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
    }
    
    /**
     * Test processGroupedEvent(Object, Object, IEvent).
     */
    @Test
    public void testProcessGroupedEvent()
    {
        myView =
            new GroupEventTimeSlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                SupportConst.I_FIVE, new PropertyValueExpression("timestamp", Long.class));
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c11");
        values.put("timestamp", SupportConst.L_ONE);
        
        IEvent event11 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event11);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event11});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c21");
        values.put("timestamp", SupportConst.L_ONE);
        
        IEvent event21 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_TWO, event21);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c12");
        values.put("timestamp", SupportConst.L_TWO);
        
        IEvent event12 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event12);
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c18");
        values.put("timestamp", SupportConst.L_EIGHT);
        
        IEvent event18 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event18);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, new IEvent[] {event11, event12});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event18});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c26");
        values.put("timestamp", SupportConst.L_SIX);
        
        IEvent event26 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_TWO, event26);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event26});
    }
    
    /**
     * Test getTimestamp(IEvent).
     */
    @Test
    public void testGetTimestamp()
    {
        myView =
            new GroupEventTimeSlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                SupportConst.I_FIVE, new PropertyValueExpression("timestamp", Long.class));
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c1");
        values.put("timestamp", SupportConst.L_ONE);
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        Assert.assertEquals(SupportConst.L_ONE, myView.getTimestamp(event1).longValue());
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c1");
        values.put("timestamp", SupportConst.I_ONE);
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        Assert.assertEquals(SupportConst.L_ONE, myView.getTimestamp(event2).longValue());
    }
    
    /**
     * Test update(IEvent[], IEvent[]).
     */
    @Test
    public void testUpdate()
    {
        myView =
            new GroupEventTimeSlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                SupportConst.I_FIVE, new PropertyValueExpression("timestamp", Long.class));
        myView.addView(mergeView);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c11");
        values.put("timestamp", SupportConst.L_ONE);
        
        IEvent event11 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event11}, null);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event11});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c21");
        values.put("timestamp", SupportConst.L_ONE);
        
        IEvent event21 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event21}, null);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c12");
        values.put("timestamp", SupportConst.L_TWO);
        
        IEvent event12 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event12}, null);
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c18");
        values.put("timestamp", SupportConst.L_EIGHT);
        
        IEvent event18 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event18}, null);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, new IEvent[] {event11, event12});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event18});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c26");
        values.put("timestamp", SupportConst.L_SIX);
        
        IEvent event26 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event26}, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event26});
        
        try
        {
            myView.update(new IEvent[] {event11, event12}, null);
            fail();
        }
        catch (RuntimeException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView.update(new IEvent[] {event18}, new IEvent[] {event11});
            fail();
        }
        catch (RuntimeException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView.update(null, new IEvent[] {event11, event12});
            fail();
        }
        catch (RuntimeException e)
        {
            assertTrue(true);
        }
    }
    
}

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
 * <GroupNaturalDaySlideWindowTest>
 * <功能详细描述>
 * 
 */
public class GroupNaturalDaySlideWindowTest
{
    private static final long TIME1 = 1379921851027L;
    
    private static final long TIME2 = 1379921851028L;
    
    private static final long TIME4 = 1379952000000L;
    
    private IExpression[] exprs = null;
    
    private GroupNaturalDaySlideWindow myView = null;
    
    private MergeView mergeView = null;
    
    private SupportGroupView childView = null;
    
    private SupportEventMng mng = null;
    
    private IEventType eventType = null;
    
    /** <setUp>
     * <功能详细描述>
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
    
    /** <clearDown>
     * <功能详细描述>
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
     * Test processGroupedEvent(Object, IDataCollection, Object, IEvent).
     */
    @Test
    public void testProcessGroupedEvent()
    {
        myView =
            new GroupNaturalDaySlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                new PropertyValueExpression("timestamp", Long.class));
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c11");
        values.put("timestamp", TIME1);
        
        IEvent event11 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event11);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event11});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c21");
        values.put("timestamp", TIME1);
        
        IEvent event21 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_TWO, event21);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c12");
        values.put("timestamp", TIME2);
        
        IEvent event12 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event12);
        
        values.put("a", SupportConst.I_ONE);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c18");
        values.put("timestamp", TIME4);
        
        IEvent event18 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_ONE, event18);
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, new IEvent[] {event11, event12});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, new IEvent[] {event18});
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, null);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_ONE);
        values.put("c", "c26");
        values.put("timestamp", TIME4);
        
        IEvent event26 = new TupleEvent("stream", eventType, values);
        
        myView.processGroupedEvent(mergeView, null, SupportConst.I_TWO, event26);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_ONE, childView, null);
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_ONE, childView, null);
        
        SupportGroupViewDataCheck.checkOldData(SupportConst.I_TWO, childView, new IEvent[] {event21});
        SupportGroupViewDataCheck.checkNewData(SupportConst.I_TWO, childView, new IEvent[] {event26});
    }
    
    /**
     * Test GroupNaturalDaySlideWindow(IExpression[],IExpression).
     */
    @Test
    public void testGroupNaturalDaySlideWindow()
    {
        try
        {
            myView = new GroupNaturalDaySlideWindow(null, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new GroupNaturalDaySlideWindow(new IExpression[] {}, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView =
                new GroupNaturalDaySlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                    null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView =
                new GroupNaturalDaySlideWindow(new IExpression[] {new PropertyValueExpression("a", Integer.class)},
                    null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
    }
    
}

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

package com.huawei.streaming.window.sort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.process.sort.SortEnum;
import com.huawei.streaming.support.SupportConst;
import com.huawei.streaming.support.SupportEventMng;
import com.huawei.streaming.support.SupportView;
import com.huawei.streaming.support.SupportViewDataCheck;

/**
 * 
 * <LengthSortWindow测试类>
 * <功能详细描述>
 * 
 */
public class LengthSortWindowTest
{
    
    private LengthSortWindow myView;
    
    private SupportView childView;
    
    private SupportEventMng mng;
    
    private IEventType eventType;
    
    /**
     * <setup>
     * <功能详细描述>
     */
    @Before
    public void setUp()
        throws Exception
    {
        childView = new SupportView();
        mng = new SupportEventMng();
        eventType = mng.getInput();
    }
    
    /**
     * <cleanup>
     * <功能详细描述>
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
     * <测试构造函数>
     * <功能详细描述>
     */
    @Test
    public void testLengthSortWindow()
    {
        List<SortCondition> slist = new ArrayList<SortCondition>();
        slist.add(new SortCondition("a", SortEnum.ASC));
        
        try
        {
            myView = new LengthSortWindow(0, slist);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new LengthSortWindow(SupportConst.I_FIVE, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            slist.clear();
            myView = new LengthSortWindow(SupportConst.I_FIVE, slist);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
    }
    
    /**
     * <测试窗口更新>
     * <功能详细描述>
     */
    @Test
    public void testUpdate()
    {
        List<SortCondition> slist = new ArrayList<SortCondition>();
        slist.add(new SortCondition("a", SortEnum.ASC));
        
        myView = new LengthSortWindow(SupportConst.I_FIVE, slist);
        myView.addView(childView);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SEVEN);
        values.put("b", SupportConst.I_SEVEN);
        values.put("c", "c7");
        
        IEvent event7 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_EIGHT);
        values.put("b", SupportConst.I_EIGHT);
        values.put("c", "c8");
        
        IEvent event8 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_NINE);
        values.put("b", SupportConst.I_NINE);
        values.put("c", "c9");
        
        IEvent event9 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_TEN);
        values.put("b", SupportConst.I_TEN);
        values.put("c", "10");
        
        IEvent event10 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_ELEVEN);
        values.put("b", SupportConst.I_ELEVEN);
        values.put("c", "11");
        
        IEvent event11 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1, event2, event3, event4, event5}, null);
        
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4, event5});
        
        myView.update(new IEvent[] {event6, event7}, null);
        
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event7, event6});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event6, event7});
        
        myView.update(new IEvent[] {event8, event9, event10, event11}, new IEvent[] {event4, event5});
        
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event4, event5, event11, event10});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event8, event9, event10, event11});
        
    }
    
    /**
     * <测试返回窗口长度>
     * <功能详细描述>
     */
    @Test
    public void testGetKeepLength()
    {
        List<SortCondition> slist = new ArrayList<SortCondition>();
        slist.add(new SortCondition("a", SortEnum.ASC));
        
        myView = new LengthSortWindow(SupportConst.I_FIVE, slist);
        
        assertEquals(SupportConst.I_FIVE, myView.getKeepLength());
    }
    
}

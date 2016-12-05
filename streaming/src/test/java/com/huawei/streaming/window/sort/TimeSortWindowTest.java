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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
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
import com.huawei.streaming.support.SupportView;
import com.huawei.streaming.support.SupportViewDataCheck;

/**
 * <TimeSortWindowTest>
 * <功能详细描述>
 * 
 */
public class TimeSortWindowTest
{
    private static final long NUM_30 = 30L;
    
    private static final long NUM_50 = 50L;
    
    private static final long NUM_100 = 100L;
    
    private static final long NUM_200 = 200L;
    
    private static final long NUM_560 = 560L;
    
    private static final long NUM_600 = 600L;
    
    private static final long NUM_1000 = 1000L;
    
    private IExpression expr;
    
    private TimeSortWindow myView;
    
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
        expr = new PropertyValueExpression("timestamp", Long.class);
        myView = new TimeSortWindow(SupportConst.BATCH_TIME, expr);
        childView = new SupportView();
        mng = new SupportEventMng();
        eventType = mng.getTimeEventType();
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
     * 测试构造函数
     */
    @Test
    public void testTimeSortWindow()
    {
        try
        {
            myView = new TimeSortWindow(0, expr);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new TimeSortWindow(SupportConst.BATCH_TIME, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new TimeSortWindow(0, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
    }
    
    /**
     * 测试窗口更新
     * 时间戳字段数值类型
     */
    @Test
    public void testUpdate()
    {
        myView.addView(childView);
        myView.initLock();
        
        long currentTime = System.currentTimeMillis();
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        values.put("timestamp", currentTime);
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", currentTime + NUM_100);
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", currentTime + NUM_200);
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", currentTime + NUM_50);
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1, event2, event3, event4}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4});
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        values.put("timestamp", currentTime + NUM_30);
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        values.put("timestamp", currentTime - NUM_600);
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SEVEN);
        values.put("b", SupportConst.I_SEVEN);
        values.put("c", "c7");
        values.put("timestamp", currentTime + NUM_200);
        
        IEvent event7 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event5, event6, event7}, null);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event6});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event5, event6, event7});
        
        myView.update(null, new IEvent[] {event2});
        
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event2});
        SupportViewDataCheck.checkNewData(childView, null);
        
        myView.timerCallBack(currentTime + NUM_1000);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event1, event5, event4, event7, event3});
        SupportViewDataCheck.checkNewData(childView, null);
    }
    
    /**
     * 测试窗口更新
     * 时间戳字段日期类型
     */
    @Test
    public void testUpdate2()
    {
        expr = new PropertyValueExpression("timestamp", Timestamp.class);
        myView = new TimeSortWindow(SupportConst.BATCH_TIME, expr);
        
        myView.addView(childView);
        myView.initLock();
        
        long currentTime = System.currentTimeMillis();
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        values.put("timestamp", new Timestamp(currentTime));
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", new Timestamp(currentTime + NUM_100));
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", new Timestamp(currentTime + NUM_200));
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", new Timestamp(currentTime + NUM_50));
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1, event2, event3, event4}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4});
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        values.put("timestamp", new Timestamp(currentTime + NUM_30));
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        values.put("timestamp", new Timestamp(currentTime - NUM_600));
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SEVEN);
        values.put("b", SupportConst.I_SEVEN);
        values.put("c", "c7");
        values.put("timestamp", new Timestamp(currentTime + NUM_200));
        
        IEvent event7 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event5, event6, event7}, null);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event6});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event5, event6, event7});
        
        myView.update(null, new IEvent[] {event2});
        
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event2});
        SupportViewDataCheck.checkNewData(childView, null);
        
        myView.timerCallBack(currentTime + NUM_1000);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event1, event5, event4, event7, event3});
        SupportViewDataCheck.checkNewData(childView, null);
    }
    
    /**
     * 测试时间回调
     */
    @Test
    public void testTimerCallBack()
    {
        myView.addView(childView);
        myView.initLock();
        
        long currentTime = System.currentTimeMillis();
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        values.put("timestamp", currentTime);
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_TWO);
        values.put("b", SupportConst.I_TWO);
        values.put("c", "c2");
        values.put("timestamp", currentTime + NUM_100);
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_THREE);
        values.put("b", SupportConst.I_THREE);
        values.put("c", "c3");
        values.put("timestamp", currentTime + NUM_200);
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_FOUR);
        values.put("b", SupportConst.I_FOUR);
        values.put("c", "c4");
        values.put("timestamp", currentTime + NUM_50);
        
        IEvent event4 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event1, event2, event3, event4}, null);
        SupportViewDataCheck.checkOldData(childView, null);
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event1, event2, event3, event4});
        
        myView.timerCallBack(currentTime + NUM_560);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event1, event4});
        SupportViewDataCheck.checkNewData(childView, null);
        
        values.put("a", SupportConst.I_FIVE);
        values.put("b", SupportConst.I_FIVE);
        values.put("c", "c5");
        values.put("timestamp", currentTime + NUM_200);
        
        IEvent event5 = new TupleEvent("stream", eventType, values);
        
        values.put("a", SupportConst.I_SIX);
        values.put("b", SupportConst.I_SIX);
        values.put("c", "c6");
        values.put("timestamp", currentTime - NUM_600);
        
        IEvent event6 = new TupleEvent("stream", eventType, values);
        
        myView.update(new IEvent[] {event5, event6}, null);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event6});
        SupportViewDataCheck.checkNewData(childView, new IEvent[] {event5, event6});
        
        myView.timerCallBack(currentTime + NUM_1000);
        SupportViewDataCheck.checkOldData(childView, new IEvent[] {event2, event5, event3});
        SupportViewDataCheck.checkNewData(childView, null);
    }
    
}

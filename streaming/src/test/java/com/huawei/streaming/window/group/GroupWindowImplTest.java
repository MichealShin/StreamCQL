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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.support.SupportEventMng;
import com.huawei.streaming.support.SupportGroupView;
import com.huawei.streaming.support.SupportGroupViewDataCheck;
import com.huawei.streaming.support.SupportGroupWindowImpl;
import com.huawei.streaming.support.SupportMultiInstanceView;
import com.huawei.streaming.support.SupportView;
import com.huawei.streaming.support.SupportViewDataCheck;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.MergeView;

/**
 * 
 * <GroupWindowImpl测试类>
 * 
 */
public class GroupWindowImplTest
{
    private static final int TWO = 2;
    
    /**
     * 分组表达式
     */
    private IExpression[] exprs = null;
    
    private GroupWindowImpl myView = null;
    
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
        myView = new SupportGroupWindowImpl(exprs);
        mergeView = new MergeView();
        childView = new SupportGroupView(exprs);
        myView.addView(mergeView);
        mergeView.addView(childView);
        mng = new SupportEventMng();
        eventType = mng.getInput();
    }
    
    /**
     * <cleanup>
     */
    @After
    public void tearDown()
        throws Exception
    {
        exprs = null;
        
        myView.removeAllViews();
        mergeView.removeAllViews();
        myView = null;
        mergeView = null;
        childView = null;
        
        mng = null;
    }
    
    /**
     * <测试构造函数， 构造函数参数为NULL；构造函数参数为空数据>
     */
    @Test
    public void testGroupWindowImpl()
    {
        try
        {
            myView = new SupportGroupWindowImpl(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
        
        try
        {
            myView = new SupportGroupWindowImpl(new IExpression[] {});
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(true);
        }
    }
    
    /**
     * <测试创建子视图实例>
     */
    @Test
    public void testMakeSubViews()
    {
        try
        {
            myView.removeAllViews();
            
            myView.makeSubViews(myView, 1);
            fail();
        }
        catch (RuntimeException ex)
        {
            assertTrue(true);
        }
        
        try
        {
            myView.removeAllViews();
            
            SupportView view1 = new SupportView();
            SupportView view2 = new SupportView();
            myView.addView(view1);
            myView.addView(view2);
            
            myView.makeSubViews(myView, 1);
            fail();
        }
        catch (RuntimeException ex)
        {
            assertTrue(true);
        }
        
    }
    
    /**
     * <测试返回分组值>
     */
    @Test
    public void testGetGroupKey()
    {
        exprs = new IExpression[] {new PropertyValueExpression("a", Integer.class)};
        myView = new SupportGroupWindowImpl(exprs);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        assertEquals(1, myView.getGroupKey(event1));
        
        values = new HashMap<String, Object>();
        values.put("a", TWO);
        values.put("b", TWO);
        values.put("c", "c2");
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        assertEquals(TWO, myView.getGroupKey(event2));
        
        exprs =
            new IExpression[] {new PropertyValueExpression("a", Integer.class),
                new PropertyValueExpression("b", Integer.class)};
        myView = new SupportGroupWindowImpl(exprs);
        
        assertEquals(new MultiKey(new Object[] {1, 1}), myView.getGroupKey(event1));
        
        assertEquals(new MultiKey(new Object[] {TWO, TWO}), myView.getGroupKey(event2));
    }
    
    /**
     * <测试窗口更新操作>
     * <功能详细描述>
     */
    @Test
    public void testUpdate()
    {
        SupportMultiInstanceView mView = new SupportMultiInstanceView();
        FirstLevelStream stream = new FirstLevelStream();
        stream.addView(myView);
        
        myView.removeAllViews();
        
        myView.addView(mView);
        mView.addView(mergeView);
        mergeView.addView(childView);
        
        SupportMultiInstanceView.getInstances().clear();
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        stream.add(event1);
        
        assertEquals(1, SupportMultiInstanceView.getInstances().size());
        SupportMultiInstanceView child1 = SupportMultiInstanceView.getInstances().get(0);
        SupportViewDataCheck.checkOldData(child1, null);
        SupportViewDataCheck.checkNewData(child1, new IEvent[] {event1});
        SupportGroupViewDataCheck.checkOldData(1, childView, null);
        SupportGroupViewDataCheck.checkNewData(1, childView, new IEvent[] {event1});
        
        values = new HashMap<String, Object>();
        values.put("a", TWO);
        values.put("b", TWO);
        values.put("c", "c2");
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        stream.add(event2);
        
        assertEquals(TWO, SupportMultiInstanceView.getInstances().size());
        SupportMultiInstanceView child2 = SupportMultiInstanceView.getInstances().get(1);
        SupportViewDataCheck.checkOldData(child2, null);
        SupportViewDataCheck.checkNewData(child2, new IEvent[] {event2});
        SupportGroupViewDataCheck.checkOldData(TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(TWO, childView, new IEvent[] {event2});
        
        values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c2");
        
        IEvent event3 = new TupleEvent("stream", eventType, values);
        
        stream.add(event3);
        
        assertEquals(TWO, SupportMultiInstanceView.getInstances().size());
        SupportMultiInstanceView child3 = SupportMultiInstanceView.getInstances().get(0);
        SupportViewDataCheck.checkOldData(child3, null);
        SupportViewDataCheck.checkNewData(child3, new IEvent[] {event3});
        SupportGroupViewDataCheck.checkOldData(TWO, childView, null);
        SupportGroupViewDataCheck.checkNewData(TWO, childView, null);
    }
    
    /**
     * <测试根据分组值返回子视图实例>
     * <功能详细描述>
     */
    @Test
    public void testGetSubViewsPerKey()
    {
        SupportMultiInstanceView mView = new SupportMultiInstanceView();
        FirstLevelStream stream = new FirstLevelStream();
        stream.addView(myView);
        
        myView.removeAllViews();
        
        myView.addView(mView);
        mView.addView(mergeView);
        mergeView.addView(childView);
        
        SupportMultiInstanceView.getInstances().clear();
        
        assertEquals(0, SupportMultiInstanceView.getInstances().size());
        assertEquals(0, myView.getSubViewsPerKey().size());
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 1);
        values.put("b", 1);
        values.put("c", "c1");
        
        IEvent event1 = new TupleEvent("stream", eventType, values);
        
        stream.add(event1);
        
        assertEquals(1, SupportMultiInstanceView.getInstances().size());
        SupportMultiInstanceView child1 = SupportMultiInstanceView.getInstances().get(0);
        assertEquals(child1, myView.getSubViewsPerKey().get(1));
        
        values = new HashMap<String, Object>();
        values.put("a", TWO);
        values.put("b", TWO);
        values.put("c", "c2");
        
        IEvent event2 = new TupleEvent("stream", eventType, values);
        
        stream.add(event2);
        
        assertEquals(TWO, SupportMultiInstanceView.getInstances().size());
        SupportMultiInstanceView child2 = SupportMultiInstanceView.getInstances().get(1);
        assertEquals(child2, myView.getSubViewsPerKey().get(TWO));
    }
    
}

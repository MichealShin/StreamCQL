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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.support.SupportConst;
import com.huawei.streaming.support.SupportEventMng;
import com.huawei.streaming.view.IDataCollection;

/**
 * <WindowRelativeAccessTest>
 * <功能详细描述>
 * 
 */
public class WindowRelativeAccessTest
{
    private WindowRelativeAccess access;
    
    private RelativeAccessByEventAndIndexService service = new RelativeAccessByEventAndIndexService();
    
    private SupportEventMng mng = new SupportEventMng();;
    
    private IEventType eventType = mng.getInput();
    
    /**
     * Test WindowRelativeAccess(RelativeAccessByEventAndIndexService).
     */
    @Test
    public void testWindowRelativeAccess()
    {
        try
        {
            access = new WindowRelativeAccess(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
    }
    
    /**
     * Test getEvent(IEvent, int).
     */
    @Test
    public void testGetEvent()
    {
        access = new WindowRelativeAccess(service);
        
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("a", 0);
        values.put("b", 0);
        values.put("c", "c0");
        
        IEvent event0 = new TupleEvent("stream", eventType, values);
        
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
        
        access.update(new IEvent[] {event0}, null);
        assertEquals(event0, access.getEvent(event0, SupportConst.I_ZERO));
        assertNull(access.getEvent(event0, SupportConst.I_ONE));
        
        // sends the newest event last (i.e. 1 older 2 as 1 is sent first)
        access.update(new IEvent[] {event1, event2}, null);
        assertEquals(event1, access.getEvent(event1, SupportConst.I_ZERO));
        assertNull(access.getEvent(event1, SupportConst.I_ONE));
        assertEquals(event2, access.getEvent(event2, SupportConst.I_ZERO));
        assertEquals(event1, access.getEvent(event2, SupportConst.I_ONE));
        assertNull(access.getEvent(event2, SupportConst.I_TWO));
        
        // sends the newest event last (i.e. 1 older 2 as 1 is sent first)
        access.update(new IEvent[] {event3, event4, event5}, null);
        assertEquals(event3, access.getEvent(event3, SupportConst.I_ZERO));
        assertNull(access.getEvent(event3, SupportConst.I_ONE));
        assertEquals(event4, access.getEvent(event4, SupportConst.I_ZERO));
        assertEquals(event3, access.getEvent(event4, SupportConst.I_ONE));
        assertNull(access.getEvent(event4, SupportConst.I_TWO));
        assertEquals(event5, access.getEvent(event5, SupportConst.I_ZERO));
        assertEquals(event4, access.getEvent(event5, SupportConst.I_ONE));
        assertEquals(event3, access.getEvent(event5, SupportConst.I_TWO));
        assertNull(access.getEvent(event5, SupportConst.I_THREE));
    }
    
    /**
     * Test renew().
     */
    @Test
    public void testRenew()
    {
        access = new WindowRelativeAccess(service);
        IDataCollection renew = access.renew();
        
        Assert.assertEquals(true, renew instanceof WindowRelativeAccess);
    }
    
}

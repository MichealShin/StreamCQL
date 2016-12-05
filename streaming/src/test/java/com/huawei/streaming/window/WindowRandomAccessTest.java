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
 * <WindowRandomAccessTest>
 * <功能详细描述>
 * 
 */
public class WindowRandomAccessTest
{
    private WindowRandomAccess access;
    
    private RandomAccessByIndexService service = new RandomAccessByIndexService();
    
    private SupportEventMng mng = new SupportEventMng();;
    
    private IEventType eventType = mng.getInput();
    
    /**
     * Test WindowRandomAccess(RandomAccessByIndexService).
     */
    @Test
    public void testWindowRandomAccess()
    {
        try
        {
            access = new WindowRandomAccess(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertTrue(true);
        }
    }
    
    /**
     * Test getEvent(int, boolean).
     */
    @Test
    public void testGetEvent()
    {
        access = new WindowRandomAccess(service);
        
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
        
        assertNull(access.getEvent(SupportConst.I_ZERO, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
        
        access.update(new IEvent[] {event1}, null);
        assertEquals(event1, access.getEvent(SupportConst.I_ZERO, true));
        assertNull(access.getEvent(SupportConst.I_ONE, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
        
        access.update(new IEvent[] {event2, event3}, null);
        assertEquals(event3, access.getEvent(SupportConst.I_ZERO, true));
        assertEquals(event2, access.getEvent(SupportConst.I_ONE, true));
        assertEquals(event1, access.getEvent(SupportConst.I_TWO, true));
        assertNull(access.getEvent(SupportConst.I_THREE, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
        
        access.update(new IEvent[] {event4}, new IEvent[] {event1});
        assertEquals(event4, access.getEvent(SupportConst.I_ZERO, true));
        assertEquals(event3, access.getEvent(SupportConst.I_ONE, true));
        assertEquals(event2, access.getEvent(SupportConst.I_TWO, true));
        assertNull(access.getEvent(SupportConst.I_THREE, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
        
        access.update(null, new IEvent[] {event2, event3});
        assertEquals(event4, access.getEvent(SupportConst.I_ZERO, true));
        assertNull(access.getEvent(SupportConst.I_ONE, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
        
        access.update(null, new IEvent[] {event4});
        assertNull(access.getEvent(SupportConst.I_ZERO, true));
        assertNull(access.getEvent(SupportConst.I_ZERO, false));
    }
    
    /**
     * Test renew().
     */
    @Test
    public void testRenew()
    {
        access = new WindowRandomAccess(service);
        IDataCollection renew = access.renew();
        
        Assert.assertEquals(true, renew instanceof WindowRandomAccess);
    }
    
}

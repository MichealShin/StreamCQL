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

package com.huawei.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PreviousExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.output.OutPutPrint;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.processor.SimpleOutputProcessor;
import com.huawei.streaming.support.SupportConst;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.ProcessView;
import com.huawei.streaming.window.LengthBatchWindow;
import com.huawei.streaming.window.LengthSlideWindow;
import com.huawei.streaming.window.RandomAccessByIndexService;
import com.huawei.streaming.window.RelativeAccessByEventAndIndexService;
import com.huawei.streaming.window.WindowRandomAccess;
import com.huawei.streaming.window.WindowRelativeAccess;

/**
 * <PreviousTest>
 * <功能详细描述>
 * 
 */
public class PreviousTest
{
    /**
     * <test()>
     * <功能详细描述>
     */
    @Test
    public void test()
        throws StreamingException
    {
        //select prev(1, a) from schemName.WinLengthSlide(5);
        
        EventTypeMng eventTypeMng = new EventTypeMng();
        //输入流类型
        List<Attribute> schema = new ArrayList<Attribute>();
        Attribute attributeA = new Attribute(Integer.class, "a");
        Attribute attributeB = new Attribute(Integer.class, "b");
        Attribute attributeC = new Attribute(String.class, "c");
        schema.add(attributeA);
        schema.add(attributeB);
        schema.add(attributeC);
        TupleEventType tupleEventType = new TupleEventType("schemName", schema);
        //输出流类型
        List<Attribute> outschema = new ArrayList<Attribute>();
        Attribute attributePrev = new Attribute(Integer.class, "prev(1, a)");
        outschema.add(attributePrev);
        TupleEventType outtupleEventType = new TupleEventType("outschema", outschema);
        
        eventTypeMng.addEventType(tupleEventType);
        eventTypeMng.addEventType(outtupleEventType);
        
        FirstLevelStream firststream = new FirstLevelStream();
        LengthSlideWindow win = new LengthSlideWindow(SupportConst.I_FIVE);
        ProcessView processview = new ProcessView();
        
        /**
         * select
         */
        IExpression[] exprNodes = new IExpression[SupportConst.I_ONE];
        exprNodes[0] = new PreviousExpression(new ConstExpression(1), new PropertyValueExpression("a", Integer.class));
        
        /**
         * DataCollection
         */
        RandomAccessByIndexService service = new RandomAccessByIndexService();
        WindowRandomAccess randomAccess = new WindowRandomAccess(service);
        
        win.setDataCollection(randomAccess);
        ((PreviousExpression)exprNodes[0]).setService(service);
        
        SelectSubProcess selector = new SelectSubProcess("out", exprNodes, null, outtupleEventType);
        SimpleOutputProcessor simple = new SimpleOutputProcessor(selector, null, new OutPutPrint(), OutputType.I);
        
        firststream.addView(win);
        win.addView(processview);
        processview.setProcessor(simple);
        
        firststream.start();
        
        Map<String, Object> values = new HashMap<String, Object>();
        
        //发送数据
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < SupportConst.I_FIVE; i++)
        {
            values.put("a", i);
            values.put("b", i);
            values.put("c", "abc");
            
            TupleEvent tupleEvent2 = new TupleEvent("stream", "schemName", values, eventTypeMng);
            firststream.add(tupleEvent2);
        }
        long endTime = System.currentTimeMillis();
        
        firststream.stop();
        System.out.println(endTime - startTime);
    }
    
    /**
     * <test()>
     * <功能详细描述>
     */
    @Test
    public void testBatchWindow()
        throws StreamingException
    {
        //select prev(1, a) from schemName.WinLengthBatch(5);
        
        EventTypeMng eventTypeMng = new EventTypeMng();
        //输入流类型
        List<Attribute> schema = new ArrayList<Attribute>();
        Attribute attributeA = new Attribute(Integer.class, "a");
        Attribute attributeB = new Attribute(Integer.class, "b");
        Attribute attributeC = new Attribute(String.class, "c");
        schema.add(attributeA);
        schema.add(attributeB);
        schema.add(attributeC);
        TupleEventType tupleEventType = new TupleEventType("schemName", schema);
        //输出流类型
        List<Attribute> outschema = new ArrayList<Attribute>();
        Attribute attributePrev = new Attribute(Integer.class, "prev(1, a)");
        outschema.add(attributePrev);
        TupleEventType outtupleEventType = new TupleEventType("outschema", outschema);
        
        eventTypeMng.addEventType(tupleEventType);
        eventTypeMng.addEventType(outtupleEventType);
        
        FirstLevelStream firststream = new FirstLevelStream();
        LengthBatchWindow win = new LengthBatchWindow(SupportConst.I_FIVE);
        ProcessView processview = new ProcessView();
        
        /**
         * select
         */
        IExpression[] exprNodes = new IExpression[SupportConst.I_ONE];
        exprNodes[0] = new PreviousExpression(new ConstExpression(1), new PropertyValueExpression("a", Integer.class));
        
        /**
         * DataCollection
         */
        RelativeAccessByEventAndIndexService service = new RelativeAccessByEventAndIndexService();
        WindowRelativeAccess relativeAccess = new WindowRelativeAccess(service);
        win.setDataCollection(relativeAccess);
        ((PreviousExpression)exprNodes[0]).setService(service);
        
        SelectSubProcess selector = new SelectSubProcess("out", exprNodes, null, outtupleEventType);
        SimpleOutputProcessor simple = new SimpleOutputProcessor(selector, null, new OutPutPrint(), OutputType.I);
        
        firststream.addView(win);
        win.addView(processview);
        processview.setProcessor(simple);
        
        firststream.start();
        
        Map<String, Object> values = new HashMap<String, Object>();
        
        //发送数据
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < SupportConst.I_FIVE; i++)
        {
            values.put("a", i);
            values.put("b", i);
            values.put("c", "abc");
            
            TupleEvent tupleEvent2 = new TupleEvent("stream", "schemName", values, eventTypeMng);
            firststream.add(tupleEvent2);
        }
        long endTime = System.currentTimeMillis();
        
        firststream.stop();
        System.out.println(endTime - startTime);
    }
    
}

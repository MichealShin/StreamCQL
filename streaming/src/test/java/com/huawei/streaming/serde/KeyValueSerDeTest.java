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

package com.huawei.streaming.serde;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.util.datatype.TimestampParser;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;

/**
 * KeyValue格式序列化和反序列化
 * 
 */
public class KeyValueSerDeTest
{
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueSerDeTest.class);
    
    private static TupleEventType schema;
    
    private static TupleEventType schema1;

    static
    {
        
        List<Attribute> atts = Lists.newArrayList();
        atts.add(new Attribute(String.class, "a"));
        atts.add(new Attribute(int.class, "b"));
        atts.add(new Attribute(long.class, "c"));
        atts.add(new Attribute(float.class, "d"));
        atts.add(new Attribute(double.class, "e"));
        atts.add(new Attribute(boolean.class, "f"));
        atts.add(new Attribute(Time.class, "g"));
        atts.add(new Attribute(Date.class, "h"));
        atts.add(new Attribute(Timestamp.class, "i"));
        atts.add(new Attribute(BigDecimal.class, "j"));
        
        schema = new TupleEventType("S1", atts);
        
        List<Attribute> atts2 = Lists.newArrayList();
        atts2.add(new Attribute(Integer.class, "a"));
        
        schema1 = new TupleEventType("S2", atts2);
    }
    
    /**
     * 反序列化测试
     */
    @Test
    public void testDeSerialize() throws StreamingException, StreamSerDeException
    {
        StreamingConfig config = new StreamingConfig();
        KeyValueSerDe deser = new KeyValueSerDe();
        deser.setSchema(schema);
        deser.setConfig(config);
        deser.initialize();
        String s = "a=a,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            List<Object[]> objs = deser.deSerialize(s);
            int len = objs.get(0).length;
            assertTrue(len == schema.getAllAttributeNames().length);
            assertTrue(objs.get(0)[0].equals("a"));
            assertTrue(objs.get(0)[1].equals(1));
            assertTrue(objs.get(0)[2].equals(2l));
            assertTrue(objs.get(0)[3].equals(3.0f));
            assertTrue(objs.get(0)[4].equals(4.0d));
            assertTrue(objs.get(0)[5].equals(true));
            assertTrue(new BigDecimal(100.0).compareTo((BigDecimal)objs.get(0)[9]) == 0);
        }
        catch (StreamSerDeException e)
        {
            LOG.error("failed to deser keyvalue.", e);
            fail("failed to deser keyvalue.");
        }
        
    }
    
    /**
     * 反序列化测试
     */
    @Test
    public void testDeSerialize2() throws StreamingException, StreamSerDeException
    {
        StreamingConfig config = new StreamingConfig();
        KeyValueSerDe deser = new KeyValueSerDe();
        deser.setConfig(config);
        deser.setSchema(schema1);
        deser.initialize();
        String s = "a=1";
        try
        {
            List<Object[]> objs = deser.deSerialize(s);
            int len = objs.get(0).length;
            assertTrue(len == schema1.getAllAttributeNames().length);
            assertTrue(objs.get(0)[0].equals(1));
        }
        catch (StreamSerDeException e)
        {
            LOG.error("failed to deser keyvalue.", e);
            fail("failed to deser keyvalue.");
        }
        
    }
    
    /**
     * 序列化测试
     */
    @Test
    public void testSerialize() throws StreamingException, StreamSerDeException
    {
        StreamingConfig config = new StreamingConfig();
        KeyValueSerDe deser = new KeyValueSerDe();
        deser.setConfig(config);
        deser.setSchema(schema);
        deser.initialize();
        String s = "a=a,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            List<Object[]> objs = deser.deSerialize(s);
            String value = (String)deser.serialize(objs);
            assertNotNull(value);
        }
        catch (StreamSerDeException e)
        {
            LOG.error("failed to deser keyvalue.", e);
            
            fail("failed to ser keyvalue.");
        }
    }
    
    /**
     * 序列化测试
     */
    @Test
    public void testSerialize2() throws StreamingException, StreamSerDeException
    {
        List<Attribute> schema = new ArrayList<Attribute>();
        Attribute attributeA = new Attribute(Timestamp.class, "A");
        schema.add(attributeA);
        TupleEventType tupleEventType = new TupleEventType("schemaName", schema);
        
        Object[] obj = {new TimestampParser(new StreamingConfig()).createValue("2014-10-05 11:08:00")};
        List<Object[]> events = new ArrayList<Object[]>();
        events.add(obj);

        StreamingConfig config = new StreamingConfig();
        KeyValueSerDe kvserde = new KeyValueSerDe();
        kvserde.setSchema(tupleEventType);
        kvserde.setConfig(config);
        kvserde.initialize();
        Object result = kvserde.serialize(events);
        
        assertTrue("A=2014-10-05 11:08:00.000 +0800".equals(result.toString()));
    }

    /**
     * 序列化测试
     */
    @Test
    public void testSerialize3() throws StreamingException, StreamSerDeException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.SERDE_KEYVALUESERDE_SEPARATOR,"---");

        List<Attribute> schema = Lists.newArrayList();
        schema.add( new Attribute(Timestamp.class, "A"));
        schema.add(new Attribute(String.class, "B"));
        TupleEventType tupleEventType = new TupleEventType("schemaName", schema);
        Object[] obj = {new TimestampParser(config).createValue("2014-10-05 11:08:00"),"a"};
        Object[] obj2 = {new TimestampParser(config).createValue("2014-10-05 11:08:00"),"b"};
        List<Object[]> events = new ArrayList<Object[]>();
        events.add(obj);
        events.add(obj2);

        KeyValueSerDe kvserde = new KeyValueSerDe();
        kvserde.setSchema(tupleEventType);
        kvserde.setConfig(config);
        kvserde.initialize();
        Object result = kvserde.serialize(events);

        assertTrue("A=2014-10-05 11:08:00.000 +0800---B=a\nA=2014-10-05 11:08:00.000 +0800---B=b".equals(result.toString()));
    }

}

package com.huawei.streaming.serde;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import static org.junit.Assert.*;

/**
 * binary的序列化和反序列化测试用例
 */
public class BinarySerDeTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BinarySerDeTest.class);

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
     * 测试用例1
     */
    @Test
    public void testCase1() throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.SERDE_BINARYSERDE_TIMETYPE,"String");

        config.put(StreamingConfig.SERDE_BINARYSERDE_ATTRIBUTESLENGTH,"1,4,8,4,8,1,8,10,29,6");

        BinarySerDe binarySerDe = new BinarySerDe();
        binarySerDe.setSchema(schema);
        binarySerDe.setConfig(config);

        String s = "a=a,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            binarySerDe.initialize();
            KeyValueSerDe kvserde = new KeyValueSerDe();
            kvserde.setSchema(schema);
            kvserde.setConfig(config);
            kvserde.initialize();
            List<Object[]> objs = kvserde.deSerialize(s);
            List<Object> bytes = (List< Object >)binarySerDe.serialize(objs);
            objs = binarySerDe.deSerialize(bytes.get(0));

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
            LOG.error("failed to deser binary.", e);
            fail("failed to deser binary.");
        }

    }

    /**
     * 测试用例1
     */
    @Test
    public void testCase2() throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.SERDE_BINARYSERDE_TIMETYPE,"String");

        /*
         * 除了boolean和bigdecimal类型无法自动拓展长度外，其他字段都可以
         */
        config.put(StreamingConfig.SERDE_BINARYSERDE_ATTRIBUTESLENGTH,"10,40,80,40,80,1,80,100,203,6");

        BinarySerDe binarySerde = new BinarySerDe();
        binarySerde.setSchema(schema);
        binarySerde.setConfig(config);

        String s = "a=a ,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            binarySerde.initialize();
            KeyValueSerDe kvserde = new KeyValueSerDe();
            kvserde.setSchema(schema);
            kvserde.setConfig(config);
            kvserde.initialize();
            List<Object[]> objs = kvserde.deSerialize(s);
            List<Object> bytes = (List< Object >)binarySerde.serialize(objs);
            objs = binarySerde.deSerialize(bytes.get(0));

            int len = objs.get(0).length;
            assertTrue(len == schema.getAllAttributeNames().length);
            assertTrue(objs.get(0)[0].equals("a "));
            assertTrue(objs.get(0)[1].equals(1));
            assertTrue(objs.get(0)[2].equals(2l));
            assertTrue(objs.get(0)[3].equals(3.0f));
            assertTrue(objs.get(0)[4].equals(4.0d));
            assertTrue(objs.get(0)[5].equals(true));
            assertTrue(new BigDecimal(100.0).compareTo((BigDecimal)objs.get(0)[9]) == 0);
        }
        catch (StreamSerDeException e)
        {
            LOG.error("failed to deser binary.", e);
            fail("failed to deser binary.");
        }

    }


    /**
     * 测试用例1
     */
    @Test
    public void testCase4() throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.SERDE_BINARYSERDE_TIMETYPE,"long");
        config.put(StreamingConfig.SERDE_BINARYSERDE_DECIMALYPE,"String");

        /*
         * 除了boolean和bigdecimal类型无法自动拓展长度外，其他字段都可以
         */
        config.put(StreamingConfig.SERDE_BINARYSERDE_ATTRIBUTESLENGTH,"10,40,80,40,80,1,80,100,203,60");

        BinarySerDe bianrySerDe = new BinarySerDe();
        bianrySerDe.setSchema(schema);
        bianrySerDe.setConfig(config);

        String s = "a=a,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            bianrySerDe.initialize();
            KeyValueSerDe kvserde = new KeyValueSerDe();
            kvserde.setSchema(schema);
            kvserde.setConfig(config);
            kvserde.initialize();
            List<Object[]> objs = kvserde.deSerialize(s);
            List<Object> bytes = (List< Object >)bianrySerDe.serialize(objs);
            objs = bianrySerDe.deSerialize(bytes.get(0));

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
            LOG.error("failed to deser binary.", e);
            fail("failed to deser binary.");
        }

    }

    /**
     * 测试用例1
     */
    @Test
    public void testCase3() throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.put(StreamingConfig.SERDE_BINARYSERDE_TIMETYPE,"String");

        /*
         * int类型缩短长度
         */
        config.put(StreamingConfig.SERDE_BINARYSERDE_ATTRIBUTESLENGTH,"10,3,80,40,80,1,80,100,203,6");

        BinarySerDe bianrySerDe = new BinarySerDe();
        bianrySerDe.setSchema(schema);
        bianrySerDe.setConfig(config);

        String s = "a=a,b=1,c=2,d=3.0,e=4.0,f=true,g=16:44:00,h=2014-09-26,i=2014-09-26 16:44:00,j=100.0";
        try
        {
            bianrySerDe.initialize();
            KeyValueSerDe kvserde = new KeyValueSerDe();
            kvserde.setSchema(schema);

            List<Object[]> objs = kvserde.deSerialize(s);
            Object bytes = bianrySerDe.serialize(objs);
            objs = bianrySerDe.deSerialize(bytes);

            int len = objs.get(0).length;
            assertTrue(len == schema.getAllAttributeNames().length);
            assertTrue(objs.get(0)[0].equals("a"));
            assertTrue(objs.get(0)[1].equals(1));
            assertTrue(objs.get(0)[2].equals(2l));
            assertTrue(objs.get(0)[3].equals(3.0f));
            assertTrue(objs.get(0)[4].equals(4.0d));
            assertTrue(objs.get(0)[5].equals(true));
            assertTrue(new BigDecimal(100.0).compareTo((BigDecimal)objs.get(0)[9]) == 0);
            fail("Failed to deser binary.");
        }
        catch (StreamSerDeException e)
        {
            assertTrue(true);
        }

    }

}

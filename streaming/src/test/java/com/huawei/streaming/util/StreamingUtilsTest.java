package com.huawei.streaming.util;

import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by h00183771 on 2016/12/1.
 */
public class StreamingUtilsTest {
    @Test
    public void serializeSchemas() throws Exception {
        List<Attribute> schema = new ArrayList<Attribute>();
        Attribute attributeA = new Attribute(Integer.class, "a");
        Attribute attributeB = new Attribute(Integer.class, "b");
        Attribute attributeC = new Attribute(String.class, "c");
        schema.add(attributeA);
        schema.add(attributeB);
        schema.add(attributeC);
        TupleEventType tupleEventType = new TupleEventType("schemName", schema);

        String str = StreamingUtils.serializeSchemas(Arrays.asList(tupleEventType));
        List<TupleEventType> res =  StreamingUtils.deSerializeSchemas(str);
        assertTrue(res.get(0).getEventTypeName().equals("schemName"));
        assertTrue(res.get(0).getAttributeName(0).equals("a"));
    }

}
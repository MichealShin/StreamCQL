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

package com.huawei.streaming.cql.executor;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.api.opereators.KafkaInputOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.TCPClientOutputOperator;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.opereators.serdes.SimpleSerDeAPI;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.ConstInTestCase;

/**
 * 物理计划序列化方法测试类
 *
 */
public class PhysicalPlanWriterTest
{
        
    private static final String BASICPATH = File.separator + "executor" + File.separator + "planserde" + File.separator;
    
    private static PhysicalPlan phyplan = null;
    
    private static String serDir = null;
    
    private static String serFileName = "ser.xml";
    
    /**
     * 类初始化之前要执行的方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
        throws Exception
    {
        /*
         * 初始化序列化文件路径
         */
        setDir();
        
        /*
         * Schema定义
         */
        Schema schema1 = createSchema1();
        
        Schema schema2 = createSchema2();
        
        List<Schema> schemas = Lists.newArrayList();
        schemas.add(schema1);
        schemas.add(schema2);
        
        /**
         * 算子定义
         */
        KafkaInputOperator input1 = new KafkaInputOperator("id1", 1);
        KafkaInputOperator input2 = new KafkaInputOperator("id2", 1);
        FilterOperator filter3 = new FilterOperator("id3", 1);
        JoinFunctionOperator join5 = new JoinFunctionOperator("id5", 1);
        FilterOperator filter6 = new FilterOperator("id6", 1);
        TCPClientOutputOperator output8 = new TCPClientOutputOperator("id7", 1);
        
        createKafkaReaderOperator(input1);
        createKafkaReaderOperator(input2);
        
        createFilterOperator(filter3);
        
        createJoinOperator(join5);
        
        createFilterOperator2(filter6);

        createTcpWriterOperator(output8);
        
        List<Operator> ops = Lists.newArrayList();
        ops.add(input1);
        ops.add(input2);
        ops.add(filter3);
        ops.add(join5);
        ops.add(filter6);
        ops.add(output8);
        
        /**
         * 自定义函数
         * 自定义窗口
         * 自定义参数
         */
        TreeMap<String, String> udsargs = Maps.newTreeMap();
        udsargs.put("app.args.1", "true");
        
        String[] files = {"d:\\conf.xml,e:\\abc.jar"};
        
        /**
         * 同一个spout，输出流名称必须相同
         * 同一个算子输出流名称可以一样
         * 同一个算子，输入流名称不允许重复
         *
         * 不同的算子，流名称必然不一样
         *
         * 同一个算子，分发特征可以不一样，但是schema名称和流名称必须一样
         */
        OperatorTransition ot1 = new OperatorTransition("s1", input1, filter3, DistributeType.SHUFFLE, null, schema1);
        OperatorTransition ot2 = new OperatorTransition("s2", input2, join5, DistributeType.FIELDS, "id,name", schema2);
        OperatorTransition ot3 =
            new OperatorTransition("s3", filter3, join5, DistributeType.FIELDS, "id,name", schema1);
        OperatorTransition ot4 = new OperatorTransition("s4", join5, filter6, DistributeType.SHUFFLE, null, schema1);
        
        OperatorTransition ot5 = new OperatorTransition("s5", filter6, output8, DistributeType.SHUFFLE, null, schema1);

        List<OperatorTransition> ots = Lists.newArrayList();
        ots.add(ot1);
        ots.add(ot2);
        ots.add(ot3);
        ots.add(ot4);
        ots.add(ot5);

        join5.setLeftStreamName("s3");
        join5.setRightStreamName("s4");
        
        Application app = new Application("id01");
        app.setApplicationName("testapp");
        app.setOperators(ops);
        app.setSchemas(schemas);
        app.setOpTransition(ots);
        
        setUserConfs(udsargs, files, app);
        phyplan = new PhysicalPlan();
        phyplan.setApploication(app);
    }
    
    private static void setUserConfs(TreeMap<String, String> udsargs, String[] files, Application app)
    {
        /**
         * 窗口定义好之后，直接用名称就可以识别出来
         * 系统自动进行窗口的注册
         */
        Map<String, String> testcomonArgs6 = Maps.newHashMap();
        testcomonArgs6.put("udw.length", "5");

        List<UserFunction > udfs = Lists.newArrayList();
        UserFunction userFunction = new UserFunction();
        userFunction.setName("udf1");
        userFunction.setClazz("com.huawei.streaming.udf.ToChar");
        udfs.add(userFunction);

        app.setConfs(udsargs);
        app.setUserFiles(files);
        app.setUserFunctions(udfs);
    }
    
    private static Schema createSchema2()
    {
        Schema schema2 = new Schema("s02");
        Column c3 = new Column("c3", String.class);
        Column c4 = new Column("c4", Integer.class);
        schema2.addCol(c3);
        schema2.addCol(c4);
        return schema2;
    }
    
    private static Schema createSchema1()
    {
        Schema schema1 = new Schema("s01");
        Column c1 = new Column("c1", String.class);
        Column c2 = new Column("c2", Integer.class);
        schema1.addCol(c1);
        schema1.addCol(c2);
        return schema1;
    }
    
    private static void createTcpWriterOperator(TCPClientOutputOperator output8)
    {
        TreeMap<String, String> deser8args = Maps.newTreeMap();
        deser8args.put("split", "|");
        SimpleSerDeAPI ser = new SimpleSerDeAPI();
        ser.setSeparator(",");
        output8.setArgs(deser8args);
        output8.setSerializer(ser);
        output8.setPort("10000");
        output8.setServer("localhost");
    }
    
    private static void createFilterOperator2(FilterOperator filter6)
    {
        TreeMap<String, String> testcomonArgs4 = Maps.newTreeMap();
        testcomonArgs4.put("filter.before.window", "true");
        filter6.setFilterExpression("c3<=1");
        filter6.setArgs(testcomonArgs4);
    }
    
    private static void createJoinOperator(JoinFunctionOperator join5)
    {
        TreeMap<String, String> testcomonArgs3 = Maps.newTreeMap();
        testcomonArgs3.put("join.type", "hashjoini");
        join5.setArgs(testcomonArgs3);
        join5.setJoinType(JoinType.INNER_JOIN);
        //取消new window，直接参数
        join5.setLeftWindow(Window.createKeepAllWindow());
        join5.setRightWindow(Window.createTimeSlideWindow(ConstInTestCase.I_100));
        join5.setJoinExpression("name3.c1 = name4.c1 and name3.c2 = name4.c4+2");
        join5.setFilterAfterJoinExpression("name4.c1<10");
        join5.setOutputExpression("udf(c1),to_String(c4)");
    }
    
    private static void createFilterOperator(FilterOperator filter3)
    {
        TreeMap<String, String> testcomonArgs = Maps.newTreeMap();
        testcomonArgs.put("filter.optimizer", "com.huawei.optimizer.filter");
        filter3.setFilterExpression("(a<=1) or (a>10) and b is not null and c in ('1','2','3')");
        filter3.setArgs(testcomonArgs);
    }
    
    private static void createKafkaReaderOperator(KafkaInputOperator input2)
    {
        SimpleSerDeAPI deser2 = new SimpleSerDeAPI();
        deser2.setSeparator("|");
        TreeMap<String, String> deser2args = Maps.newTreeMap();
        deser2args.put("zookeeper.ip.address", "localhost");
        deser2args.put("zookeeper.port", "2180");
        input2.setArgs(deser2args);
        input2.setDeserializer(deser2);
        input2.setZookeepers("localhost:2181");
        input2.setGroupId("g1");
    }
    
    /**
     * 设置待序列化的文件路径
     *
     */
    private static void setDir()
    {
        String classPath = PhysicalPlanWriterTest.class.getResource("/").getPath();
        
        try
        {
            classPath = URLDecoder.decode(classPath, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        
        serDir = classPath + BASICPATH + File.separator;
    }
    
    /**
     * Test method for
     * {@link com.huawei.streaming.cql.executor.PhysicalPlanWriter#write
     * (com.huawei.streaming.api.PhysicalPlan, java.lang.String)}.
     */
    @Test
    public void testWrite()
    {
        PhysicalPlanWriter.write(phyplan, serDir + serFileName);
    }
    
}

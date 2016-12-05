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

package com.huawei.streaming.cql;

import com.huawei.streaming.api.opereators.KafkaInputOperator;
import com.huawei.streaming.api.opereators.KafkaOutputOperator;
import com.huawei.streaming.cql.executor.PhysicalPlanLoader;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator;
import com.huawei.streaming.operator.inputstream.KafkaSourceOp;
import com.huawei.streaming.operator.outputstream.KafkaFunctionOp;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.parser.ApplicationParseTest;

/**
 * Driver正常测试用例
 *
 */
public class DriverTest
{

    private static final String TMP_TEST_DEPENDS = "/tmp/testDepends/";

    private static final Logger LOG = LoggerFactory.getLogger(DriverTest.class);

    /**
     * 最基本的文件路径
     */
    private static final String BASICPATH = File.separator + "driver" + File.separator;

    /**
     * 原始SQL文件路径
     */
    private static String inPutDir = null;

    /**
     * 最终结果路径，这个文件只生成一次，之后每次仅仅进行对比
     */
    private static String outPutDir = null;

    /**
     * 当前运行结果文件，每次测试用例执行前，先清空文件夹中的内容，再执行
     */
    private static String resultPutDir = null;

    /**
     * 初始化测试类之前要执行的初始化方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
     throws Exception
    {
        SimpleLexer.registerInputOperator("TCPServerInput", TCPServerInputOperator.class);
        PhysicalPlanLoader.registerPhysicalPlanAlias("TCPServerInput",
         com.huawei.streaming.cql.toolkits.api.TCPServerInputOperator.class);
        InputOutputOperatorMapping.registerOperator(
         com.huawei.streaming.cql.toolkits.api.TCPServerInputOperator.class,
         com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator.class);

        setDir();
        /*
         * 清空结果文件夹内容
         */
        CQLTestCommons.emptyDir(new File(resultPutDir));

    }

    private static void removeTmpDir(File tmpDir)
    {
        try
        {
            FileUtils.deleteDirectory(tmpDir);

        }
        catch (IOException e)
        {
            LOG.error("failed to delete tmp dir", e);
        }
    }

    private static void setDir()
     throws UnsupportedEncodingException
    {
        String classPath = ApplicationParseTest.class.getResource("/").getPath();
        classPath = URLDecoder.decode(classPath, "UTF-8");
        inPutDir = classPath + BASICPATH + CQLTestCommons.INPUT_DIR_NAME + File.separator;
        outPutDir = classPath + BASICPATH + CQLTestCommons.OUTPUT_DIR_NAME + File.separator;
        resultPutDir = classPath + BASICPATH + CQLTestCommons.RESULT_DIR_NAME + File.separator;
    }

    /**
     * 所有测试用例执行完毕之后执行的方法
     *
     */
    @AfterClass
    public static void tearDownAfterClass()
     throws Exception
    {
        SimpleLexer.unRegisterInput("TCPServerInput");
        PhysicalPlanLoader.unRegisterPhysicalPlanAlias("TCPServerInput");
        InputOutputOperatorMapping.unRegisterMapping(
         com.huawei.streaming.cql.toolkits.api.TCPServerInputOperator.class);
        File tmpDir = new File(TMP_TEST_DEPENDS);
        LOG.info("delete tmp user dir {} in test case tear down", tmpDir.getAbsolutePath());
        removeTmpDir(tmpDir);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSimple()
     throws Exception
    {
        executeCase("simple");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAggregate()
     throws Exception
    {
        executeCase("aggregate");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAggregate2()
     throws Exception
    {
        executeCase("aggregate2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAggregate3()
     throws Exception
    {
        //还有问题
        executeCase("aggregate3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin()
     throws Exception
    {
        executeCase("join");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin2()
     throws Exception
    {
        executeCase("join2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin3()
     throws Exception
    {
        executeCase("join3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin5()
     throws Exception
    {
        executeCase("join5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin6()
     throws Exception
    {
        executeCase("join6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin7()
     throws Exception
    {
        executeCase("join7");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin8()
     throws Exception
    {
        executeCase("selfJoin");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin9()
     throws Exception
    {
        executeCase("join_aggregate");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin10()
     throws Exception
    {
        executeCase("selfJoin2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin11()
     throws Exception
    {
        executeCase("selfJoin3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin12()
     throws Exception
    {
        executeCase("selfJoin4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin13()
     throws Exception
    {
        executeCase("selfJoin5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testJoin14()
     throws Exception
    {
        executeCase("join8");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testGroupby1()
     throws Exception
    {
        executeCase("groupby1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testGroupby2()
     throws Exception
    {
        executeCase("groupby2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFilterBeforeWindow()
     throws Exception
    {
        executeCase("filterbeforewindow");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFilterBeforeWindow2()
     throws Exception
    {
        executeCase("filterbeforewindow2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testWhereLengthSlide()
     throws Exception
    {
        executeCase("where_length_slide");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAggregateFilter()
     throws Exception
    {
        executeCase("aggregateFilter");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAggregateFilter2()
     throws Exception
    {
        executeCase("aggregateFilter2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testChineline()
     throws Exception
    {
        executeCase("chineline");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testConfs()
     throws Exception
    {
        executeCase("confs");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testLocalSubmit()
     throws Exception
    {
        executeCase("localSubmit");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFunctionCase1()
     throws Exception
    {
        executeCase("functionCase1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFunctionWhen1()
     throws Exception
    {
        executeCase("functionWhen1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFunctionCount()
     throws Exception
    {
        executeCase("functionCount");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testHaving1()
     throws Exception
    {
        executeCase("having1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testHaving2()
     throws Exception
    {
        executeCase("having2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testHaving3()
     throws Exception
    {
        executeCase("having3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testHaving4()
     throws Exception
    {
        executeCase("having4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSortby()
     throws Exception
    {
        executeCase("sortby");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSortby1()
     throws Exception
    {
        executeCase("sortby1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSortby2()
     throws Exception
    {
        executeCase("sortby2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSortby3()
     throws Exception
    {
        executeCase("sortby3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testLimit()
     throws Exception
    {
        executeCase("limit");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testOutputWhereEventTbatch()
     throws Exception
    {
        executeCase("output_where_event_tbatch");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery1()
     throws Exception
    {
        executeCase("subQuery1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery2()
     throws Exception
    {
        executeCase("subQuery2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery3()
     throws Exception
    {
        executeCase("subQuery3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery4()
     throws Exception
    {
        executeCase("subQuery4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery5()
     throws Exception
    {
        executeCase("subQuery5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery6()
     throws Exception
    {
        executeCase("subQuery6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery7()
     throws Exception
    {
        executeCase("subQuery7");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFunctionIn()
     throws Exception
    {
        executeCase("functionIn");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testFunctionNot()
     throws Exception
    {
        executeCase("functionNot");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testNotLike()
     throws Exception
    {
        executeCase("not_like");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAppCaseWhen()
     throws Exception
    {
        executeCase("app_case_when");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testPrevious1()
     throws Exception
    {
        executeCase("previous1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCombine1()
     throws Exception
    {
        executeCase("combine1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCombine2()
     throws Exception
    {
        executeCase("combine2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCombine3()
     throws Exception
    {
        executeCase("combine3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSameStream()
     throws Exception
    {
        executeCase("sameStream");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUniformSerde()
     throws Exception
    {
        executeCase("UniformSerde");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAutoCreateStream1()
     throws Exception
    {
        executeCase("autoCreateStream1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testAutoCreateStream2()
     throws Exception
    {
        executeCase("autoCreateStream2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCast()
     throws Exception
    {
        executeCase("cast");
    }


    /**
     * 测试
     *
     */
    @Test
    public void testCast2()
     throws Exception
    {
        executeCase("cast2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testParallel1()
     throws Exception
    {
        executeCase("parallel1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testParallel2()
     throws Exception
    {
        executeCase("parallel2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testParallel3()
     throws Exception
    {
        executeCase("parallel3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testParallel4()
        throws Exception
    {
        executeCase("parallel4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testParallel5()
        throws Exception
    {
        executeCase("parallel5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testPrm()
     throws Exception
    {
        executeCase("prm");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource()
     throws Exception
    {
        executeCase("datasource");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource2()
     throws Exception
    {
        executeCase("datasource2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource3()
     throws Exception
    {
        executeCase("datasource3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource4()
        throws Exception
    {
        executeCase("datasource4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource5()
     throws Exception
    {
        executeCase("datasource5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource6()
        throws Exception
    {
        executeCase("datasource6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDataSource7()
        throws Exception
    {
        executeCase("datasource7");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDate()
     throws Exception
    {
        executeCase("date");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserDefinedOperator1()
     throws Exception
    {
        executeCase("userdefinedOperator1");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserDefinedOperator2()
     throws Exception
    {
        executeCase("userdefinedOperator2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserDefinedOperator3()
     throws Exception
    {
        executeCase("userdefinedOperator3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSimpleLexer()
     throws Exception
    {
        executeCase("simpleLexer");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSimpleLexer3()
     throws Exception
    {
        executeCase("simpleLexer3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSimpleLexer2()
     throws Exception
    {
        executeCase("simpleLexer2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSimpleAggregate()
     throws Exception
    {
        executeCase("simpleAggregate");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testBss()
     throws Exception
    {
        executeCase("bss");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCase1Perf2()
     throws Exception
    {
        executeCase("case1_perf2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit()
     throws Exception
    {
        executeCase("split");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit2()
     throws Exception
    {
        executeCase("split2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit3()
     throws Exception
    {
        executeCase("split3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit4()
     throws Exception
    {
        executeCase("split4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit5()
     throws Exception
    {
        executeCase("split5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit6()
        throws Exception
    {
        executeCase("split6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit7()
        throws Exception
    {
        executeCase("split7");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCCC()
     throws Exception
    {
        executeCase("ccc");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCCC2()
     throws Exception
    {
        executeCase("ccc2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCurrentTime()
     throws Exception
    {
        executeCase("currenttime");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testHiSpace()
     throws Exception
    {
        executeCase("HISPACE");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDistinct()
     throws Exception
    {
        executeCase("distinctCount");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUnionParallelNumber()
     throws Exception
    {
        executeCase("new_case1_15min_count20_3_perf");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUDFDistinct()
     throws Exception
    {
        executeCase("udfDistinct");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testTCPClientInput()
     throws Exception
    {
        executeCase("TCPClientInput");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testTCPClientOutput()
     throws Exception
    {
        executeCase("TCPClientOutput");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testKafkaInput()
     throws Exception
    {
        executeCase("kafkaInput");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testKafkaOutput()
     throws Exception
    {
        executeCase("kafkaOutput");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testPipeLineHaving()
     throws Exception
    {
        executeCase("pipelinehaving");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUDFDay()
     throws Exception
    {
        executeCase("udfday");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDecimalAgg()
        throws Exception
    {
        executeCase("decimalagg");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic1()
        throws Exception
    {
        executeCase("multiArithmetic1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic2()
        throws Exception
    {
        executeCase("multiArithmetic2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic3()
        throws Exception
    {
        executeCase("multiArithmetic3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic4()
        throws Exception
    {
        executeCase("multiArithmetic4");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic5()
        throws Exception
    {
        executeCase("multiArithmetic5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic6()
        throws Exception
    {
        executeCase("multiArithmetic6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic7()
        throws Exception
    {
        executeCase("multiArithmetic7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic8()
        throws Exception
    {
        executeCase("multiArithmetic8");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator1()
        throws Exception
    {
        executeCase("userOperator1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator2()
        throws Exception
    {
        executeCase("userOperator2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator3()
        throws Exception
    {
        executeCase("userOperator3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator4()
        throws Exception
    {
        executeCase("userOperator4");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator5()
        throws Exception
    {
        executeCase("userOperator5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator6()
        throws Exception
    {
        executeCase("userOperator6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator7()
        throws Exception
    {
        executeCase("userOperator7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator8()
        throws Exception
    {
        executeCase("userOperator8");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator9()
        throws Exception
    {
        executeCase("userOperator9");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUDFWithProperties()
        throws Exception
    {
        executeCase("udfWithProperties");
    }

    @Test
    public void testTimezone1()
        throws Exception
    {
        executeCase("timezone1");
    }

    private void executeCase(String caseName)
     throws Exception
    {
        long startTime = System.currentTimeMillis();
        DriverTestUtil qt =
         new DriverTestUtil(inPutDir + caseName + CQLTestCommons.INPUT_POSTFIX, outPutDir + caseName
          + CQLTestCommons.DRIVER_TEST_POSTFIX, resultPutDir + caseName + CQLTestCommons.DRIVER_TEST_POSTFIX);
        try
        {
            LOG.info("Begin query: " + caseName);
            qt.executeAndWrite();
            if (!qt.compareResults())
            {
                LOG.error("test result doesn't same!");
                fail("test result doesn't same!");
            }
        }
        catch (Throwable e)
        {
            LOG.error("Exception: " + e.getMessage(), e);
            LOG.error("Failed query: " + caseName);
            fail("Exception: " + e.getMessage());
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info("Done query: " + caseName + " elapsedTime=" + elapsedTime / CQLTestCommons.BASICTIMESTAMP + "s");

        assertTrue("Test passed", true);
    }
}

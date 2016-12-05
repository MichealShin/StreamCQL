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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.opereators.KafkaInputOperator;
import com.huawei.streaming.api.opereators.KafkaOutputOperator;
import com.huawei.streaming.cql.executor.PhysicalPlanLoader;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator;
import com.huawei.streaming.operator.inputstream.KafkaSourceOp;
import com.huawei.streaming.operator.outputstream.KafkaFunctionOp;

/**
 * Driver正常测试用例
 *
 */
public class LocalSubmitterTest
{
    private static final String TMP_TEST_DEPENDS = "/tmp/testDepends/";
    
    private static final Logger LOG = LoggerFactory.getLogger(LocalSubmitterTest.class);
    
    private static final String BASICPATH = File.separator + "submitter" + File.separator;
    
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

        LocalTaskCommons.startZookeeperServer();
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
    
    /**
     * 所有测试用例执行完毕之后执行的方法
     *
     */
    @AfterClass
    public static void tearDownAfterClass()
        throws Exception
    {
        LocalTaskCommons.stopZooKeeperServer();
        
        File tmpDir = new File(TMP_TEST_DEPENDS);
        LOG.info("delete tmp user dir {} in test case tear down", tmpDir.getAbsolutePath());
        removeTmpDir(tmpDir);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregate()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregate");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregate2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregate2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregate3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregate3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregate4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregate4");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregate5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregate5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregateFilter()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregateFilter");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregateFilter2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregateFilter2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testAggregateFilter3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "aggregateFilter3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCast2()
     throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "cast2");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testChineline()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "chineline");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCombine1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "combine1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testConfs()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "confs");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testDatasource()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testDatasource2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testDatasource3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDatasource4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDatasource5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource5");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDatasource6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDatasource7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "datasource7");
    }


    /**
     * 测试
     *
     */
    @Test
    public void testDate()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "date");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testFunctionIn()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "function_in");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testFunctionIn2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "functionIn");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testGroupby1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "groupby1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join4");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin8()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join8");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin9()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "join9");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testLocalSubmit()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "localSubmit");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testNotLike()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "not_like");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testPrevious1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "previous1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testRightJoin()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "right_join");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSelfJoin()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "selfjoin");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSelfJoin2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "selfjoin2");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testSimple()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "simple");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSubQuery6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "subQuery6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testWhereLengthSlide()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "where_length_slide");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testWinGroupToday()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "win_group_today");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testWinToday()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "win_today");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testBss()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "bss");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testConstAgg()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "ConstAgg");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testIsNotNull()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "isnotnull");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSplit()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSplit2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSplit3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSplit4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split4");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split6");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testSplit7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "split7");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testCase7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "case7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSimpleLexer3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "simpleLexer3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCase1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "ccc");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCurrentTime()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "currenttime");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testDistinctCount()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "distinctCount");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUDFDistinct()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "udfDistinct");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUdfWithProperties()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "udfWithProperties");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testTCPClientInput()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "TCPClientInput");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTCPClientOutput()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "TCPClientOutput");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testKafkaInput()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "kafkaInput");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testKafkaOutput()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "kafkaOutput");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testPipeLineHaving()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "pipelinehaving");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testToDate()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "todate");
    }
    
 
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort4");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort9()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timesort9");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testLengthSort1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "lengthsort1");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testLengthSort2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "lengthsort2");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testLengthSort3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "lengthsort3");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testRangeSort()
     throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "rangesort");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testDecimalAgg()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "decimalagg");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic2");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic3");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic4");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic6");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic7");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testMultiArithmetic8()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "multiArithmetic8");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator1");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator2()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator2");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator3()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator3");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator4()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator4");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator5()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator5");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator6()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator6");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator7()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator7");
    }
    
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator8()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator8");
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator9()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "userOperator9");
    }

    /**
     * 测试
     *
     */
    @Test
    public void testTimezon1()
        throws Exception
    {
        LocalTaskCommons.localSubmit(BASICPATH, "timezone1");
    }
}

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

import com.huawei.streaming.exception.ErrorCode;

/**
 * Driver正常测试用例
 *
 */
public class NavigateLocalSubmitterTest
{
    private static final String TMP_TEST_DEPENDS = "/tmp/testDepends/";
    
    private static final Logger LOG = LoggerFactory.getLogger(NavigateLocalSubmitterTest.class);
    
    private static final String BASICPATH = File.separator + "navigatesubmitter" + File.separator;
    
    /**
     * 初始化测试类之前要执行的初始化方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
        throws Exception
    {
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
    public void testJoin1()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join1", ErrorCode.SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin2()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join2", ErrorCode.SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin3()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join3", ErrorCode.SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin4()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join4", ErrorCode.SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin5()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join5", ErrorCode.SEMANTICANALYZE_UNSPPORTED_WINDOW_JOIN);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testJoin6()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "join6", ErrorCode.SEMANTICANALYZE_UNSPPORTED_WINDOW_JOIN);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testDateCaseWhen()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH,
            "datecasewhen",
            ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort1()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "timesort1", ErrorCode.WINDOW_UNSUPPORTED_PARAMETERS);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testTimeSort8()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "timesort8", ErrorCode.WINDOW_UNSUPPORTED_PARAMETERS);
    }


    /**
     * 测试
     *
     */
    @Test
    public void testLengthSort1()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "lengthsort", ErrorCode.SEMANTICANALYZE_PARSE_ERROR);
    }



    /**
     * 测试
     *
     */
    @Test
    public void testLengthSort2()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "lengthsort2", ErrorCode.SEMANTICANALYZE_PARSE_ERROR);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator1()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "userOperator1", ErrorCode.SEMANTICANALYZE_INVALID_INPUTSCHEMA);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator2()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "userOperator2", ErrorCode.SEMANTICANALYZE_INVALID_INPUTSCHEMA);
    }

    
    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator3()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "userOperator3", ErrorCode.SEMANTICANALYZE_INVALID_OUTPUTSCHEMA);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testUserOperator4()
     throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "userOperator4", ErrorCode.SEMANTICANALYZE_INVALID_OUTPUTSCHEMA);
    }
    /**
     * 测试
     *
     */
    @Test
    public void testInput()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "input", ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testOutput()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "output", ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testInput_submit()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "input_submit", ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR);
    }

    /**
     * 测试
     *
     */
    @Test
    public void testOutput_submit()
        throws Exception
    {
        LocalTaskCommons.navigageSubmit(BASICPATH, "output_submit", ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR);
    }
}

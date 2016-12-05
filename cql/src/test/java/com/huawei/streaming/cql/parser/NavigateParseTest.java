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
package com.huawei.streaming.cql.parser;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.CQLTestCommons;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;

/**
 * 语法解析异常测试
 *
 */
public class NavigateParseTest
{
    private static final Logger LOG = LoggerFactory.getLogger(NavigateParseTest.class);
    
    /**
     * 最基本的文件路径
     */
    private static final String BASICPATH = File.separator + "navigateparser" + File.separator;
    
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
        setDir();
        /*
         * 清空结果文件夹内容
         */
        CQLTestCommons.emptyDir(new File(resultPutDir));
    }
    
    /**
     * create inputstream 的测试类
     *
     */
    @Test
    public void testCreateInputStream()
        throws Exception
    {
        executeCase("createInput");
    }
    
    /**
     * 命令测试
     *
     */
    @Test
    public void testCommands()
        throws Exception
    {
        executeCase("commands");
    }
    
    /**
     * 命令测试
     *
     */
    @Test
    public void testSelects()
        throws Exception
    {
        executeCase("selects");
    }
    
    /**
     * 命令测试
     *
     */
    @Test
    public void testMultiInsert()
        throws Exception
    {
        executeCase("multiInsert");
    }
    
    private static void setDir()
    {
        String classPath = NavigateParseTest.class.getResource("/").getPath();
        
        try
        {
            classPath = URLDecoder.decode(classPath, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        
        inPutDir = classPath + BASICPATH + CQLTestCommons.INPUT_DIR_NAME + File.separator;
        
        outPutDir = classPath + BASICPATH + CQLTestCommons.OUTPUT_DIR_NAME + File.separator;
        
        resultPutDir = classPath + BASICPATH + CQLTestCommons.RESULT_DIR_NAME + File.separator;
    }
    
    private void executeCase(String caseName)
        throws Exception
    {
        long startTime = System.currentTimeMillis();
        ParseTaskUtil qt =
            new ParseTaskUtil(ParserFactory.createApplicationParser(), inPutDir + caseName
                + CQLTestCommons.INPUT_POSTFIX, outPutDir + caseName + CQLTestCommons.OUTPUT_POSTFIX, resultPutDir
                + caseName + CQLTestCommons.RESULT_POSTFIX);
        try
        {
            LOG.info("Begin query: " + caseName);
            qt.parseAndWriteWithParseException();
            if (!qt.compareResults())
            {
                LOG.error("test resutl doesn't same!");
                fail("test resutl doesn't same!");
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

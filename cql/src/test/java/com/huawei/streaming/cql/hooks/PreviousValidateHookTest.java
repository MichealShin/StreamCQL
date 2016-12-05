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

package com.huawei.streaming.cql.hooks;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectStatementContext;

/**
 * previous表达式验证钩子测试用例
 *
 */
public class PreviousValidateHookTest
{
    
    /**
     * 测试
     *
     */
    @Test
    public void testSelectClause1()
        throws Exception
    {
        String cql = "select previous(1,id)  from S1";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        hook.validate(ast);
        hook.preAnalyze(null, ast);
        assertTrue(true);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSelectClause2()
        throws Exception
    {
        boolean result = false;
        String cql = "select previous(1,id,name) as a  from S1";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        try
        {
            hook.validate(ast);
            hook.preAnalyze(null, ast);
        }
        catch (Exception e)
        {
            result = true;
        }
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testSelectClause3()
        throws Exception
    {
        boolean result = false;
        String cql = "select previous(1,a,b,c) as a  from S1";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        try
        {
            hook.validate(ast);
            hook.preAnalyze(null, ast);
        }
        catch (Exception e)
        {
            result = true;
        }
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition1()
        throws Exception
    {
        boolean result = false;
        String cql = "select count(id,previous(1,a,b)>10) as a from S1";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        try
        {
            hook.validate(ast);
            hook.preAnalyze(null, ast);
        }
        catch (Exception e)
        {
            result = true;
        }
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition2()
        throws Exception
    {
        boolean result = false;
        String cql = "select count(id,previous(1,a)>10) as a from S1";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        hook.validate(ast);
        hook.preAnalyze(null, ast);
        result = true;
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition3()
        throws Exception
    {
        boolean result = false;
        String cql = "select * from S1 where previous(1,id) > 10";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        hook.validate(ast);
        hook.preAnalyze(null, ast);
        result = true;
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition4()
        throws Exception
    {
        boolean result = false;
        String cql = "select * from S1 having previous(1,id) > 10";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        hook.validate(ast);
        hook.preAnalyze(null, ast);
        result = true;
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition5()
        throws Exception
    {
        boolean result = false;
        String cql = "select * from S1 having previous(1,id,name) > 10";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        try
        {
            hook.validate(ast);
            hook.preAnalyze(null, ast);
        }
        catch (Exception e)
        {
            result = true;
        }
        assertTrue(result);
    }
    
    /**
     * 测试
     *
     */
    @Test
    public void testCondition6()
        throws Exception
    {
        boolean result = false;
        String cql =
            "select * from S1 where previous(1,id,name) > 10 group by id order by id limit 10";
        IParser parser = ParserFactory.createApplicationParser();
        SelectStatementContext ast = (SelectStatementContext)parser.parse(cql);
        SemanticAnalyzeHook hook = new PreviousValidateHook();
        try
        {
            hook.validate(ast);
            hook.preAnalyze(null, ast);
        }
        catch (Exception e)
        {
            result = true;
        }
        assertTrue(result);
    }
}

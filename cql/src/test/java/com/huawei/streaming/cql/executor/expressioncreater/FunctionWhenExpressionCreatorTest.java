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

package com.huawei.streaming.cql.executor.expressioncreater;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.CaseSearchedExpression;

/**
 * when 表达式测试
 *
 */
public class FunctionWhenExpressionCreatorTest
{
    private static IParser parser = ParserFactory.createApplicationParser();
    
    private static List<Schema> schemas;
    
    private static Map<String, String> systemConfig = new HashMap<String, String>();
    
    /**
     * 初始化测试类之前要执行的初始化方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
        throws Exception
    {
        initSchema();
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateInstance()
        throws StreamingException
    {
        String like = "select case when id = '1' then 1 when id='2' then 2 when id='3' then 3 else 99 end from s1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(like), schemas);
        SelectAnalyzeContext selectContext = (SelectAnalyzeContext)analyzer.analyze();
        SelectClauseAnalyzeContext parseContext = selectContext.getSelectClauseContext();
        
        ExpressionDescribe desc = parseContext.getExpdes().get(0);
        assertTrue(desc.toString()
            .equals("case when (s1.id = '1') then 1 when (s1.id = '2') then 2 when (s1.id = '3') then 3 else 99 end "));
        
        CaseSearchedExpression likeExp =
            (CaseSearchedExpression)new FunctionWhenExpressionCreator().createInstance(desc, systemConfig);
        assertTrue(likeExp.getType() == Integer.class);
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testNotCreateInstance()
        throws StreamingException
    {
        String like = "select case when id = '1' then 1 when id='2' then 2 when id='3' then 3 end from s1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(like), schemas);
        SelectAnalyzeContext selectContext = (SelectAnalyzeContext)analyzer.analyze();
        SelectClauseAnalyzeContext parseContext = selectContext.getSelectClauseContext();
        
        ExpressionDescribe desc = parseContext.getExpdes().get(0);
        assertTrue(desc.toString()
            .equals("case when (s1.id = '1') then 1 when (s1.id = '2') then 2 when (s1.id = '3') then 3 end "));
        
        CaseSearchedExpression likeExp =
            (CaseSearchedExpression)new FunctionWhenExpressionCreator().createInstance(desc, systemConfig);
        assertTrue(likeExp.getType() == Integer.class);
    }
    
    private static void initSchema()
        throws SemanticAnalyzerException
    {
        schemas = new ArrayList<Schema>();
        Schema s1 = new Schema("S1");
        
        s1.addCol(new Column("id", String.class));
        s1.addCol(new Column("name", String.class));
        
        schemas.add(s1);
    }
    
}

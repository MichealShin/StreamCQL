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

package com.huawei.streaming.cql.semanticanalyzer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.ConstInTestCase;
import com.huawei.streaming.cql.Driver;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ConstExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionBetweenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionCaseExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionInExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionLikeExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionWhenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.NullExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.expression.ExpressionOperator;

/**
 * where子句语义分析测试用例
 *
 */
public class WhereSemanticAnalyzerTest
{
    private static IParser parser = ParserFactory.createApplicationParser();
    
    private static Driver driver = null;
    
    /**
     * 初始化测试类之前要执行的初始化方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
        throws Exception
    {
        if (DriverContext.getFunctions().get() == null)
        {
            driver = new Driver();
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
        if (driver != null)
        {
            driver.clean();
        }
    }
    
    /**
     * 测试等于表达式
     *
     */
    @Test
    public void testEquals()
        throws Exception
    {
        String sql = "select * from s1 where Id=1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试大于表达式
     *
     */
    @Test
    public void testGreaterthan()
        throws Exception
    {
        String sql = "select * from s1 where Id>1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试小于表达式
     *
     */
    @Test
    public void testLessthan()
        throws Exception
    {
        String sql = "select * from s1 " + "where Id<1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LESSTHAN));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试大于等于表达式
     *
     */
    @Test
    public void testGreaterEqual()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id>=1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN_EQUAL));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试小于等于表达式
     *
     */
    @Test
    public void testLessEqual()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id<=1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LESSTHAN_EQUAL));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试不等于表达式
     *
     */
    @Test
    public void testNotEqual1()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id!=1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.NOT_EQUAL));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试不等于表达式
     *
     */
    @Test
    public void testNotEqual2()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id<>1";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.NOT_EQUAL));
        
        PropertyValueExpressionDesc exp2 =
            (PropertyValueExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp2.getProperty().equals("id"));
        assertTrue((int)exp3.getConstValue() == 1);
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull1()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id is null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == true);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull2()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id=null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == true);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull3()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id==null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == true);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull4()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id!=null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == false);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull5()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id<>null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == false);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull6()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id is not null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == false);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getProperty().equals("id"));
    }
    
    /**
     * 测试空值表达式
     *
     */
    @Test
    public void testIsNull7()
        throws Exception
    {
        String sql = "select * from s1 where " + "tostring(id) is null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isNull() == true);
        
        FunctionExpressionDesc exp2 = (FunctionExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getFinfo().getName().equals("tostring"));
        PropertyValueExpressionDesc exp3 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        assertTrue(exp3.getProperty().equals("id"));
    }
    
    /**
     * 测试不等于表达式
     *
     */
    @Test
    public void testAnd()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id=1 and name = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LOGICAND));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        PropertyValueExpressionDesc exp4 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp4.getProperty().equals("id"));
        assertTrue(exp5.getConstValue().equals(1));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("name"));
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
        
    }
    
    /**
     * 测试不等于表达式
     *
     */
    @Test
    public void testOr()
        throws Exception
    {
        String sql = "select * from s1 where " + "Id=1 or id = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LOGICOR));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        PropertyValueExpressionDesc exp4 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp4.getProperty().equals("id"));
        assertTrue(exp5.getConstValue().equals(1));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
    }
    
    /**
     * 测试多表条件
     *
     */
    @Test
    public void testTwoSchmemaEquals()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.Id=S2.id or s2.id = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LOGICOR));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        PropertyValueExpressionDesc exp4 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp5 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp4.getProperty().equals("id"));
        assertTrue(exp5.getProperty().equals("id"));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority1()
        throws Exception
    {
        String sql = "select * from  s1,s2 on s1.id = s2.id where " + "( s1.Id + S2.id ) * s2.id = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.MULTIPLY));
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_2);
        
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp5 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp5.getProperty().equals("id"));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue(exp7.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority2()
        throws Exception
    {
        String sql = "select * from  s1,s2 on s1.id = s2.id where " + "s1.Id + (S2.id  + s2.id) = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_2);
        
        PropertyValueExpressionDesc exp4 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        BinaryExpressionDesc exp5 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp4.getProperty().equals("id"));
        assertTrue(exp5.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp5.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp5.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue(exp7.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority3()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.Id + S2.id + s2.id = 2";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_2);
        
        //s1.Id + S2.id + s2.id
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp5 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp5.getProperty().equals("id"));
        
        //s1.Id + S2.id
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue(exp7.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority4()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "( s1.id + s2.id ) is null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue(exp7.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority5()
        throws Exception
    {
        /*
         * 加号优先级高于is null
         */
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id + s2.id is null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(0);
        assertTrue(exp1.getExpression() instanceof BinaryExpressionDesc);
        BinaryExpressionDesc bexp = (BinaryExpressionDesc)exp1.getExpression();
        assertTrue(bexp.getBexpression().getType() == ExpressionOperator.ADD);
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority6()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "( s1.id + s2.id ) * 99 > 10";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.MULTIPLY));
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_10);
        
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue((int)exp5.getConstValue() == ConstInTestCase.I_99);
        
        PropertyValueExpressionDesc exp6 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp6.getProperty().equals("id"));
        assertTrue(exp7.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority7()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "(s1.id*(s1.id+s2.id)+s2.name) is null";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof NullExpressionDesc);
        
        NullExpressionDesc exp1 = (NullExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getExpression();
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp3 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.MULTIPLY));
        assertTrue(exp3.getProperty().equals("name"));
        
        PropertyValueExpressionDesc exp5 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        BinaryExpressionDesc exp6 = (BinaryExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp5.getProperty().equals("id"));
        assertTrue(exp6.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp7 =
            (PropertyValueExpressionDesc)exp6.getArgExpressions().get(ConstInTestCase.I_0);
        PropertyValueExpressionDesc exp8 =
            (PropertyValueExpressionDesc)exp6.getArgExpressions().get(ConstInTestCase.I_1);
        
        assertTrue(exp7.getProperty().equals("id"));
        assertTrue(exp8.getProperty().equals("id"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testPriority8()
        throws Exception
    {
        /*
         * > < 等逻辑表达式优先级低于加号
         */
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "(s1.id*(s1.id+s2.id)>s2.name) or (s1.id * s2.id + s2.id >= 10)";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.LOGICOR));
        //s1.id*(s1.id+s2.id)>s2.name
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        //s1.id * s2.id + s2.id >= 10
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN_EQUAL));
        
        //s1.id*(s1.id+s2.id)
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.MULTIPLY));
        
        //s1.id * s2.id + s2.id
        BinaryExpressionDesc exp5 = (BinaryExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        assertTrue(exp5.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //s1.id+s2.id
        BinaryExpressionDesc exp6 = (BinaryExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp6.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //s1.id * s2.id
        BinaryExpressionDesc exp7 = (BinaryExpressionDesc)exp5.getArgExpressions().get(ConstInTestCase.I_0);
        assertTrue(exp7.getBexpression().getType().equals(ExpressionOperator.MULTIPLY));
        
        ConstExpressionDesc exp10 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp10.getConstValue() == ConstInTestCase.I_10);
        
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testStar1()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "count(distinct *) > 10";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        
        FunctionExpressionDesc exp2 = (FunctionExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getFinfo().getName().equals("count"));
        assertTrue(exp2.isDistinct() == true);
        assertTrue(exp2.isSelectStar() == true);
        assertTrue(exp2.getArgExpressions().size() == 0);
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_10);
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testStar2()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "count(*) > 10";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        
        FunctionExpressionDesc exp2 = (FunctionExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getFinfo().getName().equals("count"));
        assertTrue(exp2.isDistinct() == false);
        assertTrue(exp2.isSelectStar() == true);
        assertTrue(exp2.getArgExpressions().size() == 0);
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_10);
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testStar3()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "sum(*) > 10";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof BinaryExpressionDesc);
        
        BinaryExpressionDesc exp1 = (BinaryExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        
        FunctionExpressionDesc exp2 = (FunctionExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp2.getFinfo().getName().equals("sum"));
        assertTrue(exp2.isDistinct() == false);
        assertTrue(exp2.isSelectStar() == true);
        assertTrue(exp2.getArgExpressions().size() == 0);
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_10);
    }
    
    /**
     * 测试 in
     *
     */
    @Test
    public void testFunctionIn1()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id in ('1','2','3')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionInExpressionDesc);
        
        FunctionInExpressionDesc exp1 = (FunctionInExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        
        assertTrue(exp1.getInProperty() instanceof PropertyValueExpressionDesc);
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getInProperty();
        
        assertTrue(exp2.getProperty().equals("id"));
        
        List<ExpressionDescribe> exp3 = exp1.getArgs();
        
        assertTrue(exp3.size() == ConstInTestCase.I_3);
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_0)).getConstValue().toString().equals("1"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_1)).getConstValue().toString().equals("2"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_2)).getConstValue().toString().equals("3"));
    }
    
    /**
     * 测试优先级
     *
     */
    @Test
    public void testFunctionIn2()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id not in ('1','2','3')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionInExpressionDesc);
        
        FunctionInExpressionDesc exp1 = (FunctionInExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isContainsNotExpression() == true);
        
        assertTrue(exp1.getInProperty() instanceof PropertyValueExpressionDesc);
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getInProperty();
        
        assertTrue(exp2.getProperty().equals("id"));
        
        List<ExpressionDescribe> exp3 = exp1.getArgs();
        
        assertTrue(exp3.size() == ConstInTestCase.I_3);
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_0)).getConstValue().toString().equals("1"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_1)).getConstValue().toString().equals("2"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_2)).getConstValue().toString().equals("3"));
    }
    
    /**
     * 测试 in
     *
     */
    @Test
    public void testFunctionIn3()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id in (1,'2','3')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionInExpressionDesc);
        
        FunctionInExpressionDesc exp1 = (FunctionInExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        
        assertTrue(exp1.getInProperty() instanceof PropertyValueExpressionDesc);
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getInProperty();
        
        assertTrue(exp2.getProperty().equals("id"));
        
        List<ExpressionDescribe> exp3 = exp1.getArgs();
        
        assertTrue(exp3.size() == ConstInTestCase.I_3);
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_0)).getConstValue().toString().equals("1"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_1)).getConstValue().toString().equals("2"));
        assertTrue(((ConstExpressionDesc)exp3.get(ConstInTestCase.I_2)).getConstValue().toString().equals("3"));
    }
    
    /**
     * 测试函数between
     *
     */
    @Test
    public void testFunctionBetween1()
        throws Exception
    {
        /*
         * 函数的优先级低于算数表达式
         */
        String sql = "select * from s1 where " + "id + 100 between (150 + -50) AND (150 + 50)";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionBetweenExpressionDesc);
        
        FunctionBetweenExpressionDesc exp1 =
            (FunctionBetweenExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isContainsNotExpression() == false);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getBetweenProperty();
        
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp22 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp23 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp22.getProperty().equals("id"));
        assertTrue((int)exp23.getConstValue() == ConstInTestCase.I_100);
        
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getLeftExpression();
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getRightExpression();
        
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //        analyzer.analyze(parser.parse("id + 100 between (150 + -50) AND (150 + 50)"));
        ConstExpressionDesc exp31 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp32 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp31.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp32.getConstValue() == ConstInTestCase.I_N50);
        
        ConstExpressionDesc exp41 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp41.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp42.getConstValue() == ConstInTestCase.I_50);
        
    }
    
    /**
     * 测试函数between2
     *
     */
    @Test
    public void testFunctionBetween2()
        throws Exception
    {
        String sql = "select * from s1 where " + "(id + 100) between (150 + -50) AND (150 + 50)";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionBetweenExpressionDesc);
        
        FunctionBetweenExpressionDesc exp1 =
            (FunctionBetweenExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isContainsNotExpression() == false);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getBetweenProperty();
        
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp22 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp23 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp22.getProperty().equals("id"));
        assertTrue((int)exp23.getConstValue() == ConstInTestCase.I_100);
        
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getLeftExpression();
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getRightExpression();
        
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //        analyzer.analyze(parser.parse("id + 100 between (150 + -50) AND (150 + 50)"));
        ConstExpressionDesc exp31 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp32 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp31.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp32.getConstValue() == ConstInTestCase.I_N50);
        
        ConstExpressionDesc exp41 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp41.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp42.getConstValue() == ConstInTestCase.I_50);
    }
    
    /**
     * 测试函数between2
     *
     */
    @Test
    public void testFunctionBetween3()
        throws Exception
    {
        String sql = "select * from s1 where " + "id + 100 not between (150 + -50) AND (150 + 50)";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionBetweenExpressionDesc);
        
        FunctionBetweenExpressionDesc exp1 =
            (FunctionBetweenExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isContainsNotExpression() == true);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getBetweenProperty();
        
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp22 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp23 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp22.getProperty().equals("id"));
        assertTrue((int)exp23.getConstValue() == ConstInTestCase.I_100);
        
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getLeftExpression();
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getRightExpression();
        
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //        analyzer.analyze(parser.parse("id + 100 between (150 + -50) AND (150 + 50)"));
        ConstExpressionDesc exp31 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp32 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp31.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp32.getConstValue() == ConstInTestCase.I_N50);
        
        ConstExpressionDesc exp41 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp41.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp42.getConstValue() == ConstInTestCase.I_50);
    }
    
    /**
     * 测试函数between2
     *
     */
    @Test
    public void testFunctionBetween4()
        throws Exception
    {
        String sql = "select * from s1 where " + "(id + 100) not between (150 + -50) AND (150 + 50)";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionBetweenExpressionDesc);
        
        FunctionBetweenExpressionDesc exp1 =
            (FunctionBetweenExpressionDesc)parseContext.getExpdes().get(ConstInTestCase.I_0);
        assertTrue(exp1.isContainsNotExpression() == true);
        
        BinaryExpressionDesc exp2 = (BinaryExpressionDesc)exp1.getBetweenProperty();
        
        assertTrue(exp2.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        PropertyValueExpressionDesc exp22 =
            (PropertyValueExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp23 = (ConstExpressionDesc)exp2.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp22.getProperty().equals("id"));
        assertTrue((int)exp23.getConstValue() == ConstInTestCase.I_100);
        
        BinaryExpressionDesc exp3 = (BinaryExpressionDesc)exp1.getLeftExpression();
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getRightExpression();
        
        assertTrue(exp3.getBexpression().getType().equals(ExpressionOperator.ADD));
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.ADD));
        
        //        analyzer.analyze(parser.parse("id + 100 between (150 + -50) AND (150 + 50)"));
        ConstExpressionDesc exp31 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp32 = (ConstExpressionDesc)exp3.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp31.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp32.getConstValue() == ConstInTestCase.I_N50);
        
        ConstExpressionDesc exp41 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue((int)exp41.getConstValue() == ConstInTestCase.I_150);
        assertTrue((int)exp42.getConstValue() == ConstInTestCase.I_50);
    }
    
    /**
     * 测试函数like
     *
     */
    @Test
    public void testFunctionLike1()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id like '%xx%'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionLikeExpressionDesc);
        FunctionLikeExpressionDesc fexp = (FunctionLikeExpressionDesc)parseContext.getExpdes().get(0);
        PropertyValueExpressionDesc exp1 = (PropertyValueExpressionDesc)fexp.getLikeProperty();
        assertTrue(exp1.getProperty().equals("id"));
        
        ConstExpressionDesc exp2 = (ConstExpressionDesc)fexp.getLikeStringExpression();
        assertTrue(exp2.getConstValue().toString().equals("%xx%"));
    }
    
    /**
     * 测试函数like
     *
     */
    @Test
    public void testFunctionLike2()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id like 'xx%'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionLikeExpressionDesc);
        FunctionLikeExpressionDesc fexp = (FunctionLikeExpressionDesc)parseContext.getExpdes().get(0);
        PropertyValueExpressionDesc exp1 = (PropertyValueExpressionDesc)fexp.getLikeProperty();
        assertTrue(exp1.getProperty().equals("id"));
        
        ConstExpressionDesc exp2 = (ConstExpressionDesc)fexp.getLikeStringExpression();
        assertTrue(exp2.getConstValue().toString().equals("xx%"));
    }
    
    /**
     * 测试函数like
     *
     */
    @Test
    public void testFunctionLike3()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "s1.id like 'xx?'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionLikeExpressionDesc);
        FunctionLikeExpressionDesc fexp = (FunctionLikeExpressionDesc)parseContext.getExpdes().get(0);
        PropertyValueExpressionDesc exp1 = (PropertyValueExpressionDesc)fexp.getLikeProperty();
        assertTrue(exp1.getProperty().equals("id"));
        
        ConstExpressionDesc exp2 = (ConstExpressionDesc)fexp.getLikeStringExpression();
        assertTrue(exp2.getConstValue().toString().equals("xx?"));
    }
    
    /**
     * 测试函数case
     *
     */
    @Test
    public void testFunctionCase1()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case s1.id when '1' then 1 when '2' then 2 when '3' then 3 else 99 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionCaseExpressionDesc);
        FunctionCaseExpressionDesc exp1 = (FunctionCaseExpressionDesc)parseContext.getExpdes().get(0);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getCasePropertyExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_99);
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue(exp5.getConstValue().equals(1));
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue(exp6.getConstValue().toString().equals("2"));
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp8 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getConstValue().toString().equals("3"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_3);
        
        assertTrue(exp1.getElseExpression() instanceof ConstExpressionDesc);
        ConstExpressionDesc exp10 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp10.getConstValue() == ConstInTestCase.I_99);
    }
    
    /**
     * 测试函数case
     *
     */
    @Test
    public void testFunctionCase2()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case s1.id when 1 then '1' when '2' then 2 when '3' then 3 else 99 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionCaseExpressionDesc);
        FunctionCaseExpressionDesc exp1 = (FunctionCaseExpressionDesc)parseContext.getExpdes().get(0);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getCasePropertyExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_99);
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue(exp5.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue(exp6.getConstValue().toString().equals("2"));
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp8 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getConstValue().toString().equals("3"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_3);
        
        assertTrue(exp1.getElseExpression() instanceof ConstExpressionDesc);
        ConstExpressionDesc exp10 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp10.getConstValue() == ConstInTestCase.I_99);
    }
    
    /**
     * 测试函数case
     *
     */
    @Test
    public void testFunctionCase3()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case s1.id when 1 then 'true' when 2 then 'false' when '3' then 3 else 99 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionCaseExpressionDesc);
        FunctionCaseExpressionDesc exp1 = (FunctionCaseExpressionDesc)parseContext.getExpdes().get(0);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getCasePropertyExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_99);
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue(exp5.getConstValue().toString().equals("true"));
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue((int)exp6.getConstValue() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue(exp7.getConstValue().toString().equals("false"));
        
        ConstExpressionDesc exp8 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getConstValue().toString().equals("3"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_3);
        
        assertTrue(exp1.getElseExpression() instanceof ConstExpressionDesc);
        ConstExpressionDesc exp10 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp10.getConstValue() == ConstInTestCase.I_99);
    }
    
    /**
     * 测试函数case
     *
     */
    @Test
    public void testFunctionCase4()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case s1.id when 1 then '1' when 2 then '2' when '3' then 3 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionCaseExpressionDesc);
        FunctionCaseExpressionDesc exp1 = (FunctionCaseExpressionDesc)parseContext.getExpdes().get(0);
        
        PropertyValueExpressionDesc exp2 = (PropertyValueExpressionDesc)exp1.getCasePropertyExpression();
        assertTrue(exp2.getProperty().equals("id"));
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue((int)exp4.getConstValue() == 1);
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue(exp5.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue((int)exp6.getConstValue() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue(exp7.getConstValue().toString().equals("2"));
        
        ConstExpressionDesc exp8 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getConstValue().toString().equals("3"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_3);
        
        assertTrue(exp1.getElseExpression() == null);
    }
    
    /**
     * 测试函数case
     *
     */
    @Test
    public void testFunctionCase5()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case count(s1.id) when '1' then 1 when '2' then 2  when '3' then 3 else '0' end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionCaseExpressionDesc);
        FunctionCaseExpressionDesc exp1 = (FunctionCaseExpressionDesc)parseContext.getExpdes().get(0);
        
        FunctionExpressionDesc exp2 = (FunctionExpressionDesc)exp1.getCasePropertyExpression();
        assertTrue(exp2.getFinfo().getName().equals("count"));
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue(exp3.getConstValue().toString().equals("0"));
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getConstValue().toString().equals("1"));
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue((int)exp5.getConstValue() == 1);
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue(exp6.getConstValue().toString().equals("2"));
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp8 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getConstValue().toString().equals("3"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_3);
    }
    
    /***************************** case 测试用例
     * case when s1.id=true then 0 else 1 end
     * case when unf(s1.id)=true then 0 else 1 end
     * case when true then true when false then false end
     */
    
    /**
     * 测试函数when
     *
     */
    @Test
    public void testFunctionWhen1()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where "
                + "case when s1.id<'90' then 2 when s1.id = '90' then 3 when s1.id > '90'  then 5 else 4 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionWhenExpressionDesc);
        FunctionWhenExpressionDesc exp1 = (FunctionWhenExpressionDesc)parseContext.getExpdes().get(0);
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == ConstInTestCase.I_4);
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_3);
        
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.LESSTHAN));
        PropertyValueExpressionDesc exp41 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp41.getProperty().equals("id"));
        assertTrue(exp42.getConstValue().toString().equals("90"));
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue((int)exp5.getConstValue() == ConstInTestCase.I_2);
        
        BinaryExpressionDesc exp6 = (BinaryExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertTrue(exp6.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        PropertyValueExpressionDesc exp61 =
            (PropertyValueExpressionDesc)exp6.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp62 = (ConstExpressionDesc)exp6.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp61.getProperty().equals("id"));
        assertTrue(exp62.getConstValue().toString().equals("90"));
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertTrue((int)exp7.getConstValue() == ConstInTestCase.I_3);
        
        BinaryExpressionDesc exp8 = (BinaryExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getFirst();
        assertTrue(exp8.getBexpression().getType().equals(ExpressionOperator.GREATERTHAN));
        PropertyValueExpressionDesc exp81 =
            (PropertyValueExpressionDesc)exp8.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp82 = (ConstExpressionDesc)exp6.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp81.getProperty().equals("id"));
        assertTrue(exp82.getConstValue().toString().equals("90"));
        
        ConstExpressionDesc exp9 = (ConstExpressionDesc)exp1.getWhenThens().get(ConstInTestCase.I_2).getSecond();
        assertTrue((int)exp9.getConstValue() == ConstInTestCase.I_5);
    }
    
    /**
     * 测试函数when
     *
     */
    @Test
    public void testFunctionWhen2()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "case when s1.id=true then 0 else 1 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionWhenExpressionDesc);
        FunctionWhenExpressionDesc exp1 = (FunctionWhenExpressionDesc)parseContext.getExpdes().get(0);
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == 1);
        
        assertTrue(exp1.getWhenThens().size() == 1);
        
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        PropertyValueExpressionDesc exp41 =
            (PropertyValueExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp41.getProperty().equals("id"));
        assertTrue((Boolean)exp42.getConstValue());
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue((int)exp5.getConstValue() == 0);
    }
    
    /**
     * 测试函数when
     *
     */
    @Test
    public void testFunctionWhen3()
        throws Exception
    {
        String sql = "select * from s1,s2 on s1.id = s2.id where " + "case when count(s1.id)=true then 0 else 1 end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionWhenExpressionDesc);
        FunctionWhenExpressionDesc exp1 = (FunctionWhenExpressionDesc)parseContext.getExpdes().get(0);
        
        ConstExpressionDesc exp3 = (ConstExpressionDesc)exp1.getElseExpression();
        assertTrue((int)exp3.getConstValue() == 1);
        
        assertTrue(exp1.getWhenThens().size() == 1);
        
        BinaryExpressionDesc exp4 = (BinaryExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue(exp4.getBexpression().getType().equals(ExpressionOperator.EQUAL));
        FunctionExpressionDesc exp41 = (FunctionExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_0);
        ConstExpressionDesc exp42 = (ConstExpressionDesc)exp4.getArgExpressions().get(ConstInTestCase.I_1);
        assertTrue(exp41.getFinfo().getName().equals("count"));
        assertTrue((Boolean)exp42.getConstValue());
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue((int)exp5.getConstValue() == 0);
    }
    
    /**
     * 测试函数when
     *
     */
    @Test
    public void testFunctionWhen4()
        throws Exception
    {
        String sql =
            "select * from s1,s2 on s1.id = s2.id where " + "case when true then true when false then false end";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchemas());
        SelectAnalyzeContext analyzeConext = (SelectAnalyzeContext)analyzer.analyze();
        FilterClauseAnalzyeContext parseContext = analyzeConext.getWhereClauseContext();
        
        assertTrue(parseContext.getExpdes().size() == ConstInTestCase.I_1);
        assertTrue(parseContext.getExpdes().get(0) instanceof FunctionWhenExpressionDesc);
        FunctionWhenExpressionDesc exp1 = (FunctionWhenExpressionDesc)parseContext.getExpdes().get(0);
        
        assertTrue(exp1.getWhenThens().size() == ConstInTestCase.I_2);
        
        ConstExpressionDesc exp4 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getFirst();
        assertTrue((Boolean)exp4.getConstValue());
        assertTrue((Boolean)exp4.getConstValue());
        
        ConstExpressionDesc exp5 = (ConstExpressionDesc)exp1.getWhenThens().get(0).getSecond();
        assertTrue((Boolean)exp5.getConstValue());
        
        ConstExpressionDesc exp6 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getFirst();
        assertFalse((Boolean)exp6.getConstValue());
        
        ConstExpressionDesc exp7 = (ConstExpressionDesc)exp1.getWhenThens().get(1).getSecond();
        assertFalse((Boolean)exp7.getConstValue());
    }
    
    private List<Schema> initSchemas()
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = new ArrayList<Schema>();
        Schema s1 = new Schema("S1");
        Schema s2 = new Schema("S2");
        
        s1.addCol(new Column("id", String.class));
        s1.addCol(new Column("name", String.class));
        
        s2.addCol(new Column("id", String.class));
        s2.addCol(new Column("name", String.class));
        
        schemas.add(s1);
        schemas.add(s2);
        return schemas;
    }
    
    private List<Schema> initSchema()
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = new ArrayList<Schema>();
        Schema s1 = new Schema("S1");
        
        s1.addCol(new Column("id", String.class));
        s1.addCol(new Column("name", String.class));
        
        schemas.add(s1);
        return schemas;
    }
    
}

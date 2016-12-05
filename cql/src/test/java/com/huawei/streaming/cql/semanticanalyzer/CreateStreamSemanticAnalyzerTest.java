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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;

/**
 * create stream test case
 *
 */
public class CreateStreamSemanticAnalyzerTest
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
     * 测试用例
     *
     */
    @Test
    public void testCreateInputStream1()
        throws Exception
    {
        String sql =
            "CREATE INPUT STREAM S (id INT,Name STRING) "
                + "SERDE 'com.huawei.streaming.serde.CSVSerDe'  properties ('ip' = 'localhost') "
                + "SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp' PROPERTIES ('path' = '/local')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getDeserializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getSerializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().get("ip").equals("localhost"));
        assertTrue(analyzeConext.getRecordReaderClassName().equals("com.huawei.streaming.operator.inputstream.KafkaSourceOp"));
        assertTrue(analyzeConext.getReadWriterProperties().get("path").equals("/local"));
        
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateInputStream2()
        throws Exception
    {
        String sql =
            "CREATE INPUT STREAM S (id long,name float) " + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp' PROPERTIES ('path' = '/local')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Long.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(Float.class.getName()));
        assertTrue(analyzeConext.getDeserializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getSerializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordReaderClassName().equals("com.huawei.streaming.operator.inputstream.KafkaSourceOp"));
        assertTrue(analyzeConext.getReadWriterProperties().get("path").equals("/local"));
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateInputStream3()
        throws Exception
    {
        String sql =
            "CREATE INPUT STREAM S (id double,name boolean) "
                + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Double.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(Boolean.class.getName()));
        assertTrue(analyzeConext.getDeserializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getSerializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordReaderClassName().equals("com.huawei.streaming.operator.inputstream.KafkaSourceOp"));
        assertTrue(analyzeConext.getReadWriterProperties().size() == 0);
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateInputStream4()
        throws Exception
    {
        String sql =
            "CREATE INPUT STREAM S (id INT,name STRING comment 'user name') COMMENT 'this is stream comment'"
                + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getDeserializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getSerializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordReaderClassName().equals("com.huawei.streaming.operator.inputstream.KafkaSourceOp"));
        assertTrue(analyzeConext.getReadWriterProperties().size() == 0);
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateOutputStream1()
        throws Exception
    {
        String sql =
            "CREATE output STREAM S (id INT,Name STRING) "
                + "SERDE 'com.huawei.streaming.serde.CSVSerDe'  properties ('ip' = 'localhost') "
                + "SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp' PROPERTIES ('path' = '/local')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getSerializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getDeserializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().get("ip").equals("localhost"));
        assertTrue(analyzeConext.getRecordWriterClassName().equals("com.huawei.streaming.operator.outputstream.KafkaFunctionOp"));
        assertTrue(analyzeConext.getReadWriterProperties().get("path").equals("/local"));
        
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateOutputStream2()
        throws Exception
    {
        String sql =
            "CREATE output STREAM S (id INT,name STRING) " + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp' PROPERTIES ('path' = '/local')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getSerializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getDeserializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordWriterClassName().equals("com.huawei.streaming.operator.outputstream.KafkaFunctionOp"));
        assertTrue(analyzeConext.getReadWriterProperties().get("path").equals("/local"));
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateOutputStream3()
        throws Exception
    {
        String sql =
            "CREATE output STREAM S (id INT,name STRING) " + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getSerializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getDeserializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordWriterClassName().equals("com.huawei.streaming.operator.outputstream.KafkaFunctionOp"));
        assertTrue(analyzeConext.getReadWriterProperties().size() == 0);
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testCreateOutputStream4()
        throws Exception
    {
        String sql =
            "CREATE output STREAM S (id INT,name STRING comment 'user name') COMMENT 'this is stream comment'"
                + "SERDE 'com.huawei.streaming.serde.CSVSerDe' "
                + "SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        CreateStreamAnalyzeContext analyzeConext = (CreateStreamAnalyzeContext)analyzer.analyze();
        
        assertTrue(analyzeConext.getStreamName().equals("s"));
        assertTrue(analyzeConext.getSchema().getId().equals("s"));
        assertTrue(analyzeConext.getSchema().getCols().size() == ConstInTestCase.I_2);
        assertTrue(analyzeConext.getSchema().getCols().get(0).getName().equals("id"));
        assertTrue(analyzeConext.getSchema().getCols().get(0).getType().equals(Integer.class.getName()));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getName().equals("name"));
        assertTrue(analyzeConext.getSchema().getCols().get(1).getType().equals(String.class.getName()));
        assertTrue(analyzeConext.getSerializerClassName().equals("com.huawei.streaming.serde.CSVSerDe"));
        assertTrue(analyzeConext.getDeserializerClassName() == null);
        assertTrue(analyzeConext.getSerDeProperties().size() == 0);
        assertTrue(analyzeConext.getRecordWriterClassName().equals("com.huawei.streaming.operator.outputstream.KafkaFunctionOp"));
        assertTrue(analyzeConext.getReadWriterProperties().size() == 0);
    }
    
    /**
     * 测试用例
     *
     */
    @Test
    public void testPipeStream1()
        throws Exception
    {
        String sql = "CREATE STREAM S (id INT,name STRING comment 'user name')";
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(sql), initSchema());
        try
        {
            analyzer.analyze();
        }
        catch (Exception e)
        {
            assertTrue(e instanceof SemanticAnalyzerException);
        }
    }
    
    private List<Schema> initSchema()
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = new ArrayList<Schema>();
        Schema s1 = new Schema("S");
        
        s1.addCol(new Column("id", String.class));
        s1.addCol(new Column("name", String.class));
        schemas.add(s1);
        return schemas;
    }
    
}

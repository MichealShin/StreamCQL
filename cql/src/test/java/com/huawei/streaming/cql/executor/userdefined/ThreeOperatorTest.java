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
package com.huawei.streaming.cql.executor.userdefined;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.opereators.FunctionStreamOperator;
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.ConstInTestCase;
import com.huawei.streaming.cql.LocalTaskCommons;

/**
 * 用户自定义算子测试，三个算子都是functionstream
 *
 */
public class ThreeOperatorTest
{
    private static final String BASICPATH = File.separator + "executor" + File.separator + "userdefined"
        + File.separator;
    
    /**
     * 写入测试
     *
     */
    @Test
    public void writerTest()
        throws Exception
    {
        String serOutFileName = "threeOperator.out.xml";
        String serResultFileName = "threeOperator.res.xml";
        PhysicalPlan plan = create();
        
        LocalTaskCommons.write(BASICPATH, serResultFileName, create().getApploication());
        boolean compareResult = LocalTaskCommons.compare(BASICPATH, serResultFileName, serOutFileName);
        assertTrue(compareResult);
        
        LocalTaskCommons.localSubmit(plan.getApploication());
    }
    
    private PhysicalPlan create()
    {
        Application app = new Application(this.getClass().getSimpleName());
        app.setConfs(LocalTaskCommons.createLocalConfs());
        app.setOperators(createOperators());
        app.setSchemas(createSchemas());
        app.setOpTransition(createTransitions(app.getOperators(), app.getSchemas()));
        PhysicalPlan plan = new PhysicalPlan();
        plan.setApploication(app);
        return plan;
    }
    
    private List<Schema> createSchemas()
    {
        List<Column> cols1 = new ArrayList<Column>();
        cols1.add(new Column("msisdn", String.class));
        cols1.add(new Column("host", String.class));
        cols1.add(new Column("CaseID", String.class));
        
        List<Column> cols2 = new ArrayList<Column>();
        cols2.add(new Column("msisdn", String.class));
        cols2.add(new Column("host", String.class));
        cols2.add(new Column("CaseID", String.class));
        
        Schema schema1 = new Schema("inputSchema");
        schema1.setCols(cols1);
        
        Schema schema2 = new Schema("outputSchema");
        schema2.setCols(cols2);
        
        List<Schema> results = new ArrayList<Schema>();
        results.add(schema1);
        results.add(schema2);
        
        return results;
    }
    
    private List<Operator> createOperators()
    {
        List<Operator> operators = Lists.newArrayList();
        operators.add(createInputStream());
        operators.add(createOutputStream());
        operators.add(createFilterStream());
        
        return operators;
    }
    
    private static Operator createInputStream()
    {
        TreeMap<String, String> config = Maps.newTreeMap();
        config.put("port", "7999");
        config.put("fixlength", "966");
        
        InputStreamOperator operator = new InputStreamOperator("input", 1);
        operator.setArgs(config);
        operator.setRecordReaderClassName(WebSpout.class.getName());
        return operator;
    }
    
    private static Operator createOutputStream()
    {
        TreeMap<String, String> config = Maps.newTreeMap();
        
        OutputStreamOperator operator = new OutputStreamOperator("output", 1);
        operator.setArgs(config);
        operator.setRecordWriterClassName(WebOutputStream.class.getName());
        return operator;
    }
    
    private Operator createFilterStream()
    {
        TreeMap<String, String> config = Maps.newTreeMap();
        FunctionStreamOperator operator = new FunctionStreamOperator("Filter", 1);
        operator.setArgs(config);
        operator.setOperatorClass(WebFilterStream.class.getName());
        operator.setInputSchema(createSchemas().get(ConstInTestCase.I_0));
        operator.setOutputSchema(createSchemas().get(ConstInTestCase.I_1));
        return operator;
    }
    
    private List<OperatorTransition> createTransitions(List<Operator> ops, List<Schema> schemas)
    {
        List<OperatorTransition> results = new ArrayList<OperatorTransition>();
        results.add(new OperatorTransition("inputStream", ops.get(ConstInTestCase.I_0), ops.get(ConstInTestCase.I_2),
            DistributeType.SHUFFLE, null, schemas.get(ConstInTestCase.I_0)));
        results.add(new OperatorTransition("filterStream", ops.get(ConstInTestCase.I_2), ops.get(ConstInTestCase.I_1),
            DistributeType.SHUFFLE, null, schemas.get(ConstInTestCase.I_0)));
        
        return results;
    }
    
}

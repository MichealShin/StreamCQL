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

package com.huawei.streaming.datasource;

import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import static org.junit.Assert.fail;

/**
 * 数据源测试用例
 * 
 */
public class RDBDataSourceTest
{
    
    /**
     * 测试用例
     */
    @Test
    public void testEvaluateQuery()
        throws StreamingException
    {
        DataSourceContainer datasource = createDataSource();
        Map<String, Object> cqlresults = prepareCQLArgResults(datasource);
        try
        {
            datasource.initialize();
            datasource.evaluate(cqlresults);
            fail("failed to evaluate datasource");
        }
        catch (StreamingRuntimeException e)
        {
            assertTrue(true);
        }
        finally
        {
            datasource.destroy();
        }
    }
    
    private DataSourceContainer createDataSource()
        throws StreamingException
    {
        StreamingConfig config = prepareConfig();
        TupleEventType eventType = prepareEventType();
        String[] queryArguments = prepareQueryArguments();

        RDBDataSource iDataSource = new PreStatementRDBDataSource();
        iDataSource.setSchema(eventType);
        iDataSource.setConfig(config);

        DataSourceContainer dataSource = new DataSourceContainer();
        dataSource.setQueryArguments(queryArguments);
        dataSource.setDataSource(iDataSource);

        return dataSource;
    }
    
    private Map<String, Object> prepareCQLArgResults(DataSourceContainer datasource)
    {
        String[] cqlArgs = datasource.getCQLQueryArguments();
        
        Map<String, Object> cqlresults = Maps.newHashMap();
        for (String arg : cqlArgs)
        {
            cqlresults.put(arg, 1);
        }
        return cqlresults;
    }
    
    private String[] prepareQueryArguments()
    {
        String[] queryArguments = new String[2];
        queryArguments[0] = "select rid as id,rname,rtype from rdbtable where id =?";
        queryArguments[1] = "s.id";
        return queryArguments;
    }
    
    private TupleEventType prepareEventType()
    {
        List<Attribute> schema = Lists.newArrayList();
        schema.add(new Attribute(Integer.class, "id"));
        schema.add(new Attribute(String.class, "name"));
        schema.add(new Attribute(Integer.class, "type"));
        
        TupleEventType eventType = new TupleEventType("rdbdatasource", schema);
        return eventType;
    }
    
    private StreamingConfig prepareConfig()
    {
        StreamingConfig config = new StreamingConfig();
        config.put("datasource.rdb.driver", "org.postgresql.Driver");
        config.put("datasource.rdb.url", "jdbc:postgresql://158.1.130.10:1521/streaming");
        config.put("datasource.rdb.username", "root");
        config.put("datasource.rdb.password", "root");
        return config;
    }
}

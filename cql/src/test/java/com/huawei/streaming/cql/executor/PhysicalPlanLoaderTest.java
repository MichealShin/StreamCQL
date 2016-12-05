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

package com.huawei.streaming.cql.executor;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.junit.BeforeClass;
import org.junit.Test;

import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.cql.ConstInTestCase;
import com.huawei.streaming.cql.exception.ExecutorException;

/**
 * 物理执行计划加载器
 *
 */
public class PhysicalPlanLoaderTest
{
    private static final String BASICPATH = File.separator + "executor" + File.separator + "planserde" + File.separator;
    
    private static String serdeDir = null;
    
    private static String deSerFileName = "deser.xml";
    
    /**
     * 类初始化之前要执行的方法
     *
     */
    @BeforeClass
    public static void setUpBeforeClass()
        throws Exception
    {
        /*
         * 初始化反序列化文件路径
         */
        setDir();
    }
    
    /**
     * Test method for {@link com.huawei.streaming.cql.executor.PhysicalPlanLoader#load(java.lang.String)}.
     */
    @Test
    public void testLoad() throws ExecutorException
    {
        /*
         * 测试点：
         * 1、测试attribute标签能不能下放到属性中： 可以
         * 2、测试attribute标签和属性同时存在，会采用哪一个: 会冲突，属性重复，所以只能两个里面选择一个
         * 3、测试如果少了标签还能不能用？:可以
         * 4、测试如果多了没有定义的标签，还能不能用:可以，但是不会解析到对象中去，会自动忽略该属性
         * 7、测试CDATA ： 支持
         * 8、中文测试:支持
         */
        PhysicalPlan plan = PhysicalPlanLoader.load(serdeDir + deSerFileName);
        assertTrue(plan.getApploication().getSchemas().get(ConstInTestCase.I_0).getId().equals("s01"));
        assertTrue(plan.getApploication().getSchemas().get(ConstInTestCase.I_1).getId() == null);
        assertTrue(plan.getApploication().getSchemas().get(ConstInTestCase.I_2).getId().equals("unkown"));
        
        assertTrue(plan.getApploication().getOperators().size() == ConstInTestCase.I_6);
        for (Operator op : plan.getApploication().getOperators())
        {
            if (op instanceof JoinFunctionOperator)
            {
                JoinFunctionOperator jop = (JoinFunctionOperator)op;
                assertTrue(jop.getFilterAfterJoinExpression().trim().equals("name4.c1<10"));
            }
        }
        assertTrue(plan.getApploication()
            .getSchemas()
            .get(ConstInTestCase.I_2)
            .getCols()
            .get(ConstInTestCase.I_0)
            .getName()
            .equals("类型"));
        System.out.println(plan.getApploication().getOperators().get(ConstInTestCase.I_3).getClass());
        
    }
    
    /**
     * 设置待序列化的文件路径
     *
     */
    private static void setDir()
    {
        String classPath = PhysicalPlanLoaderTest.class.getResource("/").getPath();
        
        try
        {
            classPath = URLDecoder.decode(classPath, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        
        serdeDir = classPath + BASICPATH + File.separator;
    }
}

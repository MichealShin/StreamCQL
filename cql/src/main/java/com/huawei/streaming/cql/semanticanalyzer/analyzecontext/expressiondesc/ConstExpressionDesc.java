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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import com.huawei.streaming.cql.CQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.executor.expressioncreater.ConstExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 常量表达式的描述
 *
 */
@ExpressionCreatorAnnotation(ConstExpressionCreator.class)
public class ConstExpressionDesc implements ExpressionDescribe
{
    private static final Logger LOG = LoggerFactory.getLogger(ConstExpressionDesc.class);

    /**
     * 常量值
     */
    private Object constValue;

    /**
     * 常量类型，字符串类型或者int类型
     */
    private Class< ? > type;

    /**
     * <默认构造函数>
     *
     */
    public ConstExpressionDesc(Object constValue, Class< ? > type)
    {
        super();
        this.constValue = constValue;
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return CQLUtils.constantToString(type,constValue);
    }

    public Object getConstValue()
    {
        return constValue;
    }

    public void setConstValue(Object constValue)
    {
        this.constValue = constValue;
    }

    public Class< ? > getType()
    {
        return type;
    }

    public void setType(Class< ? > type)
    {
        this.type = type;
    }

}

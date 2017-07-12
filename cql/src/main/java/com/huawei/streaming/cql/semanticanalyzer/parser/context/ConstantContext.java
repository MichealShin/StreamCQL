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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;


import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ConstExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 常量表达式语法解析内容
 * 
 */
public class ConstantContext extends BaseExpressionParseContext
{
    private Class< ? > datatype;
    
    private Object value;
    
    /**
     * <默认构造函数>
     * 
     */
    public ConstantContext(Class< ? > datatype, Object value)
    {
        super();
        this.datatype = datatype;
        this.value = value;
    }
    
    public Class< ? > getDatatype()
    {
        return datatype;
    }
    
    public void setDatatype(Class< ? > datatype)
    {
        this.datatype = datatype;
    }
    
    public Object getValue()
    {
        return value;
    }
    
    public void setValue(Object value)
    {
        this.value = value;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return CQLUtils.constantToString(datatype,value);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        return new ConstExpressionDesc(value, datatype);
    }
    
}

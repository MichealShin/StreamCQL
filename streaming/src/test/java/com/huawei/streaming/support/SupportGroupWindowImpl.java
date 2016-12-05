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
/*
 * 文 件 名:  SupportGroupWindowImpl.java
 * 版    权:  Huawei Technologies Co., Ltd. Copyright YYYY-YYYY,  All rights reserved
 * 描    述:  <描述>
 * 修 改 人:  z00134314
 * 修改时间:  2013-7-2
 * 跟踪单号:  <跟踪单号>
 * 修改单号:  <修改单号>
 * 修改内容:  <修改内容>
 */
package com.huawei.streaming.support;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.window.group.GroupWindowImpl;

/**
 * <GroupWindowImpl测试支持类>
 * <功能详细描述>
 * 
 */
public class SupportGroupWindowImpl extends GroupWindowImpl
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -4726422077712328039L;
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     */
    public SupportGroupWindowImpl(IExpression[] exprs)
    {
        super(exprs);
    }
    
    /** {@inheritDoc} */
    
    @Override
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        if (null != subCollection)
        {
            subCollection.update(new IEvent[] {theEvent}, null);
        }
        
        if (null != subViews)
        {
            if (subViews instanceof IView)
            {
                ((IView)subViews).update(new IEvent[] {theEvent}, null);
            }
        }
    }
    
}

package com.huawei.streaming.cql.executor.userdefined;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.serde.BaseSerDe;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * web的输出流
 *
 */
public class WebOutputStream implements IOutputStreamOperator
{
    
    private static final long serialVersionUID = 4333986864182441152L;
    
    private static final Logger LOG = LoggerFactory.getLogger(WebOutputStream.class);
    
    private String sid;
    
    private String host;
    
    private String uri;

    private StreamingConfig config;

    private StreamSerDe serde;
    /**
     * 执行
     *
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        
        if (event == null)
        {
            LOG.info("Input event is null");
        }
        
        List<Object[]> values = BaseSerDe.changeEventsToList(event);
        
        for (int i = 0; i < values.size(); i++)
        {
            Object[] vals = values.get(i);
            sid = vals[CQLConst.I_0].toString();
            host = vals[CQLConst.I_1].toString();
            uri = vals[CQLConst.I_2].toString();
            LOG.info(sid + "," + host + "," + uri);
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        this.serde = streamSerDe;
    }

    /**
     * 获取序列化类
     *
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        this.config = conf;
    }

    /**
     * 获取配置属性
     * 编译时接口
     *
     */
    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws StreamingException
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
     throws StreamingException
    {

    }
}

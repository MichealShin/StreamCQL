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

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.jboss.netty.handler.codec.socks.SocksMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 起一个TCP的Server端，监听指定端口，接收数据
 * 接收指定长度的数据
 *
 */
public class WebSpout implements IInputStreamOperator
{
    private static final long serialVersionUID = 5462468262823916954L;
    
    private static final Logger LOG = LoggerFactory.getLogger(WebSpout.class);
    
    private static final int DEFAULT_LISTENER_PORT = 9999;
    
    private static final int DEFAULT_FIXED_LENGTH = 883;
    
    private static final int DEFAULT_QUEUE_LENGTH = 10000;
    
    private int listenerPort = DEFAULT_LISTENER_PORT;
    
    private int fixedLength = DEFAULT_FIXED_LENGTH;
    
    private ArrayBlockingQueue<Object[]> queue;
    
    private boolean isAlreadyStart = false;
    
    private ServerBootstrap bootstrap = null;

    private IEmitter emitter;

    private StreamingConfig config;

    /**
     * 设置配置属性
     * 编译时接口
     * 各种配置属性的缺失，可以在该阶段快速发现
     *
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        queue = new ArrayBlockingQueue<Object[]>(DEFAULT_QUEUE_LENGTH);
        listenerPort = conf.getIntValue("port");
        fixedLength =conf.getIntValue("fixlength");
        this.config = conf;
    }

    /**
     * {@inheritDoc}
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
    public void initialize()
        throws StreamingException
    {

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        if (bootstrap != null)
        {
            bootstrap.releaseExternalResources();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute()
        throws StreamingException
    {
        if (isAlreadyStart)
        {
            emitData(emitter);
            return;
        }
        LOG.info("start to call execute.");
        startNettyServer();
        isAlreadyStart = true;
        LOG.info("finished to call execute.");
    }

    /**
     * 设置数据发送对象
     * 运行时调用
     *
     */
    @Override
    public void setEmitter(IEmitter iEmitter)
    {
        emitter = iEmitter;
    }

    /**
     * 设置序列化和反序列化类
     *
     */
    @Override
    public void setSerDe(StreamSerDe serde)
    {

    }

    /**
     * 获取序列化和反序列化类
     *
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return null;
    }

    private void startNettyServer()
    {
        //storm要求spout要在同一个线程内执行emit和execute，以及acker，所以这里只能使用单线程
        bootstrap =
            new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new ServerChannelPipelineFactory());
        LOG.info("tcp server will start on {}", listenerPort);
        bootstrap.bind(new InetSocketAddress(listenerPort));
    }
    
    private void emitData(IEmitter emitter)
        throws StreamingException
    {
        int i = 0;
        while (!queue.isEmpty() && i < CQLConst.I_1000)
        {
            emitter.emit(queue.poll());
            i++;
        }
        
        if (i == 0)
        {
            try
            {
                Thread.sleep(CQLConst.I_50);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * netty channel factory
     *
     */
    private class ServerChannelPipelineFactory implements ChannelPipelineFactory
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public ChannelPipeline getPipeline()
            throws Exception
        {
            ChannelPipeline pipleline = Channels.pipeline();
            pipleline.addLast("encode", new SocksMessageEncoder());
            pipleline.addLast("decode", new FixedLengthFrameDecoder(fixedLength));
            pipleline.addLast("handler", new NettyServerHandler());
            return pipleline;
        }
        
    }
    
    /**
     * server处理句柄
     *
     */
    private class NettyServerHandler extends SimpleChannelUpstreamHandler
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception
        {
            BigEndianHeapChannelBuffer buffer = (BigEndianHeapChannelBuffer)e.getMessage();
            byte[] bytes = buffer.array();
            queue.put(parseEDR(bytes));
        }
        
        private Object[] parseEDR(byte[] bt)
        {
            Object[] starr = new Object[CQLConst.I_3];
            int WEB_MESSAGE_MSISDN_OFFSET = 41;
            int WEB_MESSAGE_MSISDN_LENGTH = 16;
            int WEB_MESSAGE_HOST_OFFSET = 357;
            int WEB_MESSAGE_HOST_LENGTH = 64;
            int WEB_MESSAGE_FST_URI_OFFSET = 421;
            int WEB_MESSAGE_FST_URI_LENGTH = 128;
            
            char[] chs = new char[WEB_MESSAGE_MSISDN_LENGTH];
            for (int i = WEB_MESSAGE_MSISDN_OFFSET; i < WEB_MESSAGE_MSISDN_OFFSET + WEB_MESSAGE_MSISDN_LENGTH; ++i)
            {
                if (bt[i] == 0x0)
                {
                    break;
                }
                //sb1.append((char)bt[i]);
                chs[i - WEB_MESSAGE_MSISDN_OFFSET] = (char)bt[i];
            }
            starr[0] = String.valueOf(chs).trim();
            
            char[] chs2 = new char[WEB_MESSAGE_HOST_LENGTH];
            for (int i = WEB_MESSAGE_HOST_OFFSET; i < WEB_MESSAGE_HOST_OFFSET + WEB_MESSAGE_HOST_LENGTH; ++i)
            {
                if (bt[i] == 0x0)
                {
                    break;
                }
                chs2[i - WEB_MESSAGE_HOST_OFFSET] = (char)bt[i];
            }
            starr[1] = String.valueOf(chs2).trim();
            
            char[] chs3 = new char[WEB_MESSAGE_FST_URI_LENGTH];
            for (int i = WEB_MESSAGE_FST_URI_OFFSET; i < WEB_MESSAGE_FST_URI_OFFSET + WEB_MESSAGE_FST_URI_LENGTH; ++i)
            {
                if (bt[i] == 0x0)
                {
                    break;
                }
                chs3[i - WEB_MESSAGE_FST_URI_OFFSET] = (char)bt[i];
            }
            starr[CQLConst.I_2] = String.valueOf(chs3).trim();
            
            return starr;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception
        {
            LOG.error("Client has an error,Error cause:" + e.getCause());
            e.getChannel().close();
        }
        
    }
    
}

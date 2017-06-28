package com.pzw.streaming.flink.plan.input;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.common.TupleEventSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/25 下午9:00
 */
public class TcpSource implements ISourceFunction,SourceFunction<TupleEvent> {

    private static Logger LOG = LoggerFactory.getLogger(TcpSource.class);

    public static final String MAX_RETRY="max.retry";

    private String hostName;
    private int    port;
    private int    maxRetry = -1;

    private TupleEventSchema schema;

    private volatile boolean running=true;

    @Override
    public void setSerDe(StreamSerDe serde) {
        schema = new TupleEventSchema(serde);
    }

    @Override
    public void setConfig(StreamingConfig config) throws StreamingException {
        hostName=config.getStringValue(StreamingConfig.OPERATOR_TCPCLIENT_SERVER);
        port=config.getIntValue(StreamingConfig.OPERATOR_TCPCLIENT_PORT);
        maxRetry=config.getIntValue(MAX_RETRY,maxRetry);
    }

    @Override
    public SourceFunction<TupleEvent> getSourceFunction() {
        return this;
    }

    @Override
    public void run(SourceContext<TupleEvent> sourceContext) throws Exception {

        long attempt = 0;

        while (running) {

            try(Socket socket = new Socket()) {

                LOG.info("connecting to {}:{}",hostName,port);
                socket.connect(new InetSocketAddress(hostName,port));

                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String line;
                while (running && (line = reader.readLine()) != null) {

                    try {

                        TupleEvent event = schema.deserialize(line.getBytes());
                        if (event != null) {
                            sourceContext.collect(event);
                        }

                    } catch (IOException e) {
                        LOG.warn("error in deserialize:"+line,e);
                    }
                }

            } catch (IOException e) {
                LOG.error("error in io",e);
            }

            if(running) {
                attempt++;
                if(maxRetry==-1 || attempt<=maxRetry) {
                    Thread.sleep(1000);
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

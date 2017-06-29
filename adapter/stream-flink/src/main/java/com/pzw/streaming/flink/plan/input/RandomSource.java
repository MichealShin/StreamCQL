package com.pzw.streaming.flink.plan.input;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.inputstream.HeadStreamSourceOp.HeadStream;
import com.huawei.streaming.serde.StreamSerDe;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/29 上午10:23
 */
public class RandomSource implements ISourceFunction, SourceFunction<TupleEvent> {

    private TimeUnit timeUnit;
    private int period;
    private int eventNumPerPeriod;


    private IEventType eventType;

    private ScheduledExecutorService timer;

    private volatile boolean running = true;

    @Override
    public void setSerDe(StreamSerDe serde) {
        eventType = serde.getSchema();
    }

    @Override
    public void setConfig(StreamingConfig config) throws StreamingException {
        initConfig(config);
    }

    @Override
    public SourceFunction<TupleEvent> getSourceFunction() {
        return this;
    }

    @Override
    public void run(final SourceContext<TupleEvent> ctx) throws Exception {

        final HeadStream headStream = new HeadStream(eventType);

        timer = new ScheduledThreadPoolExecutor(1);

        timer.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {

                for(int i=0;i<eventNumPerPeriod;i++) {

                    Object[] values = headStream.getOutput();
                    TupleEvent event = new TupleEvent("random", eventType, values);

                    ctx.collect(event);
                }
            }
        },0,period,timeUnit);

        while (running);
    }

    @Override
    public void cancel() {
        timer.shutdown();
        running=false;
    }


    private void initConfig(StreamingConfig config) throws StreamingException {

        timeUnit = TimeUnit.valueOf(config.getStringValue(StreamingConfig.OPERATOR_HEADSTREAM_TIMEUNIT,"SECONDS"));
        period = config.getIntValue(StreamingConfig.OPERATOR_HEADSTREAM_PERIOD,1);
        eventNumPerPeriod = config.getIntValue(StreamingConfig.OPERATOR_HEADSTREAM_EVENTNUMPERPERIOD,1);
    }

}

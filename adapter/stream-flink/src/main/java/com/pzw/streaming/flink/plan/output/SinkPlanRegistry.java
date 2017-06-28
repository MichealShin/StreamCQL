package com.pzw.streaming.flink.plan.output;

import com.google.common.collect.Maps;

import java.util.Map;

/**sink plan注册表
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/18 下午3:48
 */
public class SinkPlanRegistry {

    private static Map<String,Class<? extends ISinkFunction>> registry= Maps.newHashMap();

    static {
        register("kafkaoutput",KafkaSinkPlan.class);
        register("consoleoutput",ConsoleSink.class);
    }

    public static void register(String name,Class<? extends ISinkFunction> sourcePlan) {
        registry.put(name,sourcePlan);
    }

    public static Class<? extends ISinkFunction> get(String name) {
        if(name==null)
            return null;
        return registry.get(name.toLowerCase());
    }
}

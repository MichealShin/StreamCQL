package com.pzw.streaming.flink.plan.input;

import com.google.common.collect.Maps;

import java.util.Map;

/** SourcePlan注册表
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/17 下午9:11
 */
public class SourcePlanRegistry {

    private static Map<String,Class<? extends ISourceFunction>> registry= Maps.newHashMap();

    static {
        register("KafkaInput".toLowerCase(),KafkaSource.class);
        register("TcpClientInput".toLowerCase(),TcpSource.class);
        register("RandomGen".toLowerCase(),RandomSource.class);
    }

    public static void register(String name,Class<? extends ISourceFunction> sourcePlan) {
        registry.put(name,sourcePlan);
    }

    public static Class<? extends ISourceFunction> get(String name) {
        if(name==null)
            return null;
        return registry.get(name.toLowerCase());
    }
}

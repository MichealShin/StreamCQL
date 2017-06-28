package com.pzw.streaming.flink.util;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/15 下午4:27
 */
public class ConfigUtils {

    public static final String CHECKPOINT_MODE_AT_LEAST_ONCE="at_least_once";
    public static final String CHECKPOINT_MODE_EXACTLY_ONCE="extractly_once";
    public static final String CHECKPOINT_MODE_NONE="none";


    public static final String FLINK_EXECUTION_LOCAL="local";
    public static final String FLINK_EXECUTION_YARN="yarn";
    public static final String FLINK_EXECUTION_STANDALONE="standalone";

    public static CheckpointingMode getCheckpointingMode(String value) {

        switch (value) {
            case CHECKPOINT_MODE_AT_LEAST_ONCE:
                return CheckpointingMode.AT_LEAST_ONCE;
            case CHECKPOINT_MODE_EXACTLY_ONCE:
                return CheckpointingMode.EXACTLY_ONCE;
            case CHECKPOINT_MODE_NONE:
                return null;
        }

        throw new IllegalArgumentException("bad checkpoint mode configuration value:"+value);
    }


    public static ClusterClient createRemoteClusterClient(String mode,
                                                          Configuration configuration,
                                                          PackagedProgram program) throws Exception {
        switch (mode) {

            case FLINK_EXECUTION_LOCAL:

                return null;
            case FLINK_EXECUTION_STANDALONE:

                return new StandaloneClusterClient(configuration);
            case FLINK_EXECUTION_YARN:

                AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor();
                descriptor.setFlinkConfiguration(configuration);
                descriptor.setProvidedUserJarFiles(program.getAllLibraries());

                return descriptor.deploy();
        }

        throw new IllegalArgumentException("unknown execute mode:"+mode);
    }


}

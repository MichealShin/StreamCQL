package com.pzw.streaming.flink.graph;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.streams.Schema;

import java.util.List;
import java.util.TreeMap;

/** 应用的图结构
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午2:05
 */
public class ApplicationGraph {

    private String applicationName;
    private TreeMap<String, String> confs;
    private List<String> userFiles= Lists.newArrayList();
    private List<UserFunction> userFunctions=Lists.newArrayList();
    private List<Schema> schemas = Lists.newArrayList();

    private List<ApplicationVertex> applicationVertices=Lists.newArrayList();

    public ApplicationGraph(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public TreeMap<String, String> getConfs() {
        return confs;
    }

    public void setConfs(TreeMap<String, String> confs) {
        this.confs = confs;
    }

    public List<String> getUserFiles() {
        return userFiles;
    }

    public void setUserFiles(List<String> userFiles) {
        this.userFiles = userFiles;
    }

    public List<UserFunction> getUserFunctions() {
        return userFunctions;
    }

    public void setUserFunctions(List<UserFunction> userFunctions) {
        this.userFunctions = userFunctions;
    }

    public List<Schema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<Schema> schemas) {
        this.schemas = schemas;
    }

    public List<ApplicationVertex> getApplicationVertices() {
        return applicationVertices;
    }

    public void setApplicationVertices(List<ApplicationVertex> applicationVertices) {
        this.applicationVertices = applicationVertices;
    }

    public Schema getSchemaByName(String streamName) {
        for(Schema schema:schemas) {
            if(streamName.equalsIgnoreCase(schema.getId())) {
                return schema;
            }
        }
        return null;
    }
}

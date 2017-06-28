package com.pzw.streaming.flink.graph;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.opereators.*;
import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.api.opereators.serdes.UserDefinedSerDeAPI;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.exception.ParseException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.GroupbyClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;
import com.huawei.streaming.serde.StreamSerDe;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午2:40
 */
public class ApplicationGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationGraphGenerator.class);

    private Application application;

    public ApplicationGraphGenerator(Application application) {
        this.application = application;
    }

    public ApplicationGraph generator() {
        ApplicationGraph graph = new ApplicationGraph(application.getApplicationId());

        graph.setConfs(application.getConfs() != null ?
                application.getConfs() : new TreeMap<String, String>());

        graph.setSchemas(application.getSchemas());
        graph.setUserFiles(application.getUserFiles() != null ?
                Lists.newArrayList(application.getUserFiles()) : Lists.<String>newArrayList());

        graph.setUserFunctions(application.getUserFunctions() != null ?
                application.getUserFunctions() : Lists.<UserFunction>newArrayList());

        buildGraph(graph);

        return graph;
    }

    /**
     * 构建图结构
     *
     * @param graph
     */
    private void buildGraph(ApplicationGraph graph) {
        Map<String, ApplicationVertex> vertices = Maps.newHashMap();

        List<ApplicationVertex> topVertices = Lists.newArrayList();

        //算子封装成图的顶点
        for (Operator operator : application.getOperators()) {
            ApplicationVertex vertex = new ApplicationVertex();

            if (operator instanceof InnerInputSourceOperator) {
                operator = formatInnerInputStream(operator);
            }
            if (operator instanceof InnerOutputSourceOperator) {
                operator = formatInnerOutputStream(operator);
            }

            vertex.setOperator(operator);
            vertices.put(vertex.getId(), vertex);

            if (operator instanceof InputStreamOperator) {
                topVertices.add(vertex);
            }
        }
        //设置图的顶点
        graph.setApplicationVertices(topVertices);

        Set<OperatorTransition> newTransitions = new HashSet<>();

        //聚合算子里面的filterBeforeAgg提取到单独的FilterOperator中
        moveFilterInAggregate(vertices,newTransitions);

        //删除SplitterOperator算子,将算子中不同分支的处理逻辑分别封装到新的算子中
        removeSplitterOperator(vertices,newTransitions);

        //替换select表达式中的group by表达式
        replaceGroupByKeyInSelect(vertices,newTransitions);

        //连接图的所有节点
        for (OperatorTransition transition : newTransitions) {

            String formId = transition.getFromOperatorId();
            String toId = transition.getToOperatorId();

            ApplicationVertex vertex = vertices.get(formId);
            ApplicationVertex childVertex = vertices.get(toId);

            vertex.addChild(childVertex, transition);
        }
    }

    private void moveFilterInAggregate(Map<String, ApplicationVertex> vertices,
                                       Set<OperatorTransition> newTransitions) {

        Set<String> aggSet = new HashSet<>();

        for (OperatorTransition transition : application.getOpTransition()) {
            String toId = transition.getToOperatorId();

            ApplicationVertex childVertex = vertices.get(toId);

            if (childVertex.getOperator() instanceof AggregateOperator) {

                AggregateOperator aggOp = (AggregateOperator) childVertex.getOperator();
                if (!Strings.isNullOrEmpty(aggOp.getFilterBeforeAggregate())) {

                    if (!aggSet.contains(aggOp.getId())) {//防止重复生成Filter算子

                        FilterOperator filterOp = new FilterOperator(getIdForFilterBeforeAggOpereation(aggOp),
                                aggOp.getParallelNumber());

                        filterOp.setFilterExpression(aggOp.getFilterBeforeAggregate());

                        ApplicationVertex filterVertex = new ApplicationVertex();
                        filterVertex.setOperator(filterOp);
                        vertices.put(filterVertex.getId(), filterVertex);


                        OperatorTransition t = new OperatorTransition(transition.getStreamName(), filterOp.getId(),
                                childVertex.getOperator().getId(), DistributeType.SHUFFLE, null, transition.getSchemaName());

                        newTransitions.add(t);
                    }

                    //原来的连线改线
                    transition.setToOperatorId(getIdForFilterBeforeAggOpereation(aggOp));
                    aggSet.add(aggOp.getId());
                }
            }
            newTransitions.add(transition);
        }
    }

    private String getIdForFilterBeforeAggOpereation(AggregateOperator operator) {
        return "filterBefore-" + operator.getId();
    }

    /**
     * 删除SplitterOperator算子,将算子中不同分支的处理逻辑分别封装到新的算子中
     * 这样执行计划的翻译逻辑能实现统一,不用为SplitterOperator单独处理
     * @param vertices
     * @param newTransitions
     */
    private void removeSplitterOperator(Map<String, ApplicationVertex> vertices,
                                        Set<OperatorTransition> newTransitions) {

        //存放SplitterOperator的上游连线
        Map<String,OperatorTransition> spliterOpUpperTransitions = Maps.newHashMap();

        for(OperatorTransition transition:application.getOpTransition()) {
            String toId = transition.getToOperatorId();

            ApplicationVertex toVertex = vertices.get(toId);
            if(toVertex.getOperator() instanceof SplitterOperator) {
                spliterOpUpperTransitions.put(toId,transition);
            }
        }

        List<OperatorTransition> toDeleteTransitions = Lists.newArrayList();

        for(OperatorTransition transition : application.getOpTransition()) {

            String fromId = transition.getFromOperatorId();
            String streamName = transition.getStreamName();

            ApplicationVertex fromVertex = vertices.get(fromId);

            if(fromVertex.getOperator() instanceof SplitterOperator) {

                SplitterOperator splitterOperator = (SplitterOperator) fromVertex.getOperator();
                List<SplitterSubContext> splitterSubContexts = splitterOperator.getSubSplitters();

                for(SplitterSubContext splitterSubContext:splitterSubContexts) {
                    if(splitterSubContext.getStreamName().equals(streamName)) {

                        //生成新算子
                        FunctorOperator newOperator = new FunctorOperator(
                                getIdForSplitSubContextOperator(splitterOperator,splitterSubContext),
                                splitterOperator.getParallelNumber());

                        newOperator.setFilterExpression(splitterSubContext.getFilterExpression());
                        newOperator.setOutputExpression(splitterSubContext.getOutputExpression());

                        //SplitterOperator的上游连线
                        OperatorTransition upperTransition=spliterOpUpperTransitions.get(fromId);

                        //建立SplitterOperator上游节点和新算子的连线
                        OperatorTransition upperFunctorToNewFunctor=new OperatorTransition(
                                streamName,
                                upperTransition.getFromOperatorId(),
                                newOperator.getId(),
                                DistributeType.SHUFFLE,
                                null,
                                upperTransition.getSchemaName());

                        ApplicationVertex newVertex = new ApplicationVertex();
                        newVertex.setOperator(newOperator);
                        vertices.put(newVertex.getId(),newVertex);

                        //原来的连线改线
                        transition.setFromOperatorId(newOperator.getId());

                        newTransitions.add(upperFunctorToNewFunctor);

                        //将SplitterOperator的上游连线加入到待删除列表中
                        toDeleteTransitions.add(upperTransition);

                        break;
                    }
                }
            }
            newTransitions.add(transition);
        }

        //移除所有SplitterOperator算子上游连线
        newTransitions.removeAll(toDeleteTransitions);

        //移除所有SplitterOperator算子
        for(String id:spliterOpUpperTransitions.keySet()) {
            vertices.remove(id);
        }
    }


    private String getIdForSplitSubContextOperator(SplitterOperator splitter,SplitterSubContext context) {
        return "splitter-"+splitter.getId()+"-"+context.getStreamName();
    }


    private void replaceGroupByKeyInSelect(Map<String, ApplicationVertex> vertices,
                                           Set<OperatorTransition> newTransitions) {

        for(OperatorTransition transition:newTransitions) {
            String toId = transition.getToOperatorId();
            ApplicationVertex vertex = vertices.get(toId);

            Operator operator =vertex.getOperator();

            if(operator instanceof AggregateOperator &&
                    StringUtils.isNotEmpty(transition.getDistributedFields())) {

                AggregateOperator aggOperator = (AggregateOperator) operator;

                String selectExpression = aggOperator.getOutputExpression();
                IParser selectParser = ParserFactory.createSelectClauseParser();

                try {
                    SelectClauseContext selectClauseContext = (SelectClauseContext) selectParser.parse(selectExpression);

                    IParser groupByParser = ParserFactory.createGroupbyClauseParser();
                    GroupbyClauseContext groupbyClauseContext = (GroupbyClauseContext) groupByParser.parse(transition.getDistributedFields());

                    GroupbyKeyInSelectReplacer replacer = new GroupbyKeyInSelectReplacer(groupbyClauseContext.getExpressions());

                    List<SelectItemContext> selectItems = selectClauseContext.getSelectItems();

                    for(SelectItemContext selectItem:selectItems) {
                        selectItem.getExpression().getExpression().walkChildAndReplace(replacer);
                    }

                    aggOperator.setOutputExpression(Joiner.on(",").join(selectClauseContext.getSelectItems()));

                } catch (ParseException e) {
                    LOG.warn("error in cql parse:"+selectExpression,e);
                }
            }
        }
    }



    private Operator formatInnerInputStream(Operator op) {

        InputStreamOperator inputOperator = new InputStreamOperator(op.getId(), op.getParallelNumber());
        inputOperator.setArgs(op.getArgs());
        Class<? extends SerDeAPI> deserClass = ((InnerInputSourceOperator) op).getDeserializer().getClass();
        Class<? extends StreamSerDe> deserializerClass = AnnotationUtils.getStreamSerDeAnnoationOverClass(deserClass);
        String recordeReaderclass = InputOutputOperatorMapping.getPlatformOperatorByAPI(op.getClass().getName());

        if (deserializerClass != null) {
            inputOperator.setDeserializerClassName(deserializerClass.getName());
        } else {
            UserDefinedSerDeAPI userDeserializerAPI =
                    (UserDefinedSerDeAPI) ((InnerInputSourceOperator) op).getDeserializer();
            inputOperator.setDeserializerClassName(userDeserializerAPI.getSerDeClazz().getName());

        }
        inputOperator.setRecordReaderClassName(recordeReaderclass);

        if (inputOperator.getArgs() == null) {
            inputOperator.setArgs(new TreeMap<String, String>());
        }
        if (application.getConfs() != null)
            inputOperator.getArgs().putAll(application.getConfs());

        try {
            Map<String, String> operatorConfig = AnnotationUtils.getAnnotationsToConfig(op);
            if (operatorConfig != null && !operatorConfig.isEmpty()) {
                inputOperator.getArgs().putAll(operatorConfig);
            }

            Map<String, String> serdeConfig =
                    AnnotationUtils.getAnnotationsToConfig(((InnerInputSourceOperator) op).getDeserializer());
            if (serdeConfig != null && !serdeConfig.isEmpty()) {
                inputOperator.getArgs().putAll(serdeConfig);
            }
        } catch (Exception e) {
            LOG.error(null, e);
        }
        return inputOperator;
    }

    private Operator formatInnerOutputStream(Operator op) {
        OutputStreamOperator outputOperator = new OutputStreamOperator(op.getId(), op.getParallelNumber());
        outputOperator.setArgs(op.getArgs());

        Class<? extends SerDeAPI> deserClass = ((InnerOutputSourceOperator) op).getSerializer().getClass();
        Class<? extends StreamSerDe> serializerClass = AnnotationUtils.getStreamSerDeAnnoationOverClass(deserClass);
        String recordeWriter = InputOutputOperatorMapping.getPlatformOperatorByAPI(op.getClass().getName());

        if (serializerClass != null) {
            outputOperator.setSerializerClassName(serializerClass.getName());
        } else {
            UserDefinedSerDeAPI serializer = (UserDefinedSerDeAPI) ((InnerOutputSourceOperator) op).getSerializer();
            outputOperator.setSerializerClassName(serializer.getSerDeClazz().getName());
        }
        outputOperator.setRecordWriterClassName(recordeWriter);

        if (outputOperator.getArgs() == null) {
            outputOperator.setArgs(new TreeMap<String, String>());
        }

        if (application.getConfs() != null)
            outputOperator.getArgs().putAll(application.getConfs());

        try {
            Map<String, String> operatorConfig = AnnotationUtils.getAnnotationsToConfig(op);
            if (operatorConfig != null && !operatorConfig.isEmpty()) {
                outputOperator.getArgs().putAll(operatorConfig);
            }

            Map<String, String> serdeConfig =
                    AnnotationUtils.getAnnotationsToConfig(((InnerOutputSourceOperator) op).getSerializer());
            if (serdeConfig != null && !serdeConfig.isEmpty()) {
                outputOperator.getArgs().putAll(serdeConfig);
            }
        } catch (Exception e) {
            LOG.error(null, e);
        }
        return outputOperator;
    }
}

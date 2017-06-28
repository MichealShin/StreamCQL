package com.pzw.streaming.flink.graph;

import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.AtomExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.BaseExpressionParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;

import java.util.List;

/** 替换select表达式中的group by 字段
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/26 下午6:02
 */
public class GroupbyKeyInSelectReplacer implements ParseContextReplacer {

    private List<ExpressionContext> shuffleExpressionContexts;
    private int groupByIndex=-1;

    public GroupbyKeyInSelectReplacer(List<ExpressionContext> shuffleExpressionContexts) {
        this.shuffleExpressionContexts = shuffleExpressionContexts;
    }


    @Override
    public boolean isChildsReplaceable(BaseExpressionParseContext parseContext) {

        for(int i=0;i<shuffleExpressionContexts.size();i++) {
            if(shuffleExpressionContexts.get(i).toString().equalsIgnoreCase(parseContext.toString())) {
                groupByIndex=i;
                return true;
            }
        }
        return false;
    }

    @Override
    public BaseExpressionParseContext createReplaceParseContext() {
        AtomExpressionContext atomExpressionContext = new AtomExpressionContext();
        atomExpressionContext.setColumnName(getGroupByExpressionReplacedName(groupByIndex));
        return atomExpressionContext;
    }


    public static String getGroupByExpressionReplacedName(int groupByIndex) {

        return "groupby"+groupByIndex;
    }
}

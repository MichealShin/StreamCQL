package com.pzw.streaming.flink.application;

import org.junit.Test;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/25 下午8:42
 */
public class CQLTest extends CQLBaseTest {

    @Test
    public void testSelect() throws Exception {
        execSql("cql/select.cql");
        while (true);
    }

    @Test
    public void testFilter() throws Exception {
        execSql("cql/filter.cql");
        while (true);
    }

    @Test
    public void testGroupBy() throws Exception {
        execSql("cql/groupby.cql");
        while (true);
    }

    @Test
    public void testGroupByWithFilter() throws Exception {
        execSql("cql/groupby_with_filter.cql");
        while (true);
    }

    @Test
    public void testNoKeyGroupBy() throws Exception {
        execSql("cql/no_key_groupby.cql");
        while (true);
    }

    @Test
    public void testNoWindowGroupBy() throws Exception {
       execSql("cql/no_window_groupby.cql");
        while (true);
    }

    @Test
    public void testNoWindowNoKeyGroupBy() throws Exception {
        execSql("cql/no_window_no_key_groupby.cql");
        while (true);
    }

    @Test
    public void testTimeWindowGroupBy() throws Exception {
        execSql("cql/time_window_groupby.cql");
        while (true);
    }



    @Test
    public void testSplitter() throws Exception {
        execSql("cql/splitter.cql");
        while (true);
    }

    @Test
    public void testRandom() throws Exception {
        execSql("cql/random_input.cql");
        while (true);
    }
}

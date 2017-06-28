package com.pzw.streaming.flink.application;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.huawei.streaming.cql.Driver;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/25 下午8:39
 */
public class CQLBaseTest {

    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception {

        //添加streaming-storm和streaming的jar包到服务端运行
        URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
        for (URL url : classLoader.getURLs()) {
            //随便找一个jar包进行填充
            System.setProperty("cql.dependency.jar", url.getPath());
            break;
        }

    }

    protected String readSql(String path) throws IOException {
        List<String> lines = Resources.readLines(Resources.getResource(path), Charset.forName("UTF-8"));
        Joiner joiner = Joiner.on("\n");
        return joiner.join(lines);
    }


    protected void execSql(String path) throws Exception {
        Driver driver = new Driver();

        String sql = readSql(path);
        //sql=removeCommet(sql);
        String[] lines = sql.split(";");

        for (String line : lines) {
            if (line.trim().length() > 0) {
                driver.run(line);
            }
        }
        List<String[]> results=driver.getResult().getResults();
        for(String[] result:results) {
            for(String r:result) {
                System.out.println(r);
            }
        }
    }
}

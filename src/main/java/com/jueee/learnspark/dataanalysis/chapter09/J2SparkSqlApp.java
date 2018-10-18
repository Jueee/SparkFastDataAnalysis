package com.jueee.learnspark.dataanalysis.chapter09;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.File;

public class J2SparkSqlApp {

    public static final String fileName = FilesUtilByJava.getDataPath() + File.separator + "chapter09" + File.separator + "testweet.json";

    /**
     * 在 Java 中创建 SQL 上下文环境
     */
    public static SQLContext initializingSparkSQL(){
        StringsUtilByJava.printFinish();
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext hiveCtx = new HiveContext(sc);
        System.out.println(hiveCtx);
        return hiveCtx;
    }

    /**
     * 在 Java 中读取并查询推文
     */
    public static void basicQueryExample(SQLContext hiveCtx){
        StringsUtilByJava.printFinish();
        Dataset<Row> input = hiveCtx.jsonFile(fileName);
        input.show();

        input.registerTempTable("tweets");
        Dataset<Row> result = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
        result.show();

        // 访问第一列
        JavaRDD<Object> map = input.toJavaRDD().map(row -> row.get(0));
        map.collect().forEach(System.out::println);
    }

    public static void main(String[] args){
        SQLContext hiveCtx = initializingSparkSQL();
        basicQueryExample(hiveCtx);
    }
}

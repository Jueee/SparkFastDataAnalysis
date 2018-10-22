package com.jueee.learnspark.dataanalysis.chapter09;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

public class J51SparkSqlUDF {

    /**
     * Java 版本的字符串长度 UDF
     */
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext hiveCtx = new HiveContext(sc);
        System.out.println(hiveCtx);
        Dataset<Row> input = hiveCtx.jsonFile(J2SparkSqlApp.fileName);
        input.registerTempTable("tweets");
        input.show();

        hiveCtx.udf().register("stringLengthJava", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);
        Dataset<Row> tweetLength = hiveCtx.sql("SELECT stringLengthJava(text),text FROM tweets LIMIT 10");
        tweetLength.show();
    }
}

package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class J41HiveSQL {


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hiveContext = new HiveContext(sc);
        Dataset<Row> rows = hiveContext.sql("select * from test limit 5");
        Row firstRow = rows.first();
        System.out.println(firstRow.getString(0));
    }
}

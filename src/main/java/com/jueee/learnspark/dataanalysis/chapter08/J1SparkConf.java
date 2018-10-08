package com.jueee.learnspark.dataanalysis.chapter08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class J1SparkConf {

    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "My Spark App");
        conf.set("spark.master", "local[4]");
        conf.set("spark.ui.port", "36000"); // 重载默认端口配置
        System.out.println(conf);

        // 使用这个配置对象创建一个SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc);
    }
}

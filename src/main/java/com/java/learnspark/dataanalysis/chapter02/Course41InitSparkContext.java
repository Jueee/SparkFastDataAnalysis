package com.java.learnspark.dataanalysis.chapter02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Course41InitSparkContext {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("java init:" + sc);
        sc.close();
    }
}

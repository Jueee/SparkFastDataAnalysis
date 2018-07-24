package com.jueee.learnspark.dataanalysis.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        org.apache.spark.api.java.JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<String> javaRDD = lines.filter(line -> line.contains("J"));
        System.out.println(javaRDD);
        for (String line:javaRDD.take((int) javaRDD.count())){
            System.out.println(line);
        }
        javaRDD.collect().forEach(System.out::println);
    }
}

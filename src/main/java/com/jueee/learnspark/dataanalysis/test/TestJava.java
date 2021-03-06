package com.jueee.learnspark.dataanalysis.test;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.IntStream;

public class TestJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        org.apache.spark.api.java.JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<String> javaRDD = lines.filter(line -> line.contains("J"));
        System.out.println(javaRDD);
        for (String line:javaRDD.take((int) javaRDD.count())){
            System.out.println(line);
        }
        javaRDD.collect().forEach(System.out::println);


        IntStream.range(1,8).forEach(System.out::print);
    }
}

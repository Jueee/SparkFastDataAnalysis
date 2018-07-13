package com.java.learnspark.dataanalysis.chapter01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TestSparkContext {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("java init:" + sc);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        int result = distData.reduce(Integer::sum);
        System.out.println("result: " + result);

    }
}

package com.jueee.learnspark.dataanalysis.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

public class JavaSparkContext {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        org.apache.spark.api.java.JavaSparkContext sc = new org.apache.spark.api.java.JavaSparkContext(conf);
        System.out.println("java init:" + sc);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        int result = distData.reduce(Integer::sum);
        System.out.println("result: " + result);

        sc.close();
    }
}

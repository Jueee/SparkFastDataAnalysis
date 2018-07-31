package com.jueee.learnspark.dataanalysis.chapter03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

public class J6Persist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 计算 RDD 中各值的平方
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> result = rdd.map(x -> x*x);

        result.persist(StorageLevel.DISK_ONLY());

        System.out.println(result.count());
        result.collect().forEach(System.out::println);
    }
}

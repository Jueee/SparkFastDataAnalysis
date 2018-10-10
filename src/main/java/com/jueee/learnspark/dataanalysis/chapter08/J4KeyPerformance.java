package com.jueee.learnspark.dataanalysis.chapter08;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class J4KeyPerformance {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取输入文件
        JavaRDD<String> input = sc.textFile("*.*");
        System.out.println(input.getNumPartitions());

        // 排除掉大部分数据的筛选方法
        JavaRDD<String> lines = input.filter(line -> line.startsWith("#"));
        System.out.println(lines.getNumPartitions());

        // 在缓存lines之前先对其进行合并操作
        JavaRDD<String> caches = lines.coalesce(5).cache();
        System.out.println(caches.getNumPartitions());

        System.out.println(caches.count());
        caches.collect().forEach(System.out::println);
    }
}

package com.jueee.learnspark.dataanalysis.chapter03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.Arrays;

/**
 * 用 Java 创建 DoubleRDD
 */
public class J52DoubleRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1,2,3,4,5));

        JavaDoubleRDD result = lines.mapToDouble(
                new DoubleFunction<Integer>() {
                    @Override
                    public double call(Integer x) throws Exception {
                        return (double) x*x;
                    }
                }
        );
        System.out.println(result.mean());
    }
}

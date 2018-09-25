package com.jueee.learnspark.dataanalysis.chapter04;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class J33Joins {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaPairRDD<String, String> storeAddress = sc.parallelizePairs(Arrays.asList(
                new Tuple2("Ritual", "1026 Valencia St"),new Tuple2("Philz", "748 Van Ness Ave"),
                new Tuple2("Philz", "3101 24th St"),new Tuple2("Starbucks", "Seattle")
        ));

        JavaPairRDD<String, Double> storeRating = sc.parallelizePairs(Arrays.asList(
                new Tuple2("Ritual", 4.9),new Tuple2("Philz",4.8),new Tuple2("Jueee",4.2)
        ));

//        System.out.println("--- join ---");
//        JavaPairRDD<String, Tuple2<String, Double>> joinResult = storeAddress.join(storeRating);
//        joinResult.collect().forEach(System.out::println);
//
//        System.out.println("--- leftOuterJoin ---");
//        JavaPairRDD<String, Tuple2<String, Optional<Double>>> leftOuterJoinResult = storeAddress.leftOuterJoin(storeRating);
//        leftOuterJoinResult.collect().forEach(System.out::println);
//
//
//        System.out.println("--- rightOuterJoin ---");
//        JavaPairRDD<String, Tuple2<Optional<String>, Double>>  rightOuterJoinResult = storeAddress.rightOuterJoin(storeRating);
//        rightOuterJoinResult.collect().forEach(System.out::println);
    }
}

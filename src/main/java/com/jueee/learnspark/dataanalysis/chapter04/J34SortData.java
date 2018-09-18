package com.jueee.learnspark.dataanalysis.chapter04;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class J34SortData {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(
                4,5,3,12,6,323,32,2,54,7
        ));

        JavaPairRDD<Integer,Integer> numsPairs0 = nums.mapToPair(s -> new Tuple2(s,s));
        JavaPairRDD<Integer, Integer> orderByResult0 = numsPairs0.sortByKey();
        orderByResult0.collect().forEach(System.out::println);

        System.out.println();

        JavaPairRDD<Integer,Integer> numsPairs1 = nums.mapToPair(s -> new Tuple2(s,s));
        JavaPairRDD<Integer, Integer> orderByResult1 = numsPairs1.sortByKey(new StringComparator());
        orderByResult1.collect().forEach(System.out::println);

    }
}


class StringComparator implements Comparator<Integer>, Serializable {
    @Override
    public int compare(Integer o1, Integer o2) {
        return String.valueOf(o1).compareTo(String.valueOf(o2));
    }
}
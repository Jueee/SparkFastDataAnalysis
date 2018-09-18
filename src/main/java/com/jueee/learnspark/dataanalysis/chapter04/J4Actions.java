package com.jueee.learnspark.dataanalysis.chapter04;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class J4Actions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        countByKey(sc);
        collectAsMap(sc);
        lookup(sc);
    }

    /**
     * 对每个键对应的元素分别计数
     * @param sc
     */
    public static void countByKey(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<Integer, Integer> storeAddress = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, 2), new Tuple2(3, 4), new Tuple2(3, 6)
        ));
        Map<Integer, Long> result = storeAddress.countByKey();
        System.out.println(result);
    }

    /**
     * 将结果以映射表的形式返回，以便查询
     * @param sc
     */
    public static void collectAsMap(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<Integer, Integer> storeAddress = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, 2), new Tuple2(3, 4), new Tuple2(3, 6)
        ));
        Map<Integer, Integer> result = storeAddress.collectAsMap();
        System.out.println(result);

        System.out.println();

        JavaPairRDD<Integer, Integer> storeAddress2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, 2), new Tuple2(3, 4), new Tuple2(5, 6)
        ));
        Map<Integer, Integer> result2 = storeAddress2.collectAsMap();
        System.out.println(result2);
    }

    /**
     * 返回给定键对应的所有值
     * @param sc
     */
    public static void lookup(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<Integer, Integer> storeAddress = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, 2), new Tuple2(3, 4), new Tuple2(3, 6)
        ));
        List<Integer> result = storeAddress.lookup(3);
        System.out.println(result);
    }
}

package com.jueee.learnspark.dataanalysis.chapter04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class J31Aggregations {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        reduceByKeyJava7(sc);
        reduceByKeyJava8(sc);
    }

    /**
     * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
     * @param sc
     */
    public static void reduceByKeyJava7(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0),new Tuple2("pink",3),new Tuple2("pirate",3),new Tuple2("panda",1),new Tuple2("pink",4)));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = pairs.mapValues(
                new Function<Integer, Tuple2<Integer,Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer integer) {
                        return new Tuple2<Integer, Integer>(integer,1);
                    }
                }
        ).reduceByKey(
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                        return new Tuple2<Integer, Integer>(x._1()+y._1(),x._2()+y._2());
                    }
                }
        );
        for (Tuple2<String, Tuple2<Integer, Integer>> value:values.collect()){
            System.out.println(value);
        }
    }
    /**
     * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
     * @param sc
     */
    public static void reduceByKeyJava8(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0),new Tuple2("pink",3),new Tuple2("pirate",3),new Tuple2("panda",1),new Tuple2("pink",4)));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = pairs.mapValues(x -> new Tuple2<Integer, Integer>(x,1))
                .reduceByKey((x,y) -> new Tuple2<Integer, Integer>(x._1()+y._1(),x._2()+y._2()));
        for (Tuple2<String, Tuple2<Integer, Integer>> value:values.collect()){
            System.out.println(value);
        }
    }
}

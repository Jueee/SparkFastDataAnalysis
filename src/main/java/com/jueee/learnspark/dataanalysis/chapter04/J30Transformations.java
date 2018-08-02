package com.jueee.learnspark.dataanalysis.chapter04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class J30Transformations {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        transByOne(sc);
        transByTwo(sc);
        filterPair(sc);
    }

    public static void transByOne(JavaSparkContext sc){
        JavaPairRDD<Integer,Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2(1,2),new Tuple2(3,4),new Tuple2(3,6)));
        // 合并具有相同键的值
        // {(1,2), (3,10)}
        pairs.reduceByKey((x,y) -> x+y).collect().forEach(System.out::print);

        // 对具有相同键的值进行分组
        // {(1,CompactBuffer(2)), (3,CompactBuffer(4, 6))}
        pairs.groupByKey().collect().forEach(System.out::print);

        // 对 pair RDD 中的每个值应用一个函数而不改变键
        // {(1,3), (3,5), (3,7)}
        pairs.mapValues(x -> x+1).collect().forEach(System.out::print);

        // 返回一个仅包含键的 RDD
        // {1, 3, 3}
        pairs.keys().collect().forEach(System.out::print);

        // 返回一个仅包含值的 RDD
        // {2, 4, 6}
        pairs.values().collect().forEach(System.out::print);

        // 返回一个根据键排序的 RDD
        // {(1,2), (3,4), (3,6)}
        pairs.sortByKey().collect().forEach(System.out::print);

    }

    public static void transByTwo(JavaSparkContext sc){
        System.out.println();
        JavaPairRDD<Integer,Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2(1,2),new Tuple2(3,4),new Tuple2(3,6)));
        JavaPairRDD<Integer,Integer> other = sc.parallelizePairs(Arrays.asList(new Tuple2(3, 9)));

        // 删掉 RDD 中键与 other RDD 中的键相同的元素
        // {(1,2)}
        rdd.subtractByKey(other).collect().forEach(System.out::print);

        // 对两个 RDD 进行内连接
        // {(3,(4,9)), (3,(6,9))}
        rdd.join(other).collect().forEach(System.out::print);

        // 对两个 RDD 进行连接操作，确保第一个 RDD 的键必须存在（右外连接）
        // {(3,(Some(4),9)), (3,(Some(6),9))}
        rdd.rightOuterJoin(other).collect().forEach(System.out::print);

        // 对两个 RDD 进行连接操作，确保第二个 RDD 的键必须存在（左外连接）
        // {(1,(2,None)), (3,(4,Some(9))), (3,(6,Some(9)))}
        rdd.leftOuterJoin(other).collect().forEach(System.out::print);

        // 将两个 RDD 中拥有相同键的数据分组到一起
        // {(1,(CompactBuffer(2),CompactBuffer())), (3,(CompactBuffer(4, 6),CompactBuffer(9)))}
        rdd.cogroup(other).collect().forEach(System.out::print);

    }

    public static void filterPair(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaPairRDD<String,String> pairs = lines.mapToPair(s -> new Tuple2(s.split(" ")[0],s));
//        pairs.filter{case (key,value) => value.length < 20}.foreach(println)
        System.out.println("-----Java 6-----");
        pairs.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> t) throws Exception {
                        return t._2().length() < 20;
                    }
                }
        ).collect().forEach(value -> System.out.println(value._1 + "\t---useJava8---\t" + value._2));
        System.out.println("-----Java 8-----");
        pairs.filter(t -> t._2().length() < 20).collect()
                .forEach(value -> System.out.println(value._1 + "\t---useJava8---\t" + value._2));
    }
}

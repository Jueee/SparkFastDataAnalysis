package com.jueee.learnspark.dataanalysis.chapter04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class J2CreatePairRDD {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        useJava7(sc);
        useJava8(sc);
    }


    /**
     * Java7 没有自带的二元组类型，因此 Spark 的 Java API 让用户使用 scala.Tuple2 类来创建二元组。
     *
     * Java7 用户可以通过 new Tuple2(elem1, elem2) 来创建一个新的二元组，并且可以通过 ._1() 和 ._2() 方法访问其中的元素。
     *
     * Java 用户还需要调用专门的 Spark 函数来创建 pair RDD。
     * 例如，要使用 mapToPair() 函数来代替基础版的 map() 函数
     *
     * @param sc
     */
    public static void useJava7(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");

        PairFunction<String,String,String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2(s.split(" ")[0],s);
            }
        };
        JavaPairRDD<String,String> pairs = lines.mapToPair(keyData);
        for (Tuple2<String, String> value:pairs.collect()){
            System.out.println(value._1 + "\t---useJava7---\t" + value._2);
        }
    }

    public static void useJava8(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaPairRDD<String,String> pairs = lines.mapToPair(s -> new Tuple2(s.split(" ")[0],s));
        pairs.collect().forEach(value -> System.out.println(value._1 + "\t---useJava8---\t" + value._2));
    }

}

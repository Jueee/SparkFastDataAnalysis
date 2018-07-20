package com.jueee.learnspark.dataanalysis.chapter02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class J3CoreConcepts {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        content1(sc);
        content2(sc);
        content3(sc);
    }

    public static void content1(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<String> scalaLines = lines.filter(line -> line.contains("Scala"));
        System.out.println(scalaLines.count());
        System.out.println(scalaLines.first());
    }

    /**
     * 向 Spark 传递函数
     * @param sc
     */
    public static void content2(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<String> scalaLines = lines.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        return s.contains("Scala");
                    }
                }
        );
        System.out.println(scalaLines.count());
        System.out.println(scalaLines.first());
    }

    /**
     * 向 Spark 传递函数
     * @param sc
     */
    public static void content3(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<String> scalaLines = lines.filter(line -> hasScala(line));
        System.out.println(scalaLines.count());
        System.out.println(scalaLines.first());
    }

    public static boolean hasScala(String line){
        return line.contains("Scala");
    }
}

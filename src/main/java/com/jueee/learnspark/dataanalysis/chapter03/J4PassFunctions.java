package com.jueee.learnspark.dataanalysis.chapter03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class J4PassFunctions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("README.md");

        // 在 Java 中使用匿名内部类进行函数传递
        JavaRDD<String> javaRdd = lines.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        return s.contains("Spark");
                    }
                }
        );
        System.out.println(javaRdd.first());

        // 在 Java 中使用具名类进行函数传递
        JavaRDD<String> javaRDD = lines.filter(new ContainsJava());
        System.out.println(javaRDD.first());


        // 带参数的 Java 函数类
        JavaRDD<String> testRDD1 = lines.filter(new Contains("Scala"));
        System.out.println(testRDD1.count());
        JavaRDD<String> testRDD2 = lines.filter(new Contains("Spark"));
        System.out.println(testRDD2.count());


        // 在 Java 中使用 Java 8 地 lambda 表达式进行函数传递
        JavaRDD<String> testRDD3 = lines.filter(s -> s.contains("2"));
        System.out.println(testRDD3.first());
    }
}


class ContainsJava implements Function<String,Boolean>{
    @Override
    public Boolean call(String s) {
        return s.contains("Spark");
    }
}

/**
 * 带参数的 Java 函数类
 */
class Contains implements Function<String,Boolean>{
    private String query;
    public Contains(String query){
        this.query = query;
    }
    @Override
    public Boolean call(String s) {
        return s.contains(query);
    }
}
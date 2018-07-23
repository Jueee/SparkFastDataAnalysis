package com.jueee.learnspark.dataanalysis.chapter02;

import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

public class J42WordCount {

    public static String SPLIT_REGEX_STR = "[^a-zA-Z0-9]";

    public static void wordCountByJava7(String inputFile,String outputPath){
        // 创建一个Java版本的Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取我们的输入数据
        JavaRDD<String> input = sc.textFile(inputFile);
        // 切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(SPLIT_REGEX_STR)).iterator();
                    }
                }
        );
        // 转换为键值对并计数
        JavaPairRDD<String,Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );
        // 将统计出来的单词总数存入一个文本文件，引发求值
        counts.saveAsTextFile(outputPath + File.separator + StringsUtilByJava.getMethodName());
        StringsUtilByJava.printFinish();
        sc.close();
    }

    public static void wordCountByJava8(String inputFile,String outputPath){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(SPLIT_REGEX_STR)).iterator());
        JavaPairRDD<String,Integer> counts = words.mapToPair(s -> new Tuple2<String, Integer>(s,1))
                                                  .reduceByKey((x,y) -> x + y);
        counts.saveAsTextFile(outputPath + File.separator + StringsUtilByJava.getMethodName());
        StringsUtilByJava.printFinish();
        sc.close();
    }

    public static void main(String[] args) {
        String inputFile = "pom.xml";
        String outputPath = FilesUtilByJava.getDataPath() + File.separator + "chapter02" + File.separator + "42wordcount";
        wordCountByJava7(inputFile,outputPath);
        wordCountByJava8(inputFile,outputPath);
    }
}

package com.jueee.learnspark.dataanalysis.chapter08;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;

public class J2Components {

    static String fileName = FilesUtilByJava.getResourcePath()+ File.separator+"data"+File.separator+"chapter08"+File.separator+"input.txt";

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取输入文件
        JavaRDD<String> input = sc.textFile(fileName);

        // 切分为单词并且删掉空行
        JavaRDD<String[]> tokenized = input.filter(line -> line.length() > 0)
                .map(line -> line.split(" "))
                .filter(words -> words.length > 0);

        // 提取出每行的第一个单词（日志等级）并进行计数
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(words -> new Tuple2<String, Integer>(words[0], 1)).reduceByKey((a, b) -> (a + b));

        // Spark 提供了 toDebugString() 方法来查看 RDD 的谱系。
        System.out.println(input.toDebugString());
        System.out.println(counts.toDebugString());
        counts.collect().forEach(System.out::println);
    }
}

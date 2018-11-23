package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.actors.threadpool.Arrays;

import java.util.List;


/**
 * 在 Java 中使用 TF-IDF
 */
public class J51FeatureExtraction {

    public static void main(String[] args){
        String sentence = "hello hello world";
        String[] words = sentence.split(" ");           // 将句子切分为一串单词
        HashingTF tf = new HashingTF(10000);       // 创建一个向量，其尺寸S = 10,000
        Vector transform = tf.transform(Arrays.asList(words));
        System.out.println(transform);


        // 将若干文本文件读取为TF向量
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<List> rdd = sc.wholeTextFiles("README.md").map(text -> Arrays.asList(text._2.split(" ")));
        JavaRDD<Vector> tfVectors = tf.transform(rdd);          // 对整个RDD进行转化操作
        tfVectors.collect().forEach(System.out::println);

        // 计算IDF，然后计算TF-IDF向量
        IDF idf = new IDF();
        IDFModel idfModel = idf.fit(tfVectors);
        JavaRDD<Vector> tfIdfVectors = idfModel.transform(tfVectors);
        tfIdfVectors.collect().forEach(System.out::println);
    }
}

package com.jueee.learnspark.dataanalysis.chapter11

import java.util
import java.util.{Arrays, List}

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object S51FeatureExtraction {

  def main(args: Array[String]): Unit = {
    val sentence = "hello hello world"
    val words = sentence.split(" ")
    // 将句子切分为一串单词
    val tf = new HashingTF(10000)
    // 创建一个向量，其尺寸S = 10,000
    val transform = tf.transform(words)
    println(transform)


    // 将若干文本文件读取为TF向量
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val value = sc.wholeTextFiles("README.md")
    value.collect().foreach(println)

    // 在 Scala 中缩放向量
    println("--- 在 Scala 中缩放向量 ---")
    val denseVec1 = Vectors.dense(-2.0, 5.0, 1.0)
    val denseVec2 = Vectors.dense(2.0, 0.0, 1.0)
    val dataset = sc.parallelize(Array(denseVec1, denseVec2))
    val scaler = new StandardScaler(true, true)
    val model = scaler.fit(dataset)
    val result = model.transform(dataset)
    dataset.collect.foreach(println)
    result.collect.foreach(println)
  }

}

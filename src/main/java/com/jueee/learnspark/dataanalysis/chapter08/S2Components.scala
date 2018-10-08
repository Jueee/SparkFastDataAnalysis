package com.jueee.learnspark.dataanalysis.chapter08

import java.io.File

import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, FilesUtilByJava}
import org.apache.spark.{SparkConf, SparkContext}

object S2Components {

  val fileName = FilesUtilByJava.getResourcePath+File.separator+"data"+File.separator+"chapter08"+File.separator+"input.txt";

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    // 读取输入文件
    val input = sc.textFile(fileName)

    // 切分为单词并且删掉空行
    val tokenized = input.filter(line => line.length>0)
                         .map(line => line.split(" "))
                         .filter(words => words.size>0)

    // 提取出每行的第一个单词（日志等级）并进行计数
    val count = tokenized.map(words => (words(0),1)).reduceByKey{ (a,b) => a+b}
    count.collect().foreach(println)

    // Spark 提供了 toDebugString() 方法来查看 RDD 的谱系。
    println(input.toDebugString)
    println(count.toDebugString)
  }
}

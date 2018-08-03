package com.jueee.learnspark.dataanalysis.chapter04

import com.jueee.learnspark.dataanalysis.util.StringsUtilByScala
import org.apache.spark.{SparkConf, SparkContext}

object S31Aggregations {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    reduceByKey(sc)
    wordcount(sc)
    wordcountByCountByValue(sc)
  }

  /**
    * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
    * @param sc
    */
  def reduceByKey(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val pairs = sc.parallelize(List(Tuple2("panda", 0),Tuple2("pink",3),Tuple2("pirate",3),Tuple2("panda",1),Tuple2("pink",4)))
    val values = pairs.mapValues(x => (x,1)).reduceByKey((x,y) =>(x._1+y._1, x._2+y._2))
    values.foreach(println)
    val averages = values.map(x => (x._1,x._2._1/double2Double(x._2._2)))
    averages.foreach(println)
  }

  /**
    * 实现单词计数
    * 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
    * @param sc
    */
  def wordcount(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val input = sc.textFile("README.md")
    val words = input.flatMap(x => x.split(" "))
    val result = words.map(x => (x,1)).reduceByKey((x,y) => x+y)
    result.foreach(println)
  }

  /**
    * 实现单词计数-countByValue
    * 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
    * @param sc
    */
  def wordcountByCountByValue(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val input = sc.textFile("README.md")
    val words = input.flatMap(x => x.split(" ")).countByValue()
    words.foreach(println)
  }
}

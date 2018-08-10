package com.jueee.learnspark.dataanalysis.chapter04

import com.jueee.learnspark.dataanalysis.util.StringsUtilByScala
import org.apache.spark.{SparkConf, SparkContext}

object S4Actions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    countByKey(sc)
    collectAsMap(sc)
    lookup(sc)
  }

  /**
    * 对每个键对应的元素分别计数
    * @param sc
    */
  def countByKey(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val numsPair = sc.parallelize(List(Tuple2(1, 2), Tuple2(3, 4), Tuple2(3, 6)))
    val result = numsPair.countByKey()
    result.foreach(println)
  }

  /**
    * 将结果以映射表的形式返回，以便查询
    * @param sc
    */
  def collectAsMap(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val numsPair = sc.parallelize(List(Tuple2(1, 2), Tuple2(3, 4), Tuple2(3, 6)))
    val result = numsPair.collectAsMap()
    result.foreach(println)
    println()
    val numsPair2 = sc.parallelize(List(Tuple2(1, 2), Tuple2(3, 4), Tuple2(5, 6)))
    val result2 = numsPair2.collectAsMap()
    result2.foreach(println)
  }

  /**
    * 返回给定键对应的所有值
    * @param sc
    */
  def lookup(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val numsPair = sc.parallelize(List(Tuple2(1, 2), Tuple2(3, 4), Tuple2(3, 6)))
    val result = numsPair.lookup(3)
    result.foreach(println)
  }
}

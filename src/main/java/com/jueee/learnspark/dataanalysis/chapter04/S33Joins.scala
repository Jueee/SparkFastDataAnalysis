package com.jueee.learnspark.dataanalysis.chapter04

import org.apache.spark.{SparkConf, SparkContext}

object S33Joins {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val storeAddress = sc.parallelize(List(
      Tuple2("Ritual", "1026 Valencia St"),Tuple2("Philz", "748 Van Ness Ave"),
      Tuple2("Philz", "3101 24th St"),Tuple2("Starbucks", "Seattle")
    ))

    val storeRating = sc.parallelize(List(
      Tuple2("Ritual", 4.9),Tuple2("Philz",4.8),Tuple2("Jueee",4.2)
    ))

    println("--- join ---")
    val joinResult = storeAddress.join(storeRating)
    joinResult.collect().foreach(println)

    println("--- leftOuterJoin ---")
    val leftOuterJoinResult = storeAddress.leftOuterJoin(storeRating)
    leftOuterJoinResult.collect().foreach(println)

    println("--- rightOuterJoin ---")
    val rightOuterJoinResult = storeAddress.rightOuterJoin(storeRating)
    rightOuterJoinResult.collect().foreach(println)
  }
}

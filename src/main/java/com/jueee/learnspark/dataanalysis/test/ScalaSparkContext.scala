package com.jueee.learnspark.dataanalysis.test

import org.apache.spark.{SparkConf, SparkContext}

object ScalaSparkContext {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    println("scala init:" + sc)

    val data = Array(1,2,3,4,5)
    val distData = sc.parallelize(data)
    val result = distData.reduce(_ + _)
    println("result:" + result)
  }
}

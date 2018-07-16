package com.jueee.learnspark.dataanalysis.chapter02

import org.apache.spark.{SparkConf, SparkContext}

object S41InitSparkContext {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    println("scala init:" + sc)
  }
}

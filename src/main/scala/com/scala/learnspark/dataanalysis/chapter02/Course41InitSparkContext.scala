package com.scala.learnspark.dataanalysis.chapter02

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Course41InitSparkContext {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    println("scala init:" + sc)
  }
}

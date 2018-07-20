package com.jueee.learnspark.dataanalysis.chapter02

import org.apache.spark.{SparkConf, SparkContext}

object S2ScalaShell {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("README.md")
    println(lines.count())
    println(lines.first())
  }
}

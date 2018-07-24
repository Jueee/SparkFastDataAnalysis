package com.jueee.learnspark.dataanalysis.test

import org.apache.spark.{SparkConf, SparkContext}

object TestScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    println("scala init:" + sc)
    val lines = sc.textFile("README.md")
    val scalalines = lines.filter(line => line.contains("Scala"))
    scalalines.take(scalalines.count().toInt).foreach(println)
  }
}

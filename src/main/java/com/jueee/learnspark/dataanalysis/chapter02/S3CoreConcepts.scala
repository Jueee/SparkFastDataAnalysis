package com.jueee.learnspark.dataanalysis.chapter02

import org.apache.spark.{SparkConf, SparkContext}

object S3CoreConcepts {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    content1(sc)
    content2(sc)
  }

  def content1(sc:SparkContext): Unit ={
    val lines = sc.textFile("README.md")
    val scalaLines = lines.filter(line => line.contains("Scala"))
    println(scalaLines.count())
    println(scalaLines.first())
  }

  /**
    * 向 Spark 传递函数
    */
  def content2(sc:SparkContext)={
    println("-----向 Spark 传递函数-----")
    def hasScala(line:String): Boolean ={
      return line.contains("Scala");
    }
    val lines = sc.textFile("README.md")
    val scalaLines = lines.filter(hasScala)
    println(scalaLines.count())
    println(scalaLines.first())

  }
}

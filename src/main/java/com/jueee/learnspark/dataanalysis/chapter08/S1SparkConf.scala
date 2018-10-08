package com.jueee.learnspark.dataanalysis.chapter08

import org.apache.spark.{SparkConf, SparkContext}

object S1SparkConf {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.set("spark.app.name", "My Spark App")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // 重载默认端口配置
    println(conf)

    // 使用这个配置对象创建一个SparkContext
    val sc = new SparkContext(conf)
    println(sc)
  }
}

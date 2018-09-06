package com.jueee.learnspark.dataanalysis.chapter05

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object S41HiveSQL {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    val rows = hiveCtx.sql("select * from test limit 5")
    val firstRow = rows.first()
    println(firstRow.getString(0))
  }
}

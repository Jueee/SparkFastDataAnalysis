package com.jueee.learnspark.dataanalysis.chapter09

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object S31HiveSQL {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    val rows = hiveCtx.sql("select * from test limit 5")
    val firstRow = rows.first()
    println(firstRow.getString(0))
  }
}

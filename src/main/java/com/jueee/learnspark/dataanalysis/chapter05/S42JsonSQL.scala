package com.jueee.learnspark.dataanalysis.chapter05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object S42JsonSQL {

  /**
    * 在 Scala 中使用 Spark SQL 读取 JSON 数据
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    val rowDataset = hiveCtx.jsonFile(J42JsonSQL.inputFile)
    rowDataset.registerTempTable("test")
    val rows = hiveCtx.sql("select date,city,data.ganmao from test")
    val firstRow = rows.first()
    println(firstRow.getString(0))
    rows.show(3)    // only showing top 3 rows
    rows.show()

  }

}

package com.jueee.learnspark.dataanalysis.chapter09

import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, StringsUtilByJava, StringsUtilByScala}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object S2SparkSqlApp {

  def main(args: Array[String]): Unit = {
    val hiveCtx = initializingSparkSQL()
    basicQueryExample(hiveCtx)
  }

  def initializingSparkSQL(): SQLContext ={
    StringsUtilByScala.printFinish()
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc);
    println(hiveCtx)
    hiveCtx
  }

  /**
    * 在 Scala 中读取并查询推文
    */
  def basicQueryExample(hiveCtx: SQLContext): Unit = {
    StringsUtilByScala.printFinish()
    val input = hiveCtx.jsonFile(J2SparkSqlApp.fileName)
    input.show()
    input.registerTempTable("tweets")
    val result = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    result.show()

  }
}

package com.jueee.learnspark.dataanalysis.chapter09

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object S51SparkSqlUDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc);
    println(hiveCtx)

    val input = hiveCtx.jsonFile(J2SparkSqlApp.fileName)
    input.registerTempTable("tweets")
    input.show()

    hiveCtx.udf.register("strLenScala", (_: String).length)
    val tweetLength = hiveCtx.sql("SELECT strLenScala(text),text FROM tweets LIMIT 10")
    tweetLength.show()
  }
}

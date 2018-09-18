package com.jueee.learnspark.dataanalysis.chapter04

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}

object S2CreatePairRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val lines = sc.textFile("README.md")
    val pairs = lines.map(x => (x.split(" ")(0),x))
    pairs.collect().foreach(println)
  }
}

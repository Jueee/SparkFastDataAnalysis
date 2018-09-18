package com.jueee.learnspark.dataanalysis.chapter03

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object S6Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    // 计算 RDD 中各值的平方
    val input = sc.parallelize(List(1,2,3,4,5))
    val result = input.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)

    println(result.count())
    println(result.collect().mkString(","))
  }
}

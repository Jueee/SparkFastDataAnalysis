package com.jueee.learnspark.dataanalysis.chapter06

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object S6NumericRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new JavaSparkContext(conf)

    val rdd = sc.parallelize(List("123", "231", "312"))

    val distanceDoubles = rdd.map(s => s.toDouble)

    val stats = distanceDoubles.stats()

    // 标准差
    val stddev = stats.stdev
    println(stddev)
    // 元素的平均值
    val mean = stats.mean
    println(mean)

    val reasonableDistance = distanceDoubles.filter(x => Math.abs(x - mean) < 3 * stddev)
    print(reasonableDistance.collect().toList)
  }
}

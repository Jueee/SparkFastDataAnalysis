package com.jueee.learnspark.dataanalysis.chapter03

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}

object S51BasicRDDs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    // 计算 RDD 中各值的平方
    val input = sc.parallelize(List(1,2,3,4,5))
    val result = input.map(x => x*x)
    println(result.collect().mkString(" "))

    //  flatMap() 将行数据切分为单词
    val lines = sc.parallelize(List("hello world","hi"))
    val words = lines.flatMap(line => line.split(" "))
    words.collect().foreach(println)

    //  reduce() 计算元素的总和
    val sum = result.reduce((x,y) => x+y)
    println(sum)

    // 用 aggregate() 来计算 RDD 的平均值
    val agg = result.aggregate((0,0))(
      (acc,value) => (acc._1 + value, acc._2 + 1),
      (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val avg = agg._1 / agg._2.toDouble
    println(avg)
  }
}

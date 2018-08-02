package com.jueee.learnspark.dataanalysis.chapter04

import org.apache.spark.{SparkConf, SparkContext}

object S31Aggregations {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    reduceByKey(sc)
  }

  /**
    * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
    * @param sc
    */
  def reduceByKey(sc:SparkContext): Unit ={
    val pairs = sc.parallelize(List(Tuple2("panda", 0),Tuple2("pink",3),Tuple2("pirate",3),Tuple2("panda",1),Tuple2("pink",4)))
    val values = pairs.mapValues(x => (x,1)).reduceByKey((x,y) =>(x._1+y._1, x._2+y._2))
    values.foreach(println)
  }
}

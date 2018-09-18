package com.jueee.learnspark.dataanalysis.chapter04

import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, StringsUtilByScala}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 以字符串顺序对整数进行自定义排序
  */
object S34SortData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    exampleRdd0(sc)
    exampleRdd1(sc)
    examplePairRdd0(sc)
    examplePairRdd1(sc)
  }

  def exampleRdd0(sc: SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val nums = sc.parallelize(List(4,5,3,12,6,323,32,2,6,54,7))
    val results = nums.sortBy(value => value)
    results.collect().foreach(println)
  }

  def exampleRdd1(sc: SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val nums = sc.parallelize(List(4,5,3,12,6,323,32,2,6,54,7))
    //隐式转换声明排序的依据
    implicit val sortIntegersByString = new Ordering[Int]{
      override def compare(x: Int, y: Int): Int = x.toString.compareTo(y.toString)
    }
    val results = nums.sortBy(value => value)
    results.collect().foreach(println)
  }

  def examplePairRdd0(sc: SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val numsPair = sc.parallelize(List(Tuple2(1, 1), Tuple2(12, 2), Tuple2(1, 3), Tuple2(2, 4), Tuple2(3, 4)))
    val resultsPair = numsPair.sortByKey()
    resultsPair.collect.foreach(println)
  }

  def examplePairRdd1(sc: SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val numsPair = sc.parallelize(List(Tuple2(1, 1), Tuple2(12, 2), Tuple2(1, 3), Tuple2(2, 4), Tuple2(3, 4)))
    //隐式转换声明排序的依据
    implicit val sortIntegersByString = new Ordering[Int]{
      override def compare(x: Int, y: Int): Int = x.toString.compareTo(y.toString)
    }
    val resultsPair = numsPair.sortByKey()
    resultsPair.collect.foreach(println)
  }
}

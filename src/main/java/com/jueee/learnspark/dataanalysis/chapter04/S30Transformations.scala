package com.jueee.learnspark.dataanalysis.chapter04

import org.apache.spark.{SparkConf, SparkContext}

object S30Transformations {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    transByOne(sc)
    transByTwo(sc)
    filterPair(sc)
  }

  /**
    * Pair RDD的转化操作
    * （以键值对集合 {(1, 2), (3, 4), (3, 6)} 为例）
    * @param sc
    */
  def transByOne(sc:SparkContext)={
    val lines = sc.parallelize(List(Tuple2(1,2),Tuple2(3,4),Tuple2(3,6)))
    val pairs = lines.map(x => (x._1,x._2))

    // 合并具有相同键的值
    // {(1,2), (3,10)}
    println(pairs.reduceByKey((x,y) => x+y).collect().mkString("{", ", ", "}"))

    // 对具有相同键的值进行分组
    // {(1,CompactBuffer(2)), (3,CompactBuffer(4, 6))}
    println(pairs.groupByKey.collect().mkString("{", ", ", "}"))

    // 对 pair RDD 中的每个值应用一个函数而不改变键
    // {(1,3), (3,5), (3,7)}
    println(pairs.mapValues(x => x+1).collect().mkString("{", ", ", "}"))

    // 对 pair RDD 中的每个值应用一个返回迭代器的函数
    // 然后对返回的每个元素都生成一个对应原键的键值对记录。
    // 通常用于符号化
    // {(1,2), (1,3), (1,4), (1,5), (1,6), (1,7), (1,8), (3,4), (3,5), (3,6), (3,7), (3,8), (3,6), (3,7), (3,8)}
    println(pairs.flatMapValues(x => (x to 8)).collect().mkString("{", ", ", "}"))

    // 返回一个仅包含键的 RDD
    // {1, 3, 3}
    println(pairs.keys.collect().mkString("{", ", ", "}"))

    // 返回一个仅包含值的 RDD
    // {2, 4, 6}
    println(pairs.values.collect().mkString("{", ", ", "}"))

    // 返回一个根据键排序的 RDD
    // {(1,2), (3,4), (3,6)}
    println(pairs.sortByKey().collect().mkString("{", ", ", "}"))

  }

  /**
    * 针对两个pair RDD的转化操作
    * （rdd =  {(1, 2), (3, 4), (3, 6)} other =  {(3, 9)} ）
    * @param context
    */
  def transByTwo(sc: SparkContext)={
    println()
    val value1 = sc.parallelize(List(Tuple2(1,2),Tuple2(3,4),Tuple2(3,6)))
    val rdd = value1.map(x => (x._1,x._2))
    val value2 = sc.parallelize(List(Tuple2(3, 9)))
    val other = value2.map(x => (x._1,x._2))

    // 删掉 RDD 中键与 other RDD 中的键相同的元素
    // {(1,2)}
    println(rdd.subtractByKey(other).collect().mkString("{", ", ", "}"))

    // 对两个 RDD 进行内连接
    // {(3,(4,9)), (3,(6,9))}
    println(rdd.join(other).collect().mkString("{", ", ", "}"))

    // 对两个 RDD 进行连接操作，确保第一个 RDD 的键必须存在（右外连接）
    // {(3,(Some(4),9)), (3,(Some(6),9))}
    println(rdd.rightOuterJoin(other).collect().mkString("{", ", ", "}"))

    // 对两个 RDD 进行连接操作，确保第二个 RDD 的键必须存在（左外连接）
    // {(1,(2,None)), (3,(4,Some(9))), (3,(6,Some(9)))}
    println(rdd.leftOuterJoin(other).collect().mkString("{", ", ", "}"))

    // 将两个 RDD 中拥有相同键的数据分组到一起
    // {(1,(CompactBuffer(2),CompactBuffer())), (3,(CompactBuffer(4, 6),CompactBuffer(9)))}
    println(rdd.cogroup(other).collect().mkString("{", ", ", "}"))


  }

  /**
    * 对第二个元素进行筛选
    * 选掉长度超过 20 个字符的行
    *
    * @param sc
    */
  def filterPair(sc: SparkContext)={
    println
    val lines = sc.textFile("README.md")
    val pairs = lines.map(x => (x.split(" ")(0),x))
    pairs.filter{case (key,value) => value.length < 20}.foreach(println)
  }
}

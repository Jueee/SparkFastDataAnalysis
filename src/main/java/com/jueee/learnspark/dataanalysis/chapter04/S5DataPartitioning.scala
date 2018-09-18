package com.jueee.learnspark.dataanalysis.chapter04

import java.net.URL

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object S5DataPartitioning {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    getDataPartitioning(sc)
    scalaPageRank(sc)
    customPartitioner()
  }

  /**
    * 获取 RDD 的分区方式
    * @param sc
    */
  def getDataPartitioning(sc: SparkContext): Unit ={
    // 创建出了一个由 (Int, Int) 对组成的 RDD
    val pairs = sc.parallelize(List((1,1), (2,2), (3,3)))
    // 初始时没有分区方式信息（一个值为 None 的 Option 对象）
    println(pairs.partitioner)

    // 通过对第一个 RDD 进行哈希分区，创建出了第二个 RDD。
    val partitioned = pairs.partitionBy(new HashPartitioner(2))
    println(partitioned)
    println(partitioned.partitioner)
  }

  /**
    * Scala 版 PageRank
    * @param sc
    */
  def scalaPageRank(sc: SparkContext): Unit ={
    // 假设相邻页面列表以Spark objectFile的形式存储
    val links = sc.objectFile[(String,Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()

    // 将每个页面的排序值初始化为1.0；
    // 由于使用mapValues，生成的RDD的分区方式会和"links"的一样
    var ranks = links.mapValues(v => 1.0)

    // 运行10轮PageRank迭代
    for(i <- 0 until 10){
      val contributions = links.join(ranks).flatMap{
        case (pageId,(links,rank)) => links.map(dest => (dest,rank/links.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(v => 0.15 + 0.85*v)
    }

    // 写出最终排名
    ranks.saveAsTextFile("ranks")
  }

  /**
    * Scala 自定义分区方式
    * 基于域名的分区器，这个分区器只对 URL 中的域名部分求哈希。
    */
  def customPartitioner(): Unit ={

    class DomainNamePartitioner(numParts:Int) extends Partitioner{
      override def numPartitions: Int = numParts

      override def getPartition(key: Any): Int = {
        val domain = new URL(key.toString).getHost
        val code = (domain.hashCode % numPartitions)
        if (code < 0){
          code + numPartitions    // 使其非负
        } else {
          code
        }
      }
      // 用来让Spark区分分区函数对象的Java equals方法
      // 使用 Scala 的模式匹配操作符（ match ）来检查 other 是否是 DomainNamePartitioner ，并在成立时自动进行类型转换
      override def equals(other: scala.Any): Boolean = other match{
        case dnp:DomainNamePartitioner => dnp.numPartitions == numPartitions
        case _ => false
      }
    }
  }
}
package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S5ApacheKafka {
  var zkQuorum = ""
  var group = ""

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 创建一个从主题到接收器线程数的映射表
    val topics = List(("pandas", 1), ("logs", 1)).toMap
    val topicLines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)
    topicLines.print()
  }
}

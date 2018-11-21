package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S5ApacheFlume {

  var receiverHostname = ""

  var receiverPort = 0

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    val events = FlumeUtils.createStream(ssc, receiverHostname, receiverPort)
    events.print()
  }
}

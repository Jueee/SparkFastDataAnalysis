package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S32StatefulTransformations {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)

    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 连接到本地机器7777端口上后，使用收到的数据创建DStream
    val logData = ssc.socketTextStream("localhost", 7777)
    logData.print()

    // 假设ApacheAccessingLog是用来从Apache日志中解析条目的工具类
    val accessLogsDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line))
    example1(accessLogsDStream)
    example2(accessLogsDStream)
  }


  /**
    * 每个 IP 地址的访问量计数
    */
  def example1(accessLogsDStream:DStream[ApacheAccessLog]): Unit ={
    val ipDStream = accessLogsDStream.map(logEntry => (logEntry.getIpAddress(), 1))
    val ipCountDStream = ipDStream.reduceByKeyAndWindow(
      {(x, y) => x + y}, // 加上新进入窗口的批次中的元素
      {(x, y) => x - y}, // 移除离开窗口的老批次中的元素
      Seconds(30), // 窗口时长
      Seconds(10)) // 滑动步长
    ipCountDStream.print()
  }

  /**
    * 窗口计数操作
    */
  def example2(accessLogsDStream:DStream[ApacheAccessLog]): Unit ={
    val ipDStream = accessLogsDStream.map{entry => entry.getIpAddress()}
    val ipAddressRequestCount = ipDStream.countByValueAndWindow(Seconds(30), Seconds(10))
    ipAddressRequestCount.print()
    val requestCount = accessLogsDStream.countByWindow(Seconds(30), Seconds(10))
    requestCount.print()
  }
}

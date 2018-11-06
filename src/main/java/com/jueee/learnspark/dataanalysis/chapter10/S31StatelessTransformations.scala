package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S31StatelessTransformations {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)

    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 连接到本地机器7777端口上后，使用收到的数据创建DStream
    val logData = ssc.socketTextStream("localhost", 7777)
    logData.print()

    // 假设ApacheAccessingLog是用来从Apache日志中解析条目的工具类
    val accessLogsDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line))
    val ipDStream = accessLogsDStream.map(entry => (entry.getIpAddress(), 1))
    val ipCountsDStream = ipDStream.reduceByKey((x, y) => x + y)
    ipCountsDStream.print()

    val ipBytesDStream = accessLogsDStream.map(entry => (entry.getIpAddress(), entry.getContentSize()))
    val ipBytesSumDStream = ipBytesDStream.reduceByKey((x, y) => x + y)
    val ipBytesRequestCountDStream = ipCountsDStream.join(ipBytesSumDStream)
    ipBytesRequestCountDStream.print()

  }
}

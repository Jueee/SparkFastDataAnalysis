package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class S33UpdateStateByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)

    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 连接到本地机器7777端口上后，使用收到的数据创建DStream
    val logData = ssc.socketTextStream("localhost", 7777)
    logData.print()

    // 假设ApacheAccessingLog是用来从Apache日志中解析条目的工具类
    val accessLogsDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line))

    /**
      * 使用 updateStateByKey() 运行响应代码的计数
      */

    def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
      Some(state.getOrElse(0L) + values.size)
    }
    val responseCodeDStream = accessLogsDStream.map(log => (log.getResponseCode(), 1L))
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum _)
    responseCodeCountDStream.print()
  }
}

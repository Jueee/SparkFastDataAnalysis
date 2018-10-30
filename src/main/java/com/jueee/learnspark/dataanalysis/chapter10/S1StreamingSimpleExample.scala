package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S1StreamingSimpleExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)

    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 连接到本地机器7777端口上后，使用收到的数据创建DStream
    val lines = ssc.socketTextStream("localhost", 7777)

    // 从DStream中筛选出包含字符串"error"的行
    val errorLines = lines.filter(_.contains("error"))

    // 打印出有"error"的行
    errorLines.print()


    // 启动流计算环境StreamingContext并等待它"完成"
    ssc.start()
    // 等待作业完成
    ssc.awaitTermination()
  }
}

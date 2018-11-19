package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S4SaveSequenceFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)

    // 从SparkConf创建StreamingContext并指定1秒钟的批处理大小
    val ssc = new StreamingContext(conf, Seconds(1))

    // 连接到本地机器7777端口上后，使用收到的数据创建DStream
    val logData = ssc.socketTextStream("localhost", 7777)
    logData.print()

    val accessLogsDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line))

    val ipDStream = accessLogsDStream.map{entry => entry.getIpAddress()}
    val ipAddressRequestCount = ipDStream.countByValueAndWindow(Seconds(30), Seconds(10))
    ipAddressRequestCount.print()

  }
}

package com.jueee.learnspark.dataanalysis.chapter10

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object S6DriverFaultTolerance {

  var checkpointDir = ""

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    def createStreamingContext() = {
      val sc = new SparkContext(conf)
      // 以1秒作为批次大小创建StreamingContext
      val ssc = new StreamingContext(sc, Seconds(1))
      ssc.checkpoint(checkpointDir)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext _)
  }
}

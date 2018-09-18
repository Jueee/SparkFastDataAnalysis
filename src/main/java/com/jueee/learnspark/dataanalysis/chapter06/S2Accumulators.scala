package com.jueee.learnspark.dataanalysis.chapter06

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}

object S2Accumulators {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    val lines = sc.textFile("README.md")
    val blankLines = sc.accumulator(0)
    val callSigns = lines.flatMap(line => {
      if (line == ""){
        blankLines += 1
      }
      line.split(" ")
    })

    callSigns.collect.foreach(value => println("[Value]"+value))
    println("Blank lines: " + blankLines.value)
  }
}

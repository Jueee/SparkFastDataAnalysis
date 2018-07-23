package com.jueee.learnspark.dataanalysis.chapter02

import java.io.File

import com.jueee.learnspark.dataanalysis.util.{FilesUtilByJava, StringsUtilByJava, StringsUtilByScala}
import org.apache.spark.{SparkConf, SparkContext}

object S42WordCount {

  def main(args: Array[String]): Unit = {
    val inputFile = "pom.xml";
    val outputPath = FilesUtilByJava.getDataPath + File.separator + "chapter02" + File.separator + "42wordcount"
    wordcountByScala(inputFile,outputPath)
  }

  def wordcountByScala(inputFile:String,outputPath:String)={
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split("[^a-zA-Z0-9]"))
    val counts = words.map(word => (word,1)).reduceByKey{case (x,y) => x + y}
    counts.saveAsTextFile(outputPath + File.separator + StringsUtilByJava.getMethodName)
    StringsUtilByScala.printFinish()
  }
}

package com.jueee.learnspark.dataanalysis.chapter05

import java.io.File

import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object S24SequenceFile {

  val filePath = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats"

  val fileName = filePath + File.separator + "save_by_scala.sequence" + File.separator + "part-00000"

  val saveName = filePath + File.separator + "save_by_scala.sequence"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)


    //      saveSequenceFile(sc)
    readSequenceFile(sc)
  }

  def saveSequenceFile(sc: SparkContext): Unit = {
    val data = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
    data.saveAsSequenceFile(saveName)
  }

  def readSequenceFile(sc: SparkContext): Unit = {
    val data = sc.sequenceFile(fileName, classOf[Text], classOf[IntWritable])
      .map{case (x,y) => (x.toString,y.get())}
    data.foreach(println)
  }
}

package com.jueee.learnspark.dataanalysis.chapter05

import java.io.{File, StringReader, StringWriter}

import com.jueee.learnspark.dataanalysis.util.{FilesUtilByJava, StringsUtilByScala}
import com.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.{SparkConf, SparkContext}

object S23CSV {

  val fileName = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "demo.csv"

  val saveName = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "save_by_scala.csv"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(fileName)
    lines.foreach(println)
    readCSV(sc)
    readCSVfull(sc)
    saveCSV(sc)
  }

  /**
    * 在 Scala 中使用 textFile() 读取 CSV
    * @param sc
    */
  def readCSV(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val lines = sc.textFile(fileName)
    val result = lines.map{ line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    result.collect().foreach(value =>{
      println(value,value.toList)
    })
  }

  /**
    * 在 Scala 中完整读取 CSV
    * @param sc
    */
  def readCSVfull(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    case class Person(id: String,name:String,indate:String)

    val lines = sc.wholeTextFiles(fileName)
    println(lines)
  }

//  def saveCSV(sc: SparkContext): Unit ={
//    StringsUtilByScala.printFinish()
//    val lines = sc.textFile(fileName)
//    lines.map(person => List(person).toArray)
//         .mapPartitions { person =>
//           val stringWriter = new StringWriter()
//           val csvWrite = new CSVWriter(stringWriter)
//           csvWrite.writeAll(person.toList)
//           Iterator(stringWriter.toString)
//         }.saveAsTextFile(saveName)
//  }
}

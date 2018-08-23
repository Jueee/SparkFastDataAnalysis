package com.jueee.learnspark.dataanalysis.chapter05

import java.io.File

import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.Json

object S22JSON {

  case class Person(date:String, message:String, status:Int, city:String, count:Int)

  implicit val personReads = Json.format[Person]

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "json.txt")
    lines.foreach(println)

    val parsed = lines.map(Json.parse(_))
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)
    result.foreach(println)
    println(result.count())
  }
}

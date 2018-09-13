package com.jueee.learnspark.dataanalysis.chapter05

import java.io.File

import com.jueee.learnspark.dataanalysis.util.{FilesUtilByJava, StringsUtilByScala}
import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat
import org.apache.hadoop.io.{LongWritable, MapWritable, Text}
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object S26HadoopFormats {


  val filePath = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats"

  val fileName = filePath + File.separator + "save_by_java.hadoop" + File.separator + "part-00000"

  val jsonFileName = filePath + File.separator + "json.txt"

  val protoFileName = filePath + File.separator + "save_by_scala.proto"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    byKeyValueTextInputFormat(sc)
    byElephantBird(sc)
    saveByProtocolBuffer(sc)
  }

  /**
    * 在 Scala 中使用老式 API 读取 KeyValueTextInputFormat()
    * @param sc
    */
  def byKeyValueTextInputFormat(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val input = sc.hadoopFile[Text,Text,KeyValueTextInputFormat](fileName).map{
      case (x,y) => (x.toString, y.toString)
    }
    input.collect().foreach(println)
  }

  /**
    * 在 Scala 中使用 Elephant Bird 读取 LZO 算法压缩的 JSON 文件
    * @param sc
    */
  def byElephantBird(sc:SparkContext): Unit ={
    StringsUtilByScala.printFinish()
    val conf = new Job().getConfiguration
    conf.set("io.compression.codecs","com.hadoop.compression.lzo.LzopCodec")
    conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")
    val input = sc.newAPIHadoopFile(jsonFileName,classOf[LzoJsonInputFormat],
      classOf[LongWritable], classOf[MapWritable], conf)
    input.collect().foreach(println)
  }


  def saveByProtocolBuffer(sc:SparkContext): Unit ={
    /*
    StringsUtilByScala.printFinish()
    val job = new Job()
    val conf = job.getConfiguration
    conf.setBoolean("hadoop.native.lib", true)
    LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);
    val dnaLounge = Places.Venue.newBuilder()
    dnaLounge.setId(1);
    dnaLounge.setName("DNA Lounge")
    dnaLounge.setType(Places.Venue.VenueType.CLUB)
    val data = sc.parallelize(List(dnaLounge.build()))
    val outputData = data.map{ pb =>
      val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
      protoWritable.set(pb)
      (null, protoWritable)
    }
    outputData.saveAsNewAPIHadoopFile(protoFileName, classOf[Text],
      classOf[ProtobufWritable[Places.Venue]],
      classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)
      */
  }

}

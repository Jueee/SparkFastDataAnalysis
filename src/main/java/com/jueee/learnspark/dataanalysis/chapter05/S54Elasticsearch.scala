package com.jueee.learnspark.dataanalysis.chapter05

import java.util
import java.util.Arrays

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsInputFormat

object S54Elasticsearch {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    saveElasticsearch(sc)
    readElasticsearch(sc)
  }

  /**
    * 查询Elasticsearch
    * @param sc
    */
  def readElasticsearch(sc:SparkContext): Unit ={
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, DataBaseUtil.ES_RESOURCE_READ)
    jobConf.set(ConfigurationOptions.ES_NODES, DataBaseUtil.ES_NODES)
    val currentTweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object,MapWritable]], classOf[Object],classOf[MapWritable])
    val tweets = currentTweets.map{case (key,value) => value}
    println(tweets.count)
    tweets.foreach(println)
  }

  def saveElasticsearch(sc:SparkContext): Unit ={
    val pairs = sc.parallelize(List(Tuple2(new Text("aa"),new Text("bb"))))

    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, DataBaseUtil.ES_RESOURCE_WRITE)
    jobConf.set(ConfigurationOptions.ES_NODES, DataBaseUtil.ES_NODES)
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    pairs.saveAsHadoopDataset(jobConf)


  }
}

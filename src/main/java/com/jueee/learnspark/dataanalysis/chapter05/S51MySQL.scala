package com.jueee.learnspark.dataanalysis.chapter05

import java.sql.{DriverManager, ResultSet}

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object S51MySQL {


  def createConnection()={
    Class.forName(DataBaseUtil.MYSQL_DRIVER).newInstance()
    DriverManager.getConnection(DataBaseUtil.MYSQL_CONNECTION_ALL)
  }

  def extractValues(r:ResultSet)={
    (r.getInt(1), r.getString(2), r.getString(3))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val data = new JdbcRDD(sc, createConnection, DataBaseUtil.MYSQL_TEST_SQL + "where id >= ? and id <= ?",
      lowerBound = 1, upperBound = 40, numPartitions = 2, mapRow = extractValues)

    println(data.collect().toList)

  }
}

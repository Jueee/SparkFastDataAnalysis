package com.jueee.learnspark.dataanalysis.chapter11

import java.io.File
import java.util.regex.Pattern

import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, FilesUtilByJava}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 协同过滤与推荐
  */
object S55Recommendation {
  private val TAB = Pattern.compile("\t")

  def buildModel(rdd: JavaRDD[Rating]): MatrixFactorizationModel = { //训练模型
    val rank = 10
    val numIterations = 5
    val model = ALS.train(rdd.rdd, rank, numIterations, 0.01)
    model
  }

  def splitData: Array[RDD[Rating]] = { //分割数据，一部分用于训练，一部分用于测试
    val sparkConf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(FilesUtilByJava.getDataPath + File.separator + "chapter11" + File.separator + "u.data")
    val ratings = lines.map((line: String) => {
      def foo(line: String) = {
        val tok = TAB.split(line)
        val x = tok(0).toInt
        val y = tok(1).toInt
        val rating = tok(2).toDouble
        new Rating(x, y, rating)
      }
      foo(line)
    })
    val splits = ratings.randomSplit(Array[Double](0.6, 0.4), 11L)
    splits
  }

  def getMSE(ratings: RDD[Rating], model: MatrixFactorizationModel): Double = { //计算MSE
    val usersProducts = ratings.map((rating: Rating) => new Tuple2(rating.user, rating.product))
    val predictions = model.predict(usersProducts).map(rating => new Tuple2[Tuple2[Integer, Integer], Double](new Tuple2[Integer, Integer](rating.user, rating.product), rating.rating))
    val ratesAndPreds = ratings.map(rating => new Tuple2[Tuple2[Integer, Integer], Double](new Tuple2[Integer, Integer](rating.user, rating.product), rating.rating))
    val joins = ratesAndPreds.join(predictions)
    joins.map(o => {
        val err = o._2._1 - o._2._2
        err * err
      }).mean
  }
  def main(args: Array[String]): Unit = {
    val splits = splitData
    val model = buildModel(splits(0))
    println("model:" + model)
    val MSE = getMSE(splits(0), model)
    println("Mean Squared Error = " + MSE) //训练数据的MSE

    val MSE1 = getMSE(splits(1), model)
    println("Mean Squared Error1 = " + MSE1) //测试数据的MSE

  }

  /**
    * model:org.apache.spark.mllib.recommendation.MatrixFactorizationModel@15827d0
    * Mean Squared Error = 0.4185604991701565
    * Mean Squared Error1 = 1.3170497808171187
    */
}

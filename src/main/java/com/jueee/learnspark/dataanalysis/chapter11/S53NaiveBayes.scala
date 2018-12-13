package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * 朴素贝叶斯
  * 朴素贝叶斯需要非负特征值!!!
  */
object S53NaiveBayes {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(3.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(0.0, Vectors.dense(2.0, 0.0, 1.0))
    val points = sc.parallelize(Array(denseVec1, denseVec2))
    val lr = new NaiveBayes()
    val model = lr.run(points)
    val denseVec = Vectors.dense(1.5, 2.0, 3.0)
    printf("predict: %s, theta length: %s\n", model.predict(denseVec), model.theta.length)
    // predict: 0.0, theta length: 2
  }
}

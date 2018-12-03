package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

object S53LinearRegression {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(-2.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(2.0, Vectors.dense(2.0, 0.0, 1.0))
    val dataset = sc.parallelize(Array(denseVec1, denseVec2))
    val lr = new LinearRegressionWithSGD().setIntercept(true)
    val model = lr.run(dataset)
    printf("weights: %s, intercept: %s\n", model.weights, model.intercept)
  }

}

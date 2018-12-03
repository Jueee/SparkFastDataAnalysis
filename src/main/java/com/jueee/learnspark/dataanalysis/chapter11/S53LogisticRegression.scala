package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object S53LogisticRegression {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(-2.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(0.0, Vectors.dense(2.0, 0.0, 1.0))
    val dataset = sc.parallelize(Array(denseVec1, denseVec2))
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)
    val model = lr.run(dataset)
    printf("weights: %s, intercept: %s\n", model.weights, model.intercept)
    // weights: [-5.648693355757891,3.0491285907000156,0.0], intercept: -7.349130469531492
  }
}

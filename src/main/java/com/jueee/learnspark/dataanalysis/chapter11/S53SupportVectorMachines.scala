package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 支持向量机
  */
object S53SupportVectorMachines {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(-2.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(0.0, Vectors.dense(2.0, 0.0, 1.0))
    val dataset = sc.parallelize(Array(denseVec1, denseVec2))
    val lr = new SVMWithSGD().setIntercept(true)
    val model = lr.run(dataset)
    printf("weights: %s, intercept: %s\n", model.weights, model.intercept)
    // weights: [-0.8385291954371556,0.0,-0.4192645977185778], intercept: 0.4108793057642061
  }
}

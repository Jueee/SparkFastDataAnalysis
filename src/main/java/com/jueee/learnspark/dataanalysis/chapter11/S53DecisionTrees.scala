package com.jueee.learnspark.dataanalysis.chapter11

import java.util
import java.util.{HashMap, Map}

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

/**
  * 决策树
  */
object S53DecisionTrees {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(3.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(0.0, Vectors.dense(2.0, 0.0, 1.0))
    val points = sc.parallelize(Array(denseVec1, denseVec2))

    //设置决策树参数，训练模型
    val numClasses = 3
    val categoricalFeaturesInfo = new util.HashMap[Integer, Integer]
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val decisionTreeModel = DecisionTree.trainClassifier(points, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    println("决策树模型：" + decisionTreeModel.toDebugString)

    //未处理数据，带入模型处理
    val denseVec = Vectors.dense(3.5, 2.0, 3.0)
    val predict = decisionTreeModel.predict(denseVec)
    printf("predict: %s\n", predict)

    /**
      * 决策树模型：DecisionTreeModel classifier of depth 1 with 3 nodes
      * If (feature 0 <= 2.5)
      * Predict: 0.0
      * Else (feature 0 > 2.5)
      * Predict: 1.0
      *
      * predict: 1.0
      */
  }
}

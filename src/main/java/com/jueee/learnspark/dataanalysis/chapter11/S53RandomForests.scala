package com.jueee.learnspark.dataanalysis.chapter11

import java.util

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}

/**
  * 随机森林
  */
object S53RandomForests {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val denseVec1 = LabeledPoint.apply(1.0, Vectors.dense(3.0, 5.0, 1.0))
    val denseVec2 = LabeledPoint.apply(0.0, Vectors.dense(2.0, 0.0, 1.0))
    val points = sc.parallelize(Array(denseVec1, denseVec2))

    //设置决策树参数，训练模型
    val numClasses = 3
    val categoricalFeaturesInfo = new util.HashMap[Integer, Integer]
    val numTrees = 3
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val seed = 5
    val decisionTreeModel = RandomForest.trainClassifier(points, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
    println("随机森林模型：" + decisionTreeModel.toDebugString)

    //未处理数据，带入模型处理
    val denseVec = Vectors.dense(3.5, 2.0, 3.0)
    val predict = decisionTreeModel.predict(denseVec)
    printf("predict: %s\n", predict)

    """
      |随机森林模型：TreeEnsembleModel classifier with 3 trees
      |
      |  Tree 0:
      |    Predict: 1.0
      |  Tree 1:
      |    If (feature 1 <= 2.5)
      |     Predict: 0.0
      |    Else (feature 1 > 2.5)
      |     Predict: 1.0
      |  Tree 2:
      |    Predict: 0.0
      |
      |predict: %s
      | 0.0
    """.stripMargin
  }
}

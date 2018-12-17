package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 聚类   -  K-Means 算法
  */
object S54KMeans {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val vector1 = Vectors.dense(-2.0, 5.0, 1.0)
    val vector2 = Vectors.dense(2.0, 0.0, 1.0)
    val points = sc.parallelize(Array(vector1, vector2))

    val numClusters = 3
    val numIterations = 20
    val kMeansModel = KMeans.train(points, numClusters, numIterations)
    println("K-Means 聚类模型：" + kMeansModel)

    println("kMeansModel centers:")
    for (center <- kMeansModel.clusterCenters) {
      println(" " + center)
    }
    val cost = kMeansModel.computeCost(points)
    println("Cost: " + cost)

    //未处理数据，带入模型处理
    val denseVec = Vectors.dense(3.5, 2.0, 3.0)
    val predict = kMeansModel.predict(denseVec)
    printf("predict: %s\n", predict)

    """
      |K-Means 聚类模型：org.apache.spark.mllib.clustering.KMeansModel@b67519
      |kMeansModel centers:
      | [-2.0,5.0,1.0]
      | [2.0,0.0,1.0]
      |Cost: 0.0
      |predict: 1
      |
    """.stripMargin

  }
}

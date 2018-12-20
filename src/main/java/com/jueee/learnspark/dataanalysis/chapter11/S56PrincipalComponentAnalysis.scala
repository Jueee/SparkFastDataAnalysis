package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

object S56PrincipalComponentAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val vector1 = Vectors.dense(-2.0, 5.0, 1.0)
    val vector2 = Vectors.dense(2.0, 0.0, 1.0)
    val points = sc.parallelize(Array(vector1, vector2))

    val mat = new RowMatrix(points);
    val pc = mat.computePrincipalComponents(2)

    // 将点投影到低维空间中
    val projected = mat.multiply(pc).rows

    // 在投影出的二维数据上训练k-means模型
    val kMeansModel = KMeans.train(projected, 3,10)
    println("K-Means 聚类模型：" + kMeansModel)

    println("kMeansModel centers:")
    for (center <- kMeansModel.clusterCenters) {
      println(" " + center)
    }

    /**
      * K-Means 聚类模型：org.apache.spark.mllib.clustering.KMeansModel@10c7a83
      * kMeansModel centers:
      * [-1.2493900951088484,1.0]
      * [5.153734142324,1.0]
      */
  }
}

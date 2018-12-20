package com.jueee.learnspark.dataanalysis.chapter11

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * 奇异值分解
  */
object S56SingularValueDecomposition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val vector1 = Vectors.dense(-2.0, 5.0, 1.0)
    val vector2 = Vectors.dense(2.0, 0.0, 1.0)
    val points = sc.parallelize(Array(vector1, vector2))

    val mat = new RowMatrix(points)
    val svd = mat.computeSVD(3, true)

    // U是一个分布式RowMatrix
    println("---U是一个分布式RowMatrix---")
    val u = svd.U
    println(u)

    // 奇异值用一个局部稠密向量表示
    println("---奇异值用一个局部稠密向量表示---")
    val s = svd.s
    println(s)

    // V是一个局部稠密矩阵
    println("---V是一个局部稠密矩阵---")
    val v = svd.V
    println(v)

    /**
      * ---U是一个分布式RowMatrix---
      * org.apache.spark.mllib.linalg.distributed.RowMatrix@1072e9d
      * ---奇异值用一个局部稠密向量表示---
      * [5.509533567570166,2.1552354553082034,3.131216243068682E-8]
      * ---V是一个局部稠密矩阵---
      * -0.4031460160351149  -0.8125074308681782  -0.4210759605332594
      * 0.9012313115913181   -0.2725930037664718  -0.33686076842660745
      * 0.15891951661896966  -0.5152909169406777  0.842151921066519
      */
  }
}

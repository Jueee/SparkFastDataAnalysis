package com.jueee.learnspark.dataanalysis.chapter11

import org.apache.spark.mllib.linalg.Vectors


/**
  * 用 Scala 创建向量
  */
object S4CreateVectors {


  def main(args: Array[String]): Unit = {
    val denseVec1 = Vectors.dense(1.0,2.0, 3.0, 4.0)
    val denseVec2 = Vectors.dense(Array(1.0, 2.0, 3.0))
    val sparseVec1 = Vectors.sparse(6, Array(0,2), Array(1.0,2.0))

    println(denseVec1.size)
    println(denseVec2.size)
    println(sparseVec1.size)
  }
}

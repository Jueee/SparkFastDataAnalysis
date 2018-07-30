package com.jueee.learnspark.dataanalysis.chapter03

import org.apache.spark.rdd.RDD

object S4PassFunctions {

  def main(args: Array[String]): Unit = {

    class SearchFunctions(val query:String){
      def isMatch(s:String):Boolean={
        s.contains(query)
      }
      def getMatchesFunctionReference(rdd:RDD[String]):RDD[Boolean] = {
        // 问题："isMatch"表示"this.isMatch"，因此我们要传递整个"this"
        rdd.map(isMatch)
      }
      def getMatchesFieldReference(rdd: RDD[String]):RDD[Array[String]] = {
        // 问题："query"表示"this.query"，因此我们要传递整个"this"
        rdd.map(x => x.split(query))
      }
      def getMatchesNoReference(rdd:RDD[String]):RDD[Array[String]] = {
        // 安全：只把我们需要的字段拿出来放入局部变量中
        val query_ = this.query
        rdd.map(x => x.split(query_))
      }
    }
  }
}

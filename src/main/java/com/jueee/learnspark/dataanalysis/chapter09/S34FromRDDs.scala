package com.jueee.learnspark.dataanalysis.chapter09

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object S34FromRDDs {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    // 创建了一个人的对象，并且把它转成SchemaRDD
    val happyPeopleRDD = sc.parallelize(List(new HappyPerson("holden", "coffee"),new HappyPerson("holden2", "coffee2"),new HappyPerson("holden2", "coffee2")))
    // 注意：此处发生了隐式转换
    val happyPeopleSchemaRDD = hiveCtx.applySchema(happyPeopleRDD, classOf[HappyPerson])
    happyPeopleSchemaRDD.registerTempTable("happy_people")
    happyPeopleSchemaRDD.show()

    val rows = hiveCtx.sql("select * from happy_people limit 5")
    rows.show()
  }
}

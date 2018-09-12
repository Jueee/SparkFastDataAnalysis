package com.jueee.learnspark.dataanalysis.chapter05

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  Available columns are: [key, bootstrapped, broadcast_address, cluster_name, cql_version, data_center, gossip_generation, host_id, listen_address, native_protocol_version, partitioner, rack, release_version, rpc_address, schema_version, thrift_version, tokens, truncated_at]
  */
object S52Cassandra {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", DataBaseUtil.CASSANDRA_HOSTNAME)
    val sc = new SparkContext(DataBaseUtil.SPARK_MASTER, DataBaseUtil.SPARK_APPNAME, conf)
    println(sc)

    // 为SparkContext和RDD提供附加函数的隐式转换
    import com.datastax.spark.connector._

    val data = sc.cassandraTable("system" , "local")
    // 打印出value字段的一些基本统计。
    val hostid = data.map(row => row.getString("host_id"))
    hostid.collect().foreach(println)
  }
}

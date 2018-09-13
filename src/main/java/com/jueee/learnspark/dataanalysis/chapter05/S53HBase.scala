package com.jueee.learnspark.dataanalysis.chapter05

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 报错：
  * java.lang.NoClassDefFoundError: com/google/protobuf/LiteralByteString
  * 解决：
  * protobuf-java 必须 2.5.0 以下，更高版本没有该类。
  * Apache Hadoop 2.7.3依赖于Protobuf 2.5.0
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>2.5.0</version>
        </dependency>
  */
object S53HBase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", DataBaseUtil.ZOOKERPER_HOSTNAME)
    hbaseConf.set("zookeeper.znode.parent", DataBaseUtil.HBASE_PRODUCT)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "test")  // 扫描哪张表

    val rdd = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    println(rdd.count())
    rdd.foreachPartition(fp => {
      fp.foreach({case(k,v) => {
        val kd = new String(k.get(),k.getOffset,k.getLength)
        val vd = v.rawCells()
        for(cell<-vd){
          val rowid = new String(cell.getRowArray,cell.getRowOffset,cell.getRowLength)
          val family = new String(cell.getFamilyArray,cell.getFamilyOffset,cell.getFamilyLength)
          val qulifier = new String(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)
          val value = new String(cell.getValueArray,cell.getValueOffset,cell.getValueLength)
          println(kd,rowid,family,qulifier,value)
        }
      }})
    })
  }
}

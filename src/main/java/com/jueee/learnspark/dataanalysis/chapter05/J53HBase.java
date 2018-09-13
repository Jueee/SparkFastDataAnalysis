package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

public class J53HBase {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", DataBaseUtil.ZOOKERPER_HOSTNAME);
        hbaseConf.set("zookeeper.znode.parent", DataBaseUtil.HBASE_PRODUCT);
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "test");  // 扫描哪张表

        JavaPairRDD<ImmutableBytesWritable, Result> rdd = sc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        System.out.println(rdd.count());
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
            @Override
            public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator) throws Exception {
                if (tuple2Iterator.hasNext()){
                    Tuple2<ImmutableBytesWritable, Result> next = tuple2Iterator.next();
                    ImmutableBytesWritable k = next._1();
                    Result v = next._2();
                    String kd = new String(k.get(),k.getOffset(),k.getLength());
                    Cell[] cells = v.rawCells();
                    for(Cell cell:cells){
                        String rowid = new String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                        String family = new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                        String qulifier = new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                        String value = new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                        System.out.println(kd + "," + rowid + "," + family + "," + qulifier + "," + value);
                    }
                }
            }
        });
    }
}

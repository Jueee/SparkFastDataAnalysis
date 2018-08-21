package com.jueee.learnspark.dataanalysis.chapter04;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

public class J5DataPartitioning {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        getDataPartitioning(sc);
    }

    /**
     * 获取 RDD 的分区方式
     * @param sc
     */
    public static void getDataPartitioning(JavaSparkContext sc) {
        // 创建出了一个由 (Int, Int) 对组成的 RDD
        JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(Arrays.asList(Tuple2.apply(1, 1), Tuple2.apply(2, 2), Tuple2.apply(3, 3)));
        // 初始时没有分区方式信息（一个值为 None 的 Option 对象）
        System.out.println(pairs.partitioner());

        // 通过对第一个 RDD 进行哈希分区，创建出了第二个 RDD。
        JavaPairRDD<Integer, Integer> partitioned = pairs.partitionBy(new HashPartitioner(2));
        System.out.println(partitioned);
        System.out.println(partitioned.partitioner());
    }


    class DomainNamePartitioner extends Partitioner{
        int numParts;

        public DomainNamePartitioner(int numParts) {
            this.numParts = numParts;
        }

        @Override
        public int numPartitions() {
            return numParts;
        }

        @Override
        public int getPartition(Object key) {
            String domain = null;
            try {
                domain = new URL(key.toString()).getHost();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            int code = domain.hashCode() % numPartitions();
            if (code < 0){
                return code + numPartitions();
            }
            return code;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DomainNamePartitioner){
                return ((DomainNamePartitioner) obj).numPartitions() == numPartitions();
            }
            return false;
        }
    }
}

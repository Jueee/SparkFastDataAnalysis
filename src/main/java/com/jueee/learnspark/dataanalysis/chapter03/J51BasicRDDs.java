package com.jueee.learnspark.dataanalysis.chapter03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class J51BasicRDDs {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 计算 RDD 中各值的平方
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> result = rdd.map(x -> x*x);
        for (Integer value:result.collect()){
            System.out.println(value);
        }

        //  flatMap() 将行数据切分为单词
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world","hi"));
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        for (String value:words.collect()){
            System.out.println(value);
        }

        //  reduce() 计算元素的总和
        Integer sum = result.reduce((x,y) -> x+y);
        System.out.println(sum);

        // 用 aggregate() 来计算 RDD 的平均值
        Function2<AvgCount,Integer,AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount avgCount, Integer integer) throws Exception {
                avgCount.total += integer;
                avgCount.num += 1;
                return avgCount;
            }
        };
        Function2<AvgCount,AvgCount,AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        AvgCount initial = new AvgCount(0,0);
        AvgCount resultAvg = result.aggregate(initial,addAndCount,combine);
        System.out.println(resultAvg.avg());

        //  Java 8 reduce() 计算元素的总和
        Tuple2<Integer,Integer> resultAvg8 = result.aggregate(new Tuple2<Integer,Integer>(0,0),
                ((avgCount,value) -> new Tuple2<Integer, Integer>(avgCount._1 + value,avgCount._2 + 1)),
                ((acc1,acc2) -> new Tuple2<Integer, Integer>(acc1._1 + acc2._1,acc1._2 + acc2._2))
                );
        System.out.println(resultAvg8._1/(double)resultAvg8._2);
    }
}

class AvgCount implements Serializable{
    public int total;
    public int num;
    public AvgCount(int total,int num){
        this.total = total;
        this.num = num;
    }
    public double avg(){
        return total/(double)num;
    }
}
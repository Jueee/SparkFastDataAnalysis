package com.jueee.learnspark.dataanalysis.chapter04;

import com.jueee.learnspark.dataanalysis.common.AvgCount;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class J31Aggregations {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        reduceByKeyJava7(sc);
        reduceByKeyJava8(sc);
        wordCountJava7(sc);
        wordCountJava8(sc);
        wordCountJava8BycountByValue(sc);
        combineByKeyJava7(sc);
        combineByKeyJava8(sc);
    }

    /**
     * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
     *
     * @param sc
     */
    public static void reduceByKeyJava7(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = pairs.mapValues(
                new Function<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer integer) {
                        return new Tuple2<Integer, Integer>(integer, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                        return new Tuple2<Integer, Integer>(x._1() + y._1(), x._2() + y._2());
                    }
                }
        );
        for (Tuple2<String, Tuple2<Integer, Integer>> value : values.collect()) {
            System.out.println(value);
        }
        JavaPairRDD<String, Double> averages = values.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> s) throws Exception {
                        return new Tuple2<String, Double>(s._1(), (double) s._2()._1() / s._2()._2());
                    }
                }
        );
        for (Tuple2<String, Double> average : averages.collect()) {
            System.out.println(average);
        }
    }

    ;

    /**
     * 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
     *
     * @param sc
     */
    public static void reduceByKeyJava8(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = pairs.mapValues(x -> new Tuple2<Integer, Integer>(x, 1))
                .reduceByKey((x, y) -> new Tuple2<Integer, Integer>(x._1() + y._1(), x._2() + y._2()));
        values.collect().forEach(System.out::println);
        JavaPairRDD<String, Double> averages = values.mapToPair(s -> new Tuple2<String, Double>(s._1(), (double) s._2()._1() / s._2()._2()));
        averages.collect().forEach(System.out::println);
    }

    /**
     * 实现单词计数
     * 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
     *
     * @param sc
     */
    public static void wordCountJava7(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile("README.md");
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );
        JavaPairRDD<String, Integer> result = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) throws Exception {
                        return a + b;
                    }
                }
        );
        for (Tuple2<String, Integer> v : result.collect()) {
            System.out.println(v);
        }
    }

    /**
     * 实现单词计数
     * 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
     *
     * @param sc
     */
    public static void wordCountJava8(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile("README.md");
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> result = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey((a, b) -> a + b);
        result.collect().forEach(System.out::println);
    }

    /**
     * 实现单词计数——countByValue
     * 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
     *
     * @param sc
     */
    public static void wordCountJava8BycountByValue(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile("README.md");
        Map<String, Long> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).countByValue();
        words.forEach((x, y) -> System.out.println(x + "," + y));
    }

    public static void combineByKeyJava7(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
        Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
            @Override
            public AvgCount call(Integer x) throws Exception {
                return new AvgCount(x,1);
            }
        };
        Function2<AvgCount,Integer,AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount avgCount, Integer x) throws Exception {
                avgCount.total += x;
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
        JavaPairRDD<String,AvgCount> avgCounts = pairs.combineByKey(createAcc,addAndCount,combine);
        Map<String,AvgCount> countMap = avgCounts.collectAsMap();
        for (Map.Entry<String,AvgCount> entry:countMap.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }

    public static void combineByKeyJava8(JavaSparkContext sc) {
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
    }
}


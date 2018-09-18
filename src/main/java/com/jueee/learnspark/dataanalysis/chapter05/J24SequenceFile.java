package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;

public class J24SequenceFile {


    static String filePath = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats";

    static String fileName = filePath + File.separator + "save_by_scala.sequence" + File.separator + "part-00000";


    public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>,String,Integer> {
        @Override
        public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) throws Exception {
            return new Tuple2<String, Integer>(record._1.toString(), record._2.get());
        }
    }

    /**
     * 在 Java 中读取 SequenceFile
     * @param sc
     */
    static void readSequenceFileByJava7(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(fileName, Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> result = input.mapToPair(new ConvertToNativeTypes());
        System.out.println(result.count());
        result.collect().forEach(System.out::println);
    }

    /**
     * 在 Java 中读取 SequenceFile
     * @param sc
     */
    static void readSequenceFileByJava8(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(fileName, Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> result = input.mapToPair(record -> new Tuple2<String, Integer>(record._1.toString(), record._2.get()));
        System.out.println(result.count());
        result.collect().forEach(System.out::println);
    }


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        readSequenceFileByJava7(sc);
        readSequenceFileByJava8(sc);
    }
}

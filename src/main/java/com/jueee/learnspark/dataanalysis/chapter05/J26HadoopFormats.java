package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class J26HadoopFormats {

    static String filePath = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats";

    static String fileName = filePath + File.separator + "save_by_java.hadoop" + File.separator + "part-00000";

    static String saveName = filePath + File.separator + "save_by_java.hadoop";

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String,Integer>, Text, IntWritable>{
        @Override
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) throws Exception {
            return new Tuple2<Text, IntWritable>(new Text(record._1), new IntWritable(record._2));
        }
    }

    /**
     * 在 Java 保存
     * @param sc
     */
    public static void saveHadoopFormatsByJava7(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
        JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
        result.saveAsHadoopFile(saveName,Text.class,IntWritable.class, SequenceFileOutputFormat.class);
    }

    /**
     * 在 Java 保存
     * @param sc
     */
    public static void saveHadoopFormatsByJava8(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2("panda", 0), new Tuple2("pink", 3), new Tuple2("pirate", 3), new Tuple2("panda", 1), new Tuple2("pink", 4)));
        JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(record -> new Tuple2(new Text(record._1), new IntWritable(record._2)));
        result.saveAsHadoopFile(saveName,Text.class,IntWritable.class, SequenceFileOutputFormat.class);
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
//        saveHadoopFormatsByJava7(sc);
//        saveHadoopFormatsByJava8(sc);
    }




}

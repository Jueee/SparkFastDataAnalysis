package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.io.StringReader;
import java.util.Iterator;

public class J23CSV {

    private static String inputFile = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "demo.csv";

    public static class ParseLine implements Function<String,String[]> {
        @Override
        public String[] call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            return reader.readNext();
        }
    }

    /**
     * 在 Java 中使用 textFile() 读取 CSV
     * @param sc
     */
    public static void readCSVbyJava7(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String[]> csvData = input.map(new ParseLine());
        csvData.collect().forEach(value -> {
            System.out.println(value + "--" + value.length + "--" + value[0]);
        });
    }

    /**
     * 在 Java 中使用 textFile() 读取 CSV
     * @param sc
     */
    public static void readCSVbyJava8(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String[]> csvData = input.map(line -> new CSVReader(new StringReader(line)).readNext());
        csvData.collect().forEach(value -> {
            System.out.println(value + "--" + value.length + "--" + value[1]);
        });
    }


    public static class ParseLineFull implements FlatMapFunction<Tuple2<String,String>,String[]> {
        @Override
        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2));
            return reader.readAll().iterator();
        }
    }

    /**
     * 在 Java 中完整读取 CSV
     * @param sc
     */
    public static void readFullCSVbyJava7(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(inputFile);
        JavaRDD<String[]> keyedRdd = csvData.flatMap(new ParseLineFull());
        keyedRdd.collect().forEach(value -> {
            System.out.println(value + "--" + value.length + "--" + value[0]);
        });
    }

    /**
     * 在 Java 中完整读取 CSV
     * @param sc
     */
    public static void readFullCSVbyJava8(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(inputFile);
        JavaRDD<String[]> keyedRdd = csvData.flatMap(file -> new CSVReader(new StringReader(file._2)).readAll().iterator());
        keyedRdd.collect().forEach(value -> {
            System.out.println(value + "--" + value.length + "--" + value[1]);
        });
    }

    public static void saveCSV(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaRDD<String> input = sc.textFile(inputFile);
//        input.mapPartitions(people -> {
//            StringWriter stringWriter = new StringWriter();
//            CSVWriter csvWriter = new CSVWriter(stringWriter);
//            csvWriter.writeAll(people);
//            return stringWriter.
//        });
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);
        input.collect().forEach(System.out::println);
        readCSVbyJava7(sc);
        readCSVbyJava8(sc);
        readFullCSVbyJava7(sc);
        readFullCSVbyJava8(sc);
        saveCSV(sc);
    }
}

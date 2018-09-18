package com.jueee.learnspark.dataanalysis.chapter06;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class J2Accumulators {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("README.md");

        Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> callSigns = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                if (line.equals("")) {
                    blankLines.add(1);
                }
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        callSigns.collect().forEach(System.out::println);

        System.out.println("Blank lines: "+ blankLines.value());
    }
}
